from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
import sqlalchemy
import google.generativeai as genai
import functions_framework
import logging
import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from flask import Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

logging.basicConfig(level=logging.INFO)

# Initialize Gemini API model configuration to control the generation parameters
generation_config = {
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 50,
    "max_output_tokens": 1024,
}

# Function to access secrets from Secret Manager
def access_secret_version(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/1081266316250/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

# Configure connection to Cloud SQL
def get_connection():
    connector = Connector()
    db_password = access_secret_version("sql-db-password")
    return connector.connect(
        "smart-data-dragons:us-central1:sql-smart-project",
        "pg8000",
        user="postgres",
        password=db_password,
        db="postgres"
    )

# Create SQLAlchemy connection pool
pool = create_engine(
    "postgresql+pg8000://",
    creator=get_connection,
)

# Create a session factory
Session = sessionmaker(bind=pool)

# Configure Gemini API using the secret key
gemini_api_key = access_secret_version("gemini-api-key")
genai.configure(api_key=gemini_api_key)

# Create a GenerativeModel instance using Gemini 1.5 Pro model
model = genai.GenerativeModel(
    model_name="gemini-1.5-pro",
    generation_config=generation_config,
)

# Function to analyze sentiment and generate recommendation
@functions_framework.http
def analyze_sentiment(request: Request):
    try:
        # Extract the stock symbol from the request arguments (GET parameter)
        company_name = request.args.get("company_name")
        if not company_name:
            logging.error("Missing 'company_name' parameter")
            return {"error": "Missing 'company_name' parameter"}, 400
        stock_symbol = company_name.upper()

        logging.info(f"Analyzing sentiment and generating recommendation for stock: {stock_symbol}")

        # Fetch stock data from the database for the past year
        with pool.connect() as conn:
            stock_data_query = text("""
                SELECT date, close 
                FROM stock_data 
                WHERE LOWER(symbol) = LOWER(:symbol)
                AND date > NOW() - INTERVAL '1 year'
                ORDER BY date ASC
            """)
            stock_data_df = pd.read_sql(stock_data_query, conn, params={"symbol": stock_symbol})

            if stock_data_df.empty:
                logging.warning(f"No stock data available for symbol: {stock_symbol}")
                return {"error": f"No stock data available for symbol: {stock_symbol}"}, 404

            # Drop rows with missing 'close' values
            stock_data_df.dropna(subset=['close'], inplace=True)
            
            # Calculate the moving average and percentage change for stock prices
            stock_data_df['moving_average'] = stock_data_df['close'].rolling(7).mean()  # 7-day moving average
            stock_data_df['percent_change'] = stock_data_df['close'].pct_change() * 100  # Daily percentage change
            overall_trend = stock_data_df['percent_change'].mean()  # Average of percentage changes to understand stock trend

            if pd.isna(overall_trend):
                overall_trend = 0.0  # Set to 0 if there is insufficient data to calculate trend

        logging.info(f"Calculated overall trend for {stock_symbol}: {overall_trend}")

        # Fetch news sentiment data from the database
        with pool.connect() as conn:
            news_sentiment_query = text("SELECT id, title, full_content FROM news_sentiment WHERE LOWER(symbol) = LOWER(:symbol)")
            news_sentiment_df = pd.read_sql(news_sentiment_query, conn, params={"symbol": stock_symbol})

            if news_sentiment_df.empty:
                logging.warning(f"No news sentiment data available for symbol: {stock_symbol}")
                return {"error": f"No news sentiment data available for symbol: {stock_symbol}"}, 404

        # Analyze sentiment using Gemini API with ThreadPoolExecutor
        logging.info("Analyzing news sentiment using Gemini API with ThreadPoolExecutor")
        sentiment_scores = []

        def analyze_article_sentiment(row):
            if len(row['full_content'].strip()) < 100:
                return row['id'], 0.0
            prompt = f"Provide a sentiment rating for this news article between -1 (very negative) and 1 (very positive). Only return the numeric value. Title: '{row['title']}'. Content: {row['full_content'][:5000]}"
            try:
                response = model.generate_content(prompt)
                sentiment_text = response.text.strip()
                logging.info(f"Gemini response for article '{row['title']}': {sentiment_text}")

                # Extract numeric sentiment value using regex
                match = re.search(r'[-+]?\d*\.\d+|\d+', sentiment_text)
                if match:
                    sentiment_value = float(match.group())
                    return row['id'], sentiment_value
                else:
                    logging.warning(f"Could not parse sentiment value from response: '{sentiment_text}', defaulting to 0.0")
                    return row['id'], 0.0

            except Exception as e:
                logging.error(f"Failed to get sentiment from Gemini for article '{row['title']}': {e}")
                return row['id'], 0.0

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(analyze_article_sentiment, row)
                for _, row in news_sentiment_df.iterrows()
            ]
            for future in as_completed(futures):
                news_id, sentiment_score = future.result()
                sentiment_scores.append(sentiment_score)

                # Update the sentiment score in the news_sentiment table
                try:
                    with Session() as session:
                        update_sentiment_query = text("""
                            UPDATE news_sentiment
                            SET sentiment_score = :sentiment_score
                            WHERE id = :id
                        """)
                        session.execute(update_sentiment_query, {"sentiment_score": sentiment_score, "id": news_id})
                        session.commit()  # Explicitly commit the transaction
                        logging.info(f"Updated sentiment score for article ID {news_id}: {sentiment_score}")
                except Exception as e:
                    logging.error(f"Failed to update sentiment score in the database for article ID {news_id}: {e}")

        recent_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
        logging.info(f"Average sentiment score for {stock_symbol}: {recent_sentiment}")

        # Determine sentiment and trend weights
        sentiment_weight = 0.5
        trend_weight = 0.5

        # Adjust weights based on sentiment and trend values
        if abs(recent_sentiment) > 0.5:
            sentiment_weight = 0.7
            trend_weight = 0.3
        elif abs(overall_trend) > 0.5:
            sentiment_weight = 0.3
            trend_weight = 0.7

        # Log weights for analysis
        logging.info(f"Sentiment weight: {sentiment_weight}, Trend weight: {trend_weight}")

        # Calculate recommendation score using weighted average formula
        recommendation_score = (sentiment_weight * recent_sentiment) + (trend_weight * overall_trend)
        
        # Generate final recommendation based on the recommendation score
        if recommendation_score > 0.2:
            final_recommendation = 'Buy'
        elif recommendation_score < -0.2:
            final_recommendation = 'Sell'
        else:
            final_recommendation = 'Hold'

        reasoning = (f"Based on the recent news sentiment ({recent_sentiment:.2f}) "
                     f"and stock trend ({overall_trend:.2f}), the recommendation is '{final_recommendation}'. "
                     "The overall market trend combined with the sentiment suggests that investors should "
                     f"{final_recommendation.lower()} this stock.")

        logging.info(f"Recommendation for {stock_symbol}: {final_recommendation} with score {recommendation_score:.2f}")

        # Insert or update the recommendation in the recommendations table
        with Session() as session:
            try:
                # Remove oldest records if more than 5 records exist for the same symbol
                remove_old_records_query = text("""
                    DELETE FROM recommendations
                    WHERE symbol = :symbol
                    AND id IN (
                        SELECT id FROM recommendations WHERE symbol = :symbol ORDER BY created_at ASC LIMIT GREATEST(0, (SELECT COUNT(*) FROM recommendations WHERE symbol = :symbol) - 4)
                    )
                """)
                session.execute(remove_old_records_query, {"symbol": stock_symbol})

                # Insert the new recommendation or update if it already exists
                insert_recommendation_query = text("""
                    INSERT INTO recommendations (symbol, recommendation_score, final_recommendation, reasoning, created_at, close_price, moving_average, date)
                    VALUES (:symbol, :recommendation_score, :final_recommendation, :reasoning, :created_at, :close_price, :moving_average, :date)
                    ON CONFLICT (symbol)
                    DO UPDATE SET recommendation_score = EXCLUDED.recommendation_score,
                                  final_recommendation = EXCLUDED.final_recommendation,
                                  reasoning = EXCLUDED.reasoning,
                                  created_at = EXCLUDED.created_at,
                                  close_price = EXCLUDED.close_price,
                                  moving_average = EXCLUDED.moving_average,
                                  date = EXCLUDED.date;
                """)
                session.execute(insert_recommendation_query, {
                    "symbol": stock_symbol,
                    "recommendation_score": recommendation_score,
                    "final_recommendation": final_recommendation,
                    "reasoning": reasoning,
                    "created_at": datetime.utcnow(),
                    "close_price": stock_data_df['close'].iloc[-1],
                    "moving_average": overall_trend,
                    "date": stock_data_df['date'].iloc[-1]
                })
                session.commit()  # Commit the new recommendation
                logging.info(f"Recommendation for {stock_symbol}: {final_recommendation} with score {recommendation_score:.2f} saved successfully.")
            
            except Exception as e:
                logging.error(f"Failed to insert or update recommendation for {stock_symbol}: {e}")
                session.rollback()  # Rollback in case of an error

        return {
            "symbol": stock_symbol,
            "recommendation_score": recommendation_score,
            "final_recommendation": final_recommendation,
            "reasoning": reasoning
        }, 200

    except Exception as e:
        logging.error(f"Error during sentiment analysis and recommendation generation: {e}")
        return {"error": f"Error occurred: {e}"}, 500