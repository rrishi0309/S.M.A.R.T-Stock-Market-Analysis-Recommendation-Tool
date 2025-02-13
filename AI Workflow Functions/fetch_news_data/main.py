from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from bs4 import BeautifulSoup
import FinNews as fn
import logging
from datetime import datetime
from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
import sqlalchemy
import functions_framework
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)

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
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection,
    pool_size=5,
    max_overflow=2,
)

# Function to fetch article content concurrently with minimal retry logic
def fetch_article_content(article):
    try:
        session = requests.Session()
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }

        response = session.get(article['link'], headers=headers, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            paragraphs = soup.find_all('p')
            full_content = ' '.join([para.get_text() for para in paragraphs])

            if len(full_content.strip()) < 50:
                logging.info(f"Skipping due to insufficient content: {article['link']}")
                return None

            # Handle the published date
            published_at = None
            if 'published' in article:
                try:
                    published_at = datetime.strptime(article['published'], '%a, %d %b %Y %H:%M:%S %z')
                except ValueError as e:
                    logging.error(f"Error parsing published date for article '{article['title']}': {e}")
                    published_at = None  # Allow article even if published date parsing fails

            # Return the processed article data
            return {
                'source': article.get('source', 'Unknown'),
                'title': article['title'],
                'publishedat': published_at,
                'url': article['link'],
                'symbol': article['topic'],
                'full_content': full_content[:5000],  # Limit content to 5000 characters
                'sentiment_score': 0.0,  # Default placeholder for sentiment score
            }
        else:
            logging.warning(f"Failed to fetch content, status code {response.status_code} for URL: {article['link']}")
    except requests.RequestException as e:
        logging.error(f"Error fetching URL {article['link']}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing article '{article['title']}': {e}")
    return None

# Function to fetch the latest news and full content for each article
@functions_framework.http
def fetch_news_data(request):
    try:
        # Extract the company name from request arguments
        company_name = request.args.get("company_name")
        if not company_name:
            return {"error": "Missing 'company_name' parameter"}, 400

        # Fetch financial news for the company from Yahoo Finance
        try:
            articles = fn.Yahoo(topics=[f'${company_name}']).get_news()
            if articles:
                logging.info(f"Fetched {len(articles)} articles from Yahoo Finance.")
            else:
                logging.info("No articles fetched from Yahoo Finance.")
        except Exception as e:
            logging.error(f"Error fetching articles from Yahoo Finance: {e}")
            articles = []

        # If no articles were fetched, return early
        if not articles:
            return {"articles": []}, 200

        # Add company name to each article for context
        for article in articles:
            article['topic'] = company_name

        news_data = []

        # Use ThreadPoolExecutor to speed up fetching article contents
        with ThreadPoolExecutor(max_workers=5) as executor:  # Increased workers to 15
            futures = [executor.submit(fetch_article_content, article) for article in articles]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    news_data.append(result)
                if len(news_data) >= 10:  # Limit to 10 articles
                    break

        # Convert the news data to a DataFrame
        news_df = pd.DataFrame(news_data)
        if news_df.empty:
            return {"articles": []}, 200

        # Store data in the database with an "Upsert"-like approach
        try:
            with pool.begin() as conn:
                for index, row in news_df.iterrows():
                    # Use a parameterized delete query to avoid duplicates
                    delete_query = text("""
                        DELETE FROM news_sentiment
                        WHERE symbol = :symbol
                        AND title = :title
                        AND (publishedat IS NULL OR publishedat = :publishedat)
                    """)
                    conn.execute(delete_query, {
                        'symbol': row['symbol'],
                        'title': row['title'],
                        'publishedat': row['publishedat']
                    })

                # Insert the new data
                news_df.to_sql('news_sentiment', con=conn, if_exists='append', index=False, dtype={'sentiment_score': sqlalchemy.types.Float()})
                logging.info(f"News data for {company_name} saved successfully.")
                logging.info(f"Number of articles saved in the database: {len(news_df)}")

        except Exception as e:
            logging.error(f"Error saving news data to database: {e}")
            return {"error": f"Error saving news data to database: {e}"}, 500

        return news_df.to_json(orient="records"), 200

    except Exception as e:
        logging.error(f"Error occurred during news data fetch or save: {e}")
        return {"error": f"Error occurred: {e}"}, 500
