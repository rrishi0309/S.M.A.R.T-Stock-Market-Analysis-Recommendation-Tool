from google.cloud.sql.connector import Connector 
from google.cloud import secretmanager
import sqlalchemy
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import logging
from flask import Request
from sqlalchemy import text

logging.basicConfig(level=logging.DEBUG)

# Function to access secrets from Secret Manager
def access_secret_version(secret_id, version_id="latest"):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/1081266316250/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        logging.error(f"Failed to retrieve secret: {e}")
        raise

# Configure connection to Cloud SQL
def get_connection():
    try:
        connector = Connector()
        db_password = access_secret_version("sql-db-password")

        return connector.connect(
            "smart-data-dragons:us-central1:sql-smart-project",
            "pg8000",
            user="postgres",
            password=db_password,
            db="postgres"
        )
    except Exception as e:
        logging.error(f"Failed to connect to Cloud SQL: {e}")
        raise

# Create SQLAlchemy connection pool
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection,
    pool_size=5,
    max_overflow=2,
)

# Function to fetch stock data and store it in Cloud SQL
def fetch_stock_data(request: Request):
    try:
        company_name = request.args.get('company_name')
        if not company_name:
            return "Error: 'company_name' parameter is required.", 400

        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        logging.info(f"Fetching stock data from {start_date} to {end_date} for {company_name} using yfinance...")

        df = yf.download(company_name, start=start_date, end=end_date, progress=False)

        if not df.empty:
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            df['symbol'] = company_name
            df['date'] = pd.to_datetime(df.index).date
            df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]

            try:
                with pool.begin() as conn:
                    # Delete existing data for the company within the date range
                    delete_query = text("""
                        DELETE FROM stock_data
                        WHERE symbol = :symbol
                        AND date >= :start_date AND date <= :end_date
                    """)
                    conn.execute(delete_query, {
                        'symbol': company_name,
                        'start_date': start_date,
                        'end_date': end_date
                    })
                    logging.info(f"Existing data for {company_name} from {start_date} to {end_date} deleted successfully.")

                    # Insert the new data into the table
                    df.to_sql('stock_data', con=conn, if_exists='append', index=False)
                    logging.info(f"Data fetched and saved successfully for {company_name}. Total records: {len(df)}")

            except Exception as e:
                logging.error(f"Error saving stock data to database: {e}")
                return {"error": f"Error saving stock data to database: {e}"}, 500

            return f"Data fetched and saved successfully for {company_name}. Total records: {len(df)}", 200
        else:
            logging.warning(f"No data available for {company_name}.")
            return f"No data available for {company_name}.", 404

    except Exception as e:
        logging.error(f"Error occurred during stock data fetch or save: {e}")
        return {"error": f"Error occurred: {e}"}, 500
