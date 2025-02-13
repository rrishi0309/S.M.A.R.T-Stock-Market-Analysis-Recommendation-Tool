from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
import sqlalchemy
import functions_framework
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

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

    # Get the password from Secret Manager
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

@functions_framework.http
def clear_tables(request):
    try:
        with pool.connect() as conn:
            logging.info("Connected to the database.")

            # Begin a transaction to delete data from both tables
            with conn.begin() as transaction:
                try:
                    # Delete data from stock_data table
                    delete_stock_data = conn.execute(text("DELETE FROM stock_data"))
                    logging.info(f"Deleted {delete_stock_data.rowcount} rows from 'stock_data' table.")

                    # Delete data from news_sentiment table
                    delete_news_sentiment = conn.execute(text("DELETE FROM news_sentiment"))
                    logging.info(f"Deleted {delete_news_sentiment.rowcount} rows from 'news_sentiment' table.")

                except SQLAlchemyError as sql_err:
                    logging.error(f"SQLAlchemy Error while clearing tables: {sql_err}")
                    transaction.rollback()  # Explicit rollback on failure
                    return {"error": f"SQLAlchemy Error clearing tables: {sql_err}"}, 500

        logging.info("All rows deleted from 'stock_data' and 'news_sentiment' tables.")
        return {
            "message": "All rows deleted from 'stock_data' and 'news_sentiment' tables."
        }, 200

    except SQLAlchemyError as sql_err:
        logging.error(f"SQLAlchemy Error connecting to the database: {sql_err}")
        return {"error": f"SQLAlchemy Error connecting to the database: {sql_err}"}, 500

    except Exception as e:
        logging.error(f"Error clearing tables: {e}")
        return {"error": f"Error clearing tables: {e}"}, 500
