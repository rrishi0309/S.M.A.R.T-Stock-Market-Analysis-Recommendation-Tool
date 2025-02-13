import streamlit as st
import requests
import pandas as pd
import yfinance as yf
import plotly.graph_objs as go
import time
import google.auth  # Import for default credentials
from streamlit_echarts import st_echarts
from google.auth import default
from google.auth.transport.requests import Request as AuthRequest
from google.auth.credentials import with_scopes_if_required
from google.cloud import secretmanager_v1 as secretmanager
import sqlalchemy
from sqlalchemy import text
import logging
from datetime import datetime, timezone
import uuid
import pg8000.native  # Directly import the pg8000 native client for PostgreSQL
from google.cloud.sql.connector import Connector
import pytz


# Set the page configuration first
st.set_page_config(
    page_title="SMART - Stock Market Analysis & Recommendation Tool", 
    page_icon="📈", 
    layout="wide"
)

logging.basicConfig(level=logging.INFO)

# Define constants
WORKFLOW_EXECUTION_URL = "https://workflowexecutions.googleapis.com/v1/projects/smart-data-dragons/locations/us-central1/workflows/SMART_Workflow/executions"
CLEAR_DB_URL = "https://clear-tables-1081266316250.us-central1.run.app"
BUY_IMAGE_URL = "https://storage.googleapis.com/gcp_public_bucket/smart_status/buy.png"
HOLD_IMAGE_URL = "https://storage.googleapis.com/gcp_public_bucket/smart_status/hold.png"
SELL_IMAGE_URL = "https://storage.googleapis.com/gcp_public_bucket/smart_status/sell.png"

# Function to access secrets from Secret Manager
def access_secret_version(secret_id, version_id="latest"):
    # No need to pass credentials, App Engine uses default ones
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/1081266316250/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

# Initialize the Cloud SQL Connector
connector = Connector()

# Configure connection to use Cloud SQL instance
def get_connection():
    db_password = access_secret_version("sql-db-password")
    connection = connector.connect(
        "smart-data-dragons:us-central1:sql-smart-project",
        "pg8000",
        user="postgres",
        password=db_password,
        db="postgres"
    )
    return connection

# Create SQLAlchemy connection pool using pg8000 driver
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=get_connection,
    pool_size=5,
    max_overflow=2,
)

# Initialize session state
if 'selected_stock' not in st.session_state:
    st.session_state['selected_stock'] = None
if 'workflow_run_in_progress' not in st.session_state:
    st.session_state['workflow_run_in_progress'] = False
if 'workflow_execution_id' not in st.session_state:
    st.session_state['workflow_execution_id'] = None
if 'search_triggered' not in st.session_state:
    st.session_state['search_triggered'] = False
if 'stock_clicked' not in st.session_state:
    st.session_state['stock_clicked'] = False

# Function to check valid stock
def is_valid_ticker(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return 'longName' in info and info['longName'] != 'N/A'
    except Exception:
        return False

# Function to fetch recommendation data
def fetch_recommendation(stock_symbol):
    with pool.connect() as conn:
        query = text("""
            SELECT symbol, final_recommendation, recommendation_score, reasoning, created_at
            FROM recommendations
            WHERE LOWER(symbol) = LOWER(:symbol)
            ORDER BY created_at DESC
            LIMIT 1
        """)
        result = conn.execute(query, {'symbol': stock_symbol}).fetchone()
        if result:
            return dict(result._mapping)
        return None

# Function to fetch news data from news_sentiment table
def fetch_news_data(stock_symbol):
    with pool.connect() as conn:
        query = text("""
            SELECT title, publishedat, sentiment_score FROM news_sentiment
            WHERE LOWER(symbol) = LOWER(:symbol)
            ORDER BY created_at DESC
            LIMIT 10
        """)
        result = conn.execute(query, {'symbol': stock_symbol}).fetchall()
        if result:
            return pd.DataFrame(result, columns=result[0]._fields)
        return pd.DataFrame()

# Function to fetch company info from Yahoo Finance
def fetch_company_info(stock_symbol):
    ticker = yf.Ticker(stock_symbol)
    company_info = {
        "Name": ticker.info.get("longName", "N/A"),
        "Industry": ticker.info.get("industry", "N/A"),
        "Summary": '. '.join(ticker.info.get("longBusinessSummary", "N/A").split('.')[:3])  # Shortened summary to three sentences
    }
    return company_info

# Function to fetch stock data from Yahoo Finance
def fetch_stock_data(stock_symbol):
    ticker = yf.Ticker(stock_symbol)
    df = ticker.history(period="1y")
    df.reset_index(inplace=True)
    return df

# Function to fetch all available stocks from recommendations table
def fetch_all_stocks():
    with pool.connect() as conn:
        query = text("SELECT symbol, final_recommendation, recommendation_score FROM recommendations ORDER BY created_at DESC LIMIT 5")
        result = conn.execute(query).fetchall()
        return [
            {
                "symbol": row[0],
                "recommendation": row[1],
                "score": row[2]
            }
            for row in result
        ]

def get_id_token(target_audience):
    credentials, _ = default()
    credentials = with_scopes_if_required(credentials, ['https://www.googleapis.com/auth/cloud-platform'])
    credentials.refresh(AuthRequest())
    if hasattr(credentials, 'token'):
        return credentials.token
    else:
        raise AttributeError("'Credentials' object has no attribute 'id_token'")

# Trigger Workflow for Stock Data Retrieval
def trigger_workflow_for_stock():
    with st.spinner("Triggering Google Cloud Workflow..."):
        try:
            # Set the workflow URL
            id_token = get_id_token(WORKFLOW_EXECUTION_URL)

            # Execute the workflow with the selected stock as input
            response = requests.post(
                WORKFLOW_EXECUTION_URL,
                headers={"Authorization": f"Bearer {id_token}"},
                json={"argument": f'{{"company_name": "{st.session_state["selected_stock"]}"}}'}
            )
            response.raise_for_status()
            
            # Extract and display a more readable execution ID
            execution_id = response.json()["name"]
            execution_id_short = execution_id.split("/")[-1]
            st.session_state['workflow_execution_id'] = execution_id
            st.success(f"Workflow triggered successfully with Execution ID: {execution_id_short}")
            st.session_state['workflow_run_in_progress'] = True

            # Call monitoring function to keep track of the workflow execution
            monitor_workflow_execution()
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to trigger Workflow due to network error: {e}")
            st.session_state['workflow_run_in_progress'] = False
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            st.session_state['workflow_run_in_progress'] = False

# Function to fetch workflow execution logs
def fetch_workflow_logs(workflow_execution_url, headers):
    try:
        response = requests.get(f"{workflow_execution_url}/logs", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching logs: {e}")
        return {"entries": []}

# Monitor workflow with enhanced logging
def monitor_workflow_execution():
    workflow_execution_url = f"https://workflowexecutions.googleapis.com/v1/{st.session_state['workflow_execution_id']}"
    headers = {
        "Authorization": f"Bearer {get_id_token(workflow_execution_url)}"
    }

    steps = [
        "Fetching news data...",
        "Fetching stock data...",
        "Analyzing sentiment...",
    ]
    
    with st.spinner("Monitoring Workflow..."):
        try:
            step_index = 0
            while st.session_state['workflow_run_in_progress']:
                response = requests.get(workflow_execution_url, headers=headers)
                response.raise_for_status()
                execution_data = response.json()
                state = execution_data.get("state", "UNKNOWN")
                
                if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    st.session_state['workflow_run_in_progress'] = False
                    st.success(f"Workflow ended with state: {state}")
                    break
                else:
                    # Show the current hardcoded step description
                    if step_index < len(steps):
                        st.info(steps[step_index])
                        step_index += 1
                    time.sleep(5)

        except requests.exceptions.RequestException as e:
            st.error(f"Failed to get Workflow status: {e}")
            st.session_state['workflow_run_in_progress'] = False


# Cancel the Workflow Execution
def cancel_workflow_execution():
    workflow_execution_url = f"https://workflowexecutions.googleapis.com/v1/{st.session_state['workflow_execution_id']}:cancel"
    
    headers = {
        "Authorization": f"Bearer {get_id_token(workflow_execution_url)}"
    }

    with st.spinner("Cancelling Workflow..."):
        try:
            response = requests.post(workflow_execution_url, headers=headers)
            response.raise_for_status()
            st.success("Workflow cancelled successfully!")
            st.session_state['workflow_run_in_progress'] = False
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to cancel Workflow: {e}")


# Function to refresh data for the stock
def refresh_data_for_stock(stock_symbol):
    st.session_state['workflow_run_in_progress'] = False
    st.session_state['workflow_execution_id'] = None
    st.session_state['selected_stock'] = stock_symbol
    st.session_state['search_triggered'] = True
    trigger_workflow_for_stock()

# Sidebar for buttons
st.sidebar.markdown("### Available Stocks")
available_stocks = fetch_all_stocks()
for index, stock in enumerate(available_stocks):
    button_label = f"{stock['symbol']} ({stock['recommendation']}, Score: {stock['score']:.2f})"
    if st.sidebar.button(button_label, key=f"stock_{stock['symbol']}_{index}"):
        st.session_state.clear()
        st.session_state['selected_stock'] = None
        st.session_state['selected_stock'] = stock['symbol']
        st.session_state['workflow_run_in_progress'] = False
        st.session_state['workflow_execution_id'] = None
        st.rerun()  # Refresh to reflect the selected stock immediately

# Sidebar - Clear Database Button
st.sidebar.markdown("---")
if st.sidebar.button("Clear Database", key="clear_db_btn"):
    try:
        # Clear the local database
        with pool.connect() as conn:
            conn.execute(text("DELETE FROM recommendations"))
            conn.execute(text("COMMIT"))
        response = requests.post(CLEAR_DB_URL)
        response.raise_for_status()
        st.success("Database cleared successfully!")
        
        # Reinitialize session state after clearing
        st.session_state.clear()
        st.session_state['selected_stock'] = None
        st.session_state['workflow_run_in_progress'] = False
        st.session_state['workflow_execution_id'] = None
        st.session_state['search_triggered'] = False

        st.experimental_rerun()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to clear the cloud database: {e}")
    except Exception as e:
        st.error(f"Failed to clear the database: {e}")

st.sidebar.markdown("---")

# Stock Selection Section
st.title("S.M.A.R.T - Stock Market Analysis & Recommendation Tool")
st.header("Stock Selection")

# Stock input for manual search within form
with st.form(key="stock_analysis_form", clear_on_submit=False, enter_to_submit=False):
    stock_symbol_input = st.text_input("Enter Stock Symbol (e.g., AAPL, TSLA, MSFT):", help="Enter the stock symbol you want to analyze")
    analyze_button_clicked = st.form_submit_button("Analyze Sentiment")

# Clear button to reset the page
if st.button("Clear Page"):
    st.session_state.clear()
    st.rerun()

# Logic to analyze sentiment if analyze button is clicked
if analyze_button_clicked and stock_symbol_input.strip() != "":
    # Clear previous session states
    st.session_state['workflow_execution_id'] = None
    st.session_state['workflow_run_in_progress'] = False

    # Validate the stock ticker using yfinance
    if not is_valid_ticker(stock_symbol_input):
        st.error("❌ Incorrect stock entered. Please try again with a valid NASDAQ stock.")
    else:
        st.session_state['selected_stock'] = stock_symbol_input
        st.session_state['search_triggered'] = True
        trigger_workflow_for_stock()


# Display Company Information if a stock is selected
if st.session_state['selected_stock'] and not st.session_state['workflow_run_in_progress']:
    stock_symbol = st.session_state['selected_stock']
    company_info = fetch_company_info(stock_symbol)
    st.header("Company Information")
    st.markdown(f"<div style='text-align: center;'>"
                f"<h3>Company Name: {company_info['Name']}</h3>"
                f"<p><strong>Industry:</strong> {company_info['Industry']}</p>"
                f"<p><strong>Summary:</strong> {company_info['Summary']}</p>"
                f"</div>", unsafe_allow_html=True)

    # Button to refresh data with unique key based on stock symbol
    if st.button("Refresh Data", key=f"refresh_data_btn_{stock_symbol}"):
        if not is_valid_ticker(stock_symbol):
            st.error("❌ Incorrect stock entered. Please try again with a valid NASDAQ ticker.")
        else:
            refresh_data_for_stock(stock_symbol)
            
    # Fetch and display stock price data and recommendation
    recommendation = fetch_recommendation(stock_symbol)
    if recommendation:
            # Convert the UTC time to local time
            utc_time = recommendation['created_at'].replace(tzinfo=timezone.utc)
            local_time = utc_time.astimezone()  # Convert to local time

            formatted_date = local_time.strftime("%Y-%m-%d %I:%M %p")

            # Display formatted local time for stock data
            st.markdown(f"**Stock data date**: {formatted_date}")

            # Fetch and display the latest fetched stock price
            with pool.connect() as conn:
                query = text("""
                    SELECT close 
                    FROM stock_data
                    WHERE LOWER(symbol) = LOWER(:symbol)
                    ORDER BY date DESC
                    LIMIT 1
                """)
                result = conn.execute(query, {'symbol': stock_symbol}).fetchone()
                if result:
                    latest_price = result[0]  # Accessing the value using an index
                    st.markdown(f"**Latest Collected Stock Price**: ${latest_price:.2f}")
                else:
                    st.warning("No price data available.")

    # Fetch and display stock price trend
    stock_data = fetch_stock_data(stock_symbol)
    if not stock_data.empty:
        st.header(f"{stock_symbol} Stock Price Trend")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=stock_data['Date'], y=stock_data['Close'], mode='lines', name='Stock Price', line=dict(color='blue')))
        fig.add_trace(go.Scatter(x=stock_data['Date'], y=stock_data['Close'].rolling(7).mean(), mode='lines', name='7-Day Moving Average', line=dict(dash='dash', color='orange')))
        fig.update_layout(title=f'{stock_symbol} Stock Price Trend Over the Past Year', xaxis_title='Date', yaxis_title='Price (USD)')
        st.plotly_chart(fig)

        # Fetch and display news data
        news_df = fetch_news_data(stock_symbol)
        if not news_df.empty:
            st.header("Fetched News Data with Sentiment Analysis")

            # Reduce the title's text size for better fit
            news_df['title'] = news_df['title'].str.slice(0, 80)  # Limit the title to 80 characters

            # Convert published date to local timezone using native Python
            def convert_to_local(dt):
                if pd.isnull(dt):
                    return None
                elif isinstance(dt, pd.Timestamp):
                    # Convert UTC to local timezone using datetime's astimezone
                    return dt.to_pydatetime().astimezone()
                elif isinstance(dt, datetime):
                    # Convert directly if it's a datetime object
                    return dt.astimezone()
                else:
                    # Assume dt is an ISO 8601 string, convert to datetime
                    dt_obj = datetime.fromisoformat(dt)
                    return dt_obj.astimezone()


            news_df['publishedat'] = news_df['publishedat'].apply(convert_to_local)

            # Sort the DataFrame by published date
            news_df = news_df.sort_values(by='publishedat', ascending=False)

            # Only display selected columns
            st.dataframe(news_df[['title', 'publishedat', 'sentiment_score']], use_container_width=True)

            # Ensure sentiment_score is numeric and handle errors
            news_df['sentiment_score'] = pd.to_numeric(news_df['sentiment_score'], errors='coerce')
            news_df['sentiment_score'].fillna('Unknown', inplace=True)

            # Map sentiment scores using range-based mapping
            def map_sentiment_score(score):
                if score >= 0.75:
                    return 'Very Positive'
                elif score >= 0.5:
                    return 'Positive'
                elif -0.5 < score < 0.5:
                    return 'Neutral'
                elif -0.75 < score <= -0.5:
                    return 'Negative'
                elif score <= -0.75:
                    return 'Very Negative'
                else:
                    return 'Unknown'

            # Apply mapping function
            news_df['sentiment_category'] = news_df['sentiment_score'].apply(map_sentiment_score)

            # Debug output to ensure mapping is correct
            st.write("Mapped Sentiment Categories:", news_df['sentiment_category'].value_counts())

            sentiment_counts = news_df['sentiment_category'].value_counts()

            if sentiment_counts.empty or (len(sentiment_counts) == 1 and 'Unknown' in sentiment_counts.index):
                st.warning("No valid sentiment data available to create a pie chart.")
            else:
                # Convert sentiment counts to a list of dictionaries for ECharts data
                pie_data = [{"value": value, "name": key} for key, value in sentiment_counts.items()]

            # Ensure all counts are converted to int (for JSON serialization)
            sentiment_data = [
                {"value": int(sentiment_counts.get("Very Positive", 0)), "name": "Very Positive", "itemStyle": {"color": "#397439"}},  # Dark Green
                {"value": int(sentiment_counts.get("Positive", 0)), "name": "Positive", "itemStyle": {"color": "#6dd06d"}},  # Light Green
                {"value": int(sentiment_counts.get("Neutral", 0)), "name": "Neutral", "itemStyle": {"color": "#808080"}},  # Grey
                {"value": int(sentiment_counts.get("Negative", 0)), "name": "Negative", "itemStyle": {"color": "#FF474D"}},  # Light Red
                {"value": int(sentiment_counts.get("Very Negative", 0)), "name": "Very Negative", "itemStyle": {"color": "#AA2F33"}},  # Dark Red
            ]


            # Define options for the ECharts pie chart
            options = {
                "title": {
                    "text": "Sentiment Distribution of Top Recent News Articles",
                    "left": "center",
                    "textStyle": {
                        "color": "#FFFFFF"
                    }
                },
                "tooltip": {"trigger": "item"},
                "legend": {
                    "top": "5%",
                    "left": "center",
                    "textStyle": {
                        "color": "#FFFFFF"
                    }
                },
                "series": [
                    {
                        "name": "Sentiment",
                        "type": "pie",
                        "radius": ["40%", "70%"],  # Donut chart
                        "avoidLabelOverlap": False,
                        "data": sentiment_data,
                        "itemStyle": {
                            "borderRadius": 10,
                            "borderColor": "#fff",
                            "borderWidth": 2
                        },
                        "emphasis": {
                            "label": {
                                "show": True,
                                "fontSize": 18,
                                "fontWeight": "bold",
                                "formatter": "{b}\n{c} news articles",
                                "color": "#FFFFFF"
                            }
                        },
                        "label": {
                            "show": False,  # Hide default label to avoid clutter
                            "position": "center"
                        },
                        "labelLine": {
                            "show": False  # Hide lines for better emphasis effect
                        }
                    }
                ],
            }

            # Render the pie chart using ECharts
            st_echarts(options=options, height="600px")  # Increased height for better visibility

        # Display recommendation data with images for Buy/Hold/Sell
        if recommendation:
            st.markdown("---")
            
            # Display image based on recommendation type
            if recommendation['final_recommendation'] == 'Buy':
                st.image(BUY_IMAGE_URL, width=100)
            elif recommendation['final_recommendation'] == 'Hold':
                st.image(HOLD_IMAGE_URL, width=100)
            elif recommendation['final_recommendation'] == 'Sell':
                st.image(SELL_IMAGE_URL, width=100)
            
            st.markdown(f"**Recommendation Score**: {recommendation['recommendation_score']:.2f}")
            st.markdown(f"**Reasoning**: {recommendation['reasoning']}")
