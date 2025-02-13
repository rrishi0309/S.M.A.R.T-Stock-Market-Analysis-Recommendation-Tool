# Base image with Python
FROM python:3.9-slim

# Create working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt ./requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Fix the yfinance bug
RUN sed -i "s/\"query not set\"/'query not set'/" /usr/local/lib/python3.9/site-packages/yfinance/screener/screener.py

# Copy the rest of the application files
COPY . /app

# Expose port 8080 for the application
EXPOSE 8080

# Run the Streamlit app
CMD ["streamlit", "run", "final.py", "--server.port=8080", "--server.enableCORS=false", "--server.enableWebsocketCompression=false"]
