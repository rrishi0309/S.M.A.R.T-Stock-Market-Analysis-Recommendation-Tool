# 📈 S.M.A.R.T - Stock Market Analysis & Recommendation Tool

S.M.A.R.T (Stock Market Analysis & Recommendation Tool) is an AI-driven system that analyzes stock market trends and news sentiment to generate **Buy, Hold, or Sell** recommendations. Built using **Google Cloud** and **AI-powered sentiment analysis**, this project enhances financial decision-making with data-driven insights.

## 🚀 Key Features

### 🔹 AI-Driven Sentiment Analysis
- Utilizes **Gemini 1.5 Pro** to analyze sentiment from financial news articles.
- Scores sentiment from **-1 (Very Negative) to 1 (Very Positive)**.

### 🔹 Stock Trend Calculation
- Analyzes stock prices using **Moving Averages** & **Percentage Changes**.
- Computes an **Overall Trend Score** for better market insight.

### 🔹 Dynamic Recommendation Engine
- Uses a **Weighted Formula**:  
  \[ R = (w_{sentiment} \times S) + (w_{trend} \times T) \]  
  where:
  - \( R \) = Recommendation Score
  - \( w_{sentiment} \) = Sentiment Weight
  - \( w_{trend} \) = Trend Weight
  - \( S \) = Sentiment Score
  - \( T \) = Trend Score
- Dynamically adjusts weights based on market conditions.

### 🔹 Scalable Infrastructure
- **Google Cloud Functions & Cloud Run** for serverless execution.
- **Google Workflows** to orchestrate AI, stock analysis, and sentiment processing.
- **Google Cloud Logging** for monitoring and debugging.

### 🔹 Optimized Performance
- **ThreadPoolExecutor** is used to process multiple news articles concurrently, reducing response time.
- Achieves up to **5x speed improvement** over sequential processing.

## 🛠️ Tech Stack
- **Google Cloud:** Cloud Functions, Cloud Run, Workflows, Secret Manager, Cloud Logging
- **AI & ML:** Gemini 1.5 Pro (Google Generative AI)
- **Python:** Pandas, SQLAlchemy, ThreadPoolExecutor
- **Database:** PostgreSQL (Google Cloud SQL)
- **Visualization:** Streamlit, ECharts

## 📌 How It Works
1. Fetches **real-time stock data** and **recent financial news**.
2. Analyzes **news sentiment** using **Gemini AI**.
3. Computes **stock trends** using **historical price data**.
4. Generates a **weighted recommendation score**.
5. Displays results on an **interactive dashboard**.

## 📹 Screen Recording

https://github.com/user-attachments/assets/9912d0ed-aafc-4682-9f0d-84d19b606d44

## 📸 Screenshots
![image](https://github.com/user-attachments/assets/f6c5c8be-76f0-41fd-b8a8-694138eb386b)
![image](https://github.com/user-attachments/assets/75b3fdc0-692c-486f-b83c-ff9d8917b8e7)
![image](https://github.com/user-attachments/assets/6c05bfeb-9072-496b-944d-519c50f9c1ac)
![image](https://github.com/user-attachments/assets/3f571a8e-4cc0-4755-86be-aeaa8f84cd41)


## 📚 Installation & Setup
```bash
# Clone the repository
git clone https://github.com/your-username/smart-stock-analysis.git
cd smart-stock-analysis

# Install dependencies
pip install -r requirements.txt

# Integrate all the workflow functions with final.py

# Create Database for the functioning

# Run the Streamlit app
streamlit run final.py
```
---

## 💎 Contact
### Developed by **Rishi Ramesh**  
### 🔗 LinkedIn: https://www.linkedin.com/in/rishi0309/  

---
## ⭐ Star this Repo
If you found this project helpful, please give it a ⭐!
