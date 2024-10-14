# Stock Price Prediction 

This project implements a data pipeline that extracts stock data for **Microsoft (MSFT)** and **Meta (META)** from the Alpha Vantage API, stores the data in Snowflake, and predicts future stock values for the next 7 days using machine learning forecasting models. The pipeline is managed using Apache Airflow for task orchestration.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Folder Structure](#folder-structure)
- [DAGs](#dags)
  - [ETL Stock Data DAG](#etl-stock-data-dag)
  - [ML Forecasting DAG](#ml-forecasting-dag)
- [How to Run the Project](#how-to-run-the-project)
- [Snowflake Tables and Views](#snowflake-tables-and-views)

## Project Overview

The project is divided into two primary tasks:
1. **Extract, Transform, Load (ETL)**: Extracts the last 90 days of stock price data for Microsoft and Meta, transforms the data, and loads it into Snowflake.
2. **Machine Learning Forecasting**: Uses historical stock price data to forecast stock prices for the next 7 days using Snowflake's built-in machine learning capabilities.

Both tasks are coordinated through **Apache Airflow** and are scheduled to run daily.

## Technologies Used
- **Python**: For task implementation and orchestration.
- **Apache Airflow**: To manage and schedule tasks as Directed Acyclic Graphs (DAGs).
- **Snowflake**: For data storage and machine learning forecasting.
- **Alpha Vantage API**: To fetch historical stock data.
- **Snowflake ML**: For forecasting stock prices.

## Folder Structure
Stock_Price_Prediction/ │ ├── dags/ │ ├── etl_stock_data.py # DAG for the ETL process │ ├── ml_forecasting.py # DAG for the ML Forecasting process │ └── README.md # Project documentation

## DAGs

### ETL Stock Data DAG
This DAG extracts stock data from Alpha Vantage's API, transforms it, and loads it into Snowflake.

- **Schedule**: Runs every day at 2:00 AM.
- **Tasks**:
  - `extract_last_90d_price`: Fetches stock prices for Microsoft and Meta for the last 90 days.
  - `transform`: Merges and transforms the extracted data.
  - `load`: Loads the transformed data into Snowflake's `stock.stocks.market_data` table.

### ETL Code Overview

# Example task: Extracting stock data
@task
def extract_last_90d_price(symbol):
    api_key = Variable.get('alpha_vantage_api_key')
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    ....'''

## ML Forecasting DAG

This DAG trains a forecasting model using the stock data and predicts future stock prices for the next 7 days.

- **Schedule**: Runs every day at 2:30 AM.
- **Tasks**:
  - **train**: Creates a Snowflake ML forecasting model based on the past 90 days of stock data.
  - **predict**: Generates a 7-day forecast for Microsoft and Meta stock prices and stores the results.

### ML Forecasting Code Overview

# Example task: Train ML model
@task
def train(train_input_table, train_view, forecast_function_name):
    create_model_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        ...
    )
    """
    cur.execute(create_model_sql)

# How to Run the Project

# Prerequisites
- Apache Airflow: Ensure Airflow is installed and running.
- Snowflake Account: Set up a Snowflake account and create the necessary schemas and tables.
- Alpha Vantage API Key: Obtain an API key from Alpha Vantage.
  
# Steps
- Set up Airflow variables:
- alpha_vantage_api_key: Your Alpha Vantage API key.
- url: The API request URL template.
- Configure Snowflake connection in Airflow (via snowflake_conn).

- Deploy the DAGs:
- Place etl_stock_data.py and ml_forecasting.py in your Airflow dags/ directory.
- Start Airflow and trigger the DAGs from the UI.

# Snowflake Tables and Views

- Table: stock.stocks.market_data: Stores historical stock price data for Microsoft and Meta.
- View: stock.adhoc.market_data_view: Used for training the machine learning forecasting model.
- Table: stock.adhoc.market_data_forecast: Stores the predicted stock prices for the next 7 days.
- Table: stock.analytics.market_data: Combines actual and predicted data for analysis.





