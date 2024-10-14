from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime, timedelta

# Defining default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initializing the ETL DAG
etl_dag = DAG(
    'etl_stock_data',
    default_args=default_args,
    description='ETL process for extracting, transforming and loading Microsoft and Meta stock data',
    schedule_interval='0 2 * * *',  # Runs every day at 2:00 AM
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Extracting stock data for Microsoft and Meta from Alpha Vantage
@task
def extract_last_90d_price(symbol):
    api_key = Variable.get('alpha_vantage_api_key')  # Airflow Variable for API key
    url_template = Variable.get("url")  # Airflow Variable for URL template
    url = url_template.format(symbol=symbol, vantage_api_key=api_key)
    response = requests.get(url)
    data = response.json()

    results = []
    ninety_days_ago = datetime.today() - timedelta(days=90)

    # Extract data for the last 90 days
    for d in data.get("Time Series (Daily)", {}):
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= ninety_days_ago:
            price_data = {
                "symbol": symbol,
                "date": d,
                "open": data["Time Series (Daily)"][d]["1. open"],
                "high": data["Time Series (Daily)"][d]["2. high"],
                "low": data["Time Series (Daily)"][d]["3. low"],
                "close": data["Time Series (Daily)"][d]["4. close"],
                "volume": data["Time Series (Daily)"][d]["5. volume"]
            }
            results.append(price_data)

    return results

# Task 2: Transforming the data using the @task decorator
@task
def transform(msft_data: list, meta_data: list):
    processed_data = []
    processed_data.extend(msft_data)
    processed_data.extend(meta_data)

    print(f"Processed Data: {json.dumps(processed_data, indent=2)}")
    return processed_data

# Task 3: Loading data into Snowflake using Airflow Connection
@task
def load(records):
    if not records:
        print("No records to load.")
        return

    # Fetching Snowflake connection via Airflow connection
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  # Airflow Connection for Snowflake
    conn = hook.get_conn()
    cur = conn.cursor()

    target_table = "stock.stocks.market_data"  # Target table in Snowflake

    try:
        # Create table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR,
            date DATE,
            open NUMBER,
            max NUMBER,
            min NUMBER,
            close NUMBER,
            volume NUMBER,
            PRIMARY KEY (date, symbol)
        )
        """)

        # Insert records into Snowflake table
        for r in records:
            symbol = r['symbol']
            date = r['date']
            open_price = r['open']
            high_price = r['high']
            low_price = r['low']
            close_price = r['close']
            volume = r['volume']

            sql = f"""
            INSERT INTO {target_table} (symbol, date, open, max, min, close, volume)
            VALUES ('{symbol}', TO_DATE('{date}', 'YYYY-MM-DD'), {open_price}, {high_price}, {low_price}, {close_price}, {volume})
            """
            cur.execute(sql)

        conn.commit()
        print(f"Successfully loaded {len(records)} records into {target_table}.")

    except Exception as e:
        conn.rollback()
        print(f"Error loading data into Snowflake: {e}")
    finally:
        cur.close()
        conn.close()

# Defining the task dependencies using the decorator functions
with etl_dag:
    # Extracting data for both companies
    msft_data = extract_last_90d_price('MSFT')
    meta_data = extract_last_90d_price('META')

    # Transforming the data from both companies
    transformed_data = transform(msft_data, meta_data)
    
    # Loading the transformed data
    load(transformed_data)