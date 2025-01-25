from airflow import DAG
from airflow.decorators import task
import pendulum
import requests
import psycopg2
from datetime import timedelta
from airflow.utils.state import State

# Stock symbol and API key
SYMBOL = 'IBM'
API_KEY = 'IG798MN7Z5KWSAX0'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
}

## Define the DAG
#with DAG('etl_ibm_stock', default_args=default_args, schedule=timedelta(seconds=5), catchup=False) as dag:

    #@task()
def extract_stock_data():
        """Extract IBM stock data from Alpha Vantage API."""

        # Build the API endpoint
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval=5min&apikey={API_KEY}'

        # Make the request
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            print(data)
            return data
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}")

    #@task()
def transform_stock_data(stock_data):
        """Transform the extracted stock data."""
        time_series = stock_data['Time Series (5min)']
        latest_timestamp = max(time_series.keys())
        latest_data = time_series[latest_timestamp]
        transformed_data = {
            'symbol': SYMBOL,
            'timestamp': latest_timestamp,
            'open': float(latest_data['1. open']),
            'high': float(latest_data['2. high']),
            'low': float(latest_data['3. low']),
            'close': float(latest_data['4. close']),
            'volume': int(latest_data['5. volume'])
        }
       
        return transformed_data

    #@task()
def load_stock_data(transformed_data):
        """Load the transformed stock data into PostgreSQL."""
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="stockprice",
            user="mahdi",
            password="123456789"
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            timestamp TIMESTAMP,
            symbol VARCHAR(10),
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            PRIMARY KEY (symbol, timestamp)
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO stock_data (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """, (
            transformed_data['symbol'],
            transformed_data['timestamp'],
            transformed_data['open'],
            transformed_data['high'],
            transformed_data['low'],
            transformed_data['close'],
            transformed_data['volume']
        ))

        conn.commit()
        cursor.close()
        conn.close()

        # Define the task dependencies
        """
        stock_data = extract_stock_data()
        transformed_data = transform_stock_data(stock_data)
        load_stock_data(transformed_data)
        """

# Test the function
if __name__ == '__main__':
    stock_data = extract_stock_data()
    transformed_data = transform_stock_data(stock_data)
    load_stock_data(transformed_data)
