from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os

# Save CSVs into /usr/local/airflow/include
OUTPUT_DIR = '/usr/local/airflow/include'

def fetch_and_save_stock_data(ticker: str, start_date: str, end_date: str, output_dir: str):
    data = yf.download(ticker, start=start_date, end=end_date)
    
    if data.empty:
        raise ValueError(f"No data fetched for {ticker}")

    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, f"{ticker}_{start_date}_to_{end_date}.csv")
    data.to_csv(file_path)
    print(f"Saved {ticker} data to {file_path}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_yahoo_finance_to_csv',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['finance', 'yahoo', 'csv'],
) as dag:
    """
        DAG Name: Fetch Yahoo Finance Data and Save as CSV

        Description:
        This DAG fetches historical stock price data for a specified ticker symbol from Yahoo Finance using the yfinance library.
        The fetched data is then saved as a CSV file into the Airflow project's 'include/' directory, 
        which is accessible both inside the container and from the local filesystem.

        Key Features:
        - Fetches stock data for a configurable ticker, start date, and end date.
        - Automatically creates the output directory if it does not exist.
        - Saves the output as a CSV file named with the ticker symbol and date range.
        - Designed for local Astro CLI Airflow development environments.
        - CSV files are saved inside the 'include/' folder for easy access on the host machine.

        Usage:
        - Adjust the 'ticker', 'start_date', and 'end_date' in the PythonOperator's `op_kwargs`.
        - Schedule can be modified as needed (default: daily).

        Example Output:
        File: include/AAPL_2024-01-01_to_2024-12-31.csv

        Tags:
        ['finance', 'yahoo', 'csv', 'astro', 'local-development']
    """

    
    fetch_stock_data_task = PythonOperator(
        task_id='fetch_and_save_stock_data',
        python_callable=fetch_and_save_stock_data,
        op_kwargs={
            'ticker': 'AAPL',
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'output_dir': OUTPUT_DIR,
        },
    )

    fetch_stock_data_task
