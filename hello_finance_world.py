from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello_finance():
    print("Hello, Finance World! This is your first automated DAG run")
    
default_args = {
    'owner': 'finance_team',
    'start_date': datetime(2025, 4, 28),
    'retries': 2
}

with DAG(
    dag_id="hello_finance_world",
    default_args=default_args,
    description="A simple Hello World DAG for the finance World",
    schedule_interval=None,
    catchup=False,
    tags=['finance', 'hello-world'],
) as dag:
    hello_task = PythonOperator(
        task_id='print_hello_finance',
        python_callable=print_hello_finance,
    )