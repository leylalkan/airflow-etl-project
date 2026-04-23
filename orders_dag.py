import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from orders_etl_logic import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'orders_etl_dag',
    default_args=default_args,
    description='A DAG to run the orders ETL process',
    schedule_interval='@daily',
)

run_etl = PythonOperator(
    task_id='run_orders_etl',
    python_callable=main,
    dag=dag,
)
