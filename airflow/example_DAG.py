from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


def read_filter_csv(file_location: str = 'sample_data/202004-divvy-tripdata.csv') -> None:
    df = pd.read_csv(file_location)
    filtered_df = df.filter(df['rideable_type'] == 'docked_bike', axis=0)
    filtered_df.to_csv('sample_data/filtered_data.csv')


def calculate_metrics(data_file: str = 'sample_data/filtered_data.csv') -> None:
    df = pd.read_csv(data_file)
    df = df.groupby(['start_station_id']).count()
    df.to_csv('metrics.csv')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('bike_trip_data',
         start_date=datetime(2021, 4, 1),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

        raw_csv_data = PythonOperator(
            task_id='raw_csv_with_filter',
            python_callable=read_filter_csv,
        )

        transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=calculate_metrics,
        )

raw_csv_data >> transform_data