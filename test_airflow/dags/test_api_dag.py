
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_api_dag',
    default_args=default_args,
    description='A simple test DAG for API testing',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello from Airflow API test!"',
    dag=dag,
)
