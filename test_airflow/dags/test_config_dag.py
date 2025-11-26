from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
import logging

# Получаем логгер
logger = logging.getLogger(__name__)

# Функция для вывода конфигурации
def print_config(**context):
    try:
        # Получаем конфигурацию из переменных Airflow
        config_json = Variable.get("pg_load_dm_enr_config", default_var="{}")
        config = json.loads(config_json)
        
        # Выводим конфигурацию в лог
        logger.info("Current configuration:")
        logger.info(json.dumps(config, indent=2, ensure_ascii=False))
        
        # Также выводим в stdout для видимости в Airflow UI
        print("Current configuration:")
        print(json.dumps(config, indent=2, ensure_ascii=False))
        
        return "Configuration printed successfully"
    except Exception as e:
        logger.error(f"Error reading configuration: {str(e)}")
        raise

# Параметры DAG'а по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG'а
dag = DAG(
    'test_config_dag',
    default_args=default_args,
    description='Тестовый DAG для проверки изменений конфигурации',
    schedule_interval=None,  # Запуск только вручную
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test', 'config'],
)

# Задача для вывода конфигурации
print_config_task = PythonOperator(
    task_id='print_config',
    python_callable=print_config,
    dag=dag,
)

# Определяем последовательность выполнения задач
print_config_task