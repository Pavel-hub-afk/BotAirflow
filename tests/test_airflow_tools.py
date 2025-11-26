"""
Тестовый файл для проверки работы инструментов Airflow без LLM
"""
import os
import sys
import json
from dotenv import load_dotenv

# Добавляем путь к src в sys.path, чтобы можно было импортировать модули
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.airflow.airflow_client import AirflowClient
from src.bot.airflow_config_manager import AirflowConfigManager
from tests.llm_tools import AirflowTools

# Загружаем переменные окружения
load_dotenv()

def test_airflow_tools():
    """Функция для тестирования инструментов Airflow без LLM"""
    print("Тестируем инструменты Airflow...")
    
    # Загружаем конфигурацию DAG'ов
    dag_mapping_path = os.path.join(os.path.dirname(__file__), "..", "src", "bot", "config", "dag_mapping.json")
    if os.path.exists(dag_mapping_path):
        with open(dag_mapping_path, "r", encoding="utf-8") as f:
            dag_mapping = json.load(f)
    else:
        # Используем пример конфигурации из dag_mapping.json
        dag_mapping = {
            "dags": [
                {
                    "dag_name": "test_config_dag",
                    "config_variable": "pg_load_dm_enr_config",
                    "tables": [
                        "hrgate_employees_enr",
                        "hrgate_units_enr"
                    ],
                    "description": "Тестовый DAG для проверки изменений конфигурации. Выводит текущую конфигурацию из переменной pg_load_dm_enr_config в лог."
                }
            ]
        }

    # Создаем клиента Airflow
    airflow_url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    airflow_username = os.getenv("AIRFLOW_USERNAME", "airflow")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "airflow")
    
    print(f"Подключение к Airflow: {airflow_url}")
    
    airflow_client = AirflowClient(
        airflow_url=airflow_url,
        username=airflow_username,
        password=airflow_password
    )

    # Создаем менеджер конфигурации
    config_manager = AirflowConfigManager(
        airflow_client=airflow_client,
        config_variable_name="pg_load_dm_enr_config"  # Используем из маппинга первый DAG
    )

    # Создаем инструменты для LLM
    from tests.llm_tools import create_airflow_tools
    tools = create_airflow_tools(airflow_client, config_manager, dag_mapping)
    
    # Разделяем инструменты
    get_available_dags_tool = tools[0]
    trigger_dag_with_params_tool = tools[1]
    get_dag_run_status_tool = tools[2]
    get_dag_info_tool = tools[3]
    
    print("\n--- Тестирование доступных DAG'ов ---")
    try:
        available_dags = get_available_dags_tool.invoke({})
        print(f"Доступные DAG'и: {available_dags}")
    except Exception as e:
        print(f"Ошибка при получении доступных DAG'ов: {e}")
    
    print("\n--- Тестирование информации о конкретном DAG'е ---")
    try:
        dag_info = get_dag_info_tool.invoke({"dag_name": "test_config_dag"})
        print(f"Информация о DAG: {dag_info}")
    except Exception as e:
        print(f"Ошибка при получении информации о DAG: {e}")
    
    print("\n--- Тестирование запуска DAG с параметрами ---")
    try:
        # Тестовый запуск DAG с параметрами
        result = trigger_dag_with_params_tool.invoke({
            "dag_name": "test_config_dag",
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
            "tables": ["hrgate_employees_enr"]
        })
        print(f"Результат запуска DAG: {result}")
    except Exception as e:
        print(f"Ошибка при запуске DAG: {e}")
        print(f"Тип ошибки: {type(e).__name__}")

if __name__ == "__main__":
    test_airflow_tools()