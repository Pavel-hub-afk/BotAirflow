import os
import sys
from dotenv import load_dotenv

# Добавляем src в путь поиска модулей
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.bot.telegram_bot import TelegramBot
from src.airflow.airflow_client import AirflowClient
from src.bot.airflow_config_manager import AirflowConfigManager
from src.bot.llm_parser import LLMMessageParser

def main():
    # Загружаем переменные окружения из .env файла
    load_dotenv()
    
    # Получаем параметры из переменных окружения
    telegram_token = os.getenv("TELEGRAM_TOKEN")
    airflow_url = os.getenv("AIRFLOW_URL")
    airflow_username = os.getenv("AIRFLOW_USERNAME")
    airflow_password = os.getenv("AIRFLOW_PASSWORD")
    allowed_user_ids_str = os.getenv("ALLOWED_USER_IDS")

    # Проверяем, что все необходимые параметры заданы
    if not all([telegram_token, airflow_url, airflow_username, airflow_password, allowed_user_ids_str]):
        print("Пожалуйста, заполните все параметры в файле .env")
        return
    
    # Преобразуем строку с ID пользователей в список целых чисел
    try:
        allowed_user_ids = [int(id.strip()) for id in allowed_user_ids_str.split(",") if id.strip()]
    except ValueError:
        print("Неверный формат ALLOWED_USER_IDS в файле .env. Должны быть целые числа, разделенные запятыми.")
        return
    
    # Инициализируем клиент Airflow
    airflow_client = AirflowClient(
        airflow_url=airflow_url,
        username=airflow_username,
        password=airflow_password,
        verify_ssl=False  # отключаем проверку SSL (для тестов)
    )

    # Путь к файлу сопоставления DAG'ов
    dag_mapping_path = os.path.join(os.path.dirname(__file__), "bot", "config", "dag_mapping.json")

    # Создаем LLM-парсер
    try:
        llm_parser = LLMMessageParser(
            dag_mapping_path=dag_mapping_path
        )
    except Exception as e:
        print(f"Ошибка при создании LLM-парсера: {e}")
        return

    # Создаем и запускаем бота
    bot = TelegramBot(
        token=telegram_token,
        airflow_client=airflow_client,
        allowed_user_ids=allowed_user_ids,
        llm_parser=llm_parser
    )

    bot.run()

if __name__ == "__main__":
    main()