import json
import os
from typing import Optional
from langchain_core.language_models import LanguageModelLike
from langchain_gigachat.chat_models import GigaChat


class LLMMessageParser:
    """
    Класс для интеграции LLM в телеграм-бот для парсинга пользовательских сообщений
    и извлечения параметров для запуска DAG'ов в Airflow
    """

    def __init__(self,
                 dag_mapping_path: str,
                 model: Optional[LanguageModelLike] = None):
        """
        Инициализация LLMMessageParser

        Args:
            dag_mapping_path: Путь к файлу сопоставления DAG'ов
            model: Модель LLM (если не указана, будет создана GigaChat)
        """
        # Загружаем конфигурацию DAG'ов
        if not os.path.exists(dag_mapping_path):
            raise FileNotFoundError(f"Файл конфигурации DAG'ов не найден: {dag_mapping_path}")

        with open(dag_mapping_path, "r", encoding="utf-8") as f:
            self.dag_mapping = json.load(f)

        # Если модель не передана, создаем GigaChat с настройками из переменных окружения
        if model is None:
            credentials = os.getenv("GIGACHAT_CREDENTIALS")
            if not credentials:
                raise ValueError("Не найдены учетные данные GigaChat API (GIGACHAT_CREDENTIALS)")

            scope = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")

            self.model = GigaChat(
                model="GigaChat-2-Pro",
                credentials=credentials,
                scope=scope,
                verify_ssl_certs=False,
                timeout=30,
                temperature=0.1,
            )
        else:
            self.model = model
    
    def parse_message(self, message: str) -> dict:
        """
        Парсит пользовательское сообщение с помощью LLM и возвращает параметры для запуска DAG.

        Args:
            message: Пользовательское сообщение

        Returns:
            Словарь с параметрами: dag_name, start_date, end_date, tables
            или None при ошибке
        """
        # Подготовим контекст с информацией о DAG'ах
        context = (
            f"Сопоставление таблиц и DAG'ов: {json.dumps(self.dag_mapping, ensure_ascii=False)}\n\n"
            f"Ты - ассистент для ручного запуска DAG'ов в Apache Airflow. "
            f"Твоя задача - обрабатывать пользовательские запросы на обновление данных за определенный период. "
            f"Когда пользователь запрашивает обновление таблицы за определенный период "
            f"(например: 'Обнови таблицу hrgate_employees_enr за июнь 2020-го'), "
            f"твоя задача: \n"
            f"- Найти подходящий DAG по маппингу, связанный с указанной таблицей\n"
            f"- Извлечь временной период из запроса (начало и конец периода), преобразовать даты в формат YYYY-MM-DD\n"
            f"- Подготовить параметры: tables (таблицы из запроса), start_date, end_date\n"
            f"- Формат дат: для июня 2020-го это будет '2020-06-01' и '2020-06-30'\n"
            f"- Для одного месяца: первая и последняя дата месяца\n"
            f"ВАЖНО: Не запускай DAG и не восстанавливай конфигурацию, просто возвращай параметры!\n"
            f"ВАЖНО: Ответ должен быть в формате JSON с полями: dag_name, start_date, end_date, tables\n"
            f"Пример ответа: {{\"dag_name\": \"test_config_dag\", \"start_date\": \"2024-02-01\", \"end_date\": \"2024-02-28\", \"tables\": [\"hrgate_units_enr\"]}}\n\n"
            f"Теперь извлеки параметры из следующего сообщения: {message}"
        )

        try:
            # Для парсинга сообщений будем использовать модель напрямую
            response = self.model.invoke([{"role": "user", "content": context}])
            content = response.content

            # Пытаемся извлечь JSON из ответа
            # Ищем JSON в формате {...}
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)

            if json_match:
                json_str = json_match.group(0)
                params = json.loads(json_str)

                # Возвращаем только нужные параметры
                return {
                    'dag_name': params.get('dag_name'),
                    'start_date': params.get('start_date'),
                    'end_date': params.get('end_date'),
                    'tables': params.get('tables')
                }

            return None

        except Exception as e:
            print(f"Ошибка при парсинге сообщения с помощью LLM: {e}")
            return None