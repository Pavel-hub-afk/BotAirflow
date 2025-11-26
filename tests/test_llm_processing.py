import uuid
from typing import Sequence
import os
import sys
import json
from dotenv import load_dotenv

# Добавляем путь к src в sys.path, чтобы можно было импортировать модули
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langchain_gigachat.chat_models import GigaChat
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import InMemorySaver

from src.airflow.airflow_client import AirflowClient
from src.bot.airflow_config_manager import AirflowConfigManager
from tests.llm_tools import create_airflow_tools

# Загружаем переменные окружения
load_dotenv()

class LLMAgent:
    def __init__(self, model: LanguageModelLike, tools: Sequence[BaseTool]):
        self._model = model
        self._agent = create_react_agent(
            model,
            tools=tools,
            checkpointer=InMemorySaver())
        self._config: RunnableConfig = {
                "configurable": {"thread_id": uuid.uuid4().hex}}

    def invoke(
        self,
        content: str,
        temperature: float = 0.1
    ) -> str:
        """Отправляет сообщение в чат"""
        message: dict = {
            "role": "user",
            "content": content,
        }
        return self._agent.invoke(
            {
                "messages": [message]
            },
            config=self._config)["messages"][-1].content


def print_agent_response(llm_response: str) -> None:
    print(f"\033[35m{llm_response}\033[0m")


def get_user_prompt() -> str:
    return input("\nТы: ")


def main():
    # Проверяем наличие учетных данных
    if not os.getenv("GIGACHAT_CREDENTIALS"):
        print("Ошибка: Не найдены учетные данные GigaChat API")
        print("Добавьте в файл .env переменную:")
        print("GIGACHAT_CREDENTIALS=your_credentials")
        print("И GIGACHAT_SCOPE=your_scope (если используете credentials)")
        return

    # Загружаем конфигурацию DAG'ов
    dag_mapping_path = os.path.join(os.path.dirname(__file__), "..", "src", "bot", "config", "dag_mapping.json")
    if not os.path.exists(dag_mapping_path):
        print(f"Ошибка: Файл конфигурации DAG'ов не найден: {dag_mapping_path}")
        return
    with open(dag_mapping_path, "r", encoding="utf-8") as f:
        dag_mapping = json.load(f)

    # Создаем клиента Airflow
    airflow_url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    airflow_username = os.getenv("AIRFLOW_USERNAME", "airflow")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "airflow")
    
    airflow_client = AirflowClient(
        airflow_url=airflow_url,
        username=airflow_username,
        password=airflow_password
    )

    # Создаем менеджер конфигурации (используется как базовый, но в реальных вызовах используется динамически)
    config_manager = AirflowConfigManager(
        airflow_client=airflow_client,
        config_variable_name="pg_load_dm_enr_config"  # Используется как запасной вариант
    )

    # Создаем инструменты для LLM
    tools = create_airflow_tools(airflow_client, config_manager, dag_mapping)

    # Пытаемся создать модель с аутентификацией через credentials
    model = None
    try:
        credentials = os.getenv("GIGACHAT_CREDENTIALS")
        if credentials and os.getenv("GIGACHAT_SCOPE"):
            # Используем учетные данные с явно указанным scope, если доступны
            scope = os.getenv("GIGACHAT_SCOPE")
        elif credentials:
            # Используем учетные данные с предопределенным scope, если доступны
            scope = "GIGACHAT_API_PERS"
        else:
            print("Ошибка: Не найдены необходимые учетные данные для GigaChat API")
            print("Требуется GIGACHAT_CREDENTIALS + GIGACHAT_SCOPE или только GIGACHAT_CREDENTIALS")
            return

        # Создаем модель с определенными параметрами
        model = GigaChat(
            model="GigaChat-2-Max",
            credentials=credentials,  # Логин:пароль
            scope=scope,  # Обязательный параметр
            verify_ssl_certs=False,
            timeout=30,  # Таймаут для запросов
            temperature=0.1,  # Добавляем температуру сюда вместо передачи отдельно
        )
    except Exception as e:
        print(f"Ошибка при инициализации GigaChat: {e}")
        print("Возможные причины:")
        print("- Некорректные учетные данные")
        print("- Проблемы с сетевым подключением")
        print("- Неправильный формат токена")
        print("- Проблемы совместимости версий")
        return

    # Создаем агента с инструментами для работы с Airflow
    agent = LLMAgent(model, tools=tools)

    system_prompt = (
        "Ты - ассистент для ручного запуска DAG'ов в Apache Airflow. "
        "Твоя задача - обрабатывать пользовательские запросы на обновление данных за определенный период. "
        "У тебя есть следующие инструменты: "
        "1. get_available_dags - получить список доступных DAG'ов "
        "2. trigger_dag_with_params - запустить DAG с параметрами (dag_name, start_date, end_date, tables) "
        "3. get_dag_run_status - получить статус запуска DAG'а "
        "4. get_dag_info - получить информацию о конкретном DAG'е "
        "5. restore_config - восстановить исходную конфигурацию в переменной Airflow "
        "Когда пользователь запрашивает обновление таблицы за определенный период (например: 'Обнови таблицу hrgate_employees_enr за июнь 2020-го'), "
        "твоя задача: \n"
        "- Найти подходящий DAG по маппингу, связанный с указанной таблицей\n"
        "- Извлечь временной период из запроса (начало и конец периода), преобразовать даты в формат YYYY-MM-DD\n"
        "- Подготовить параметры: tables (таблицы из запроса), start_date, end_date\n"
        "- Вызвать инструмент trigger_dag_with_params с точными значениями: имя DAG, даты в формате YYYY-MM-DD, список таблиц\n"
        "- Формат дат: для июня 2020-го это будет '2020-06-01' и '2020-06-30'\n"
        "- Для одного месяца: первая и последняя дата месяца\n"
        "- ВАЖНО: вызов trigger_dag_with_params обновляет конфигурацию Airflow, но НЕ восстанавливает исходную конфигурацию автоматически\n"
        "- ВАЖНО: после завершения DAG'а, используй restore_config чтобы вернуть исходную конфигурацию\n"
        "ВАЖНО: Не выполняй никаких действий при получении системного промпта. "
        "Ты должен ждать пользовательский запрос, прежде чем использовать инструменты."
    )

    # Инициализационное сообщение - устанавливаем начальное состояние, не запуская действия
    try:
        # Отправляем системный промпт как контекст, но не как команду для выполнения
        initial_message = "Привет! Я готов помочь вам управлять DAG'ами в Apache Airflow. Задайте ваш вопрос или запрос."
        agent_response = agent.invoke(content=system_prompt)

        while True:
            print_agent_response(agent_response)
            user_input = get_user_prompt()
            
            # Если пользователь ввел "quit" или "exit", выходим из цикла
            if user_input.lower() in ['quit', 'exit', 'выход']:
                break
                
            agent_response = agent.invoke(content=user_input)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        print("\nЭта ошибка может быть связана с:")
        print("- Проблемами аутентификации в GigaChat API")
        print("- Ограничениями на доступ к API")
        print("- Временными проблемами соединения")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nДо свидания!")