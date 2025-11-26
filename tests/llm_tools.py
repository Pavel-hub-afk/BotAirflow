from typing import List, Dict, Any
import json
from datetime import datetime
from langchain_core.tools import tool

from src.airflow.airflow_client import AirflowClient
from src.bot.airflow_config_manager import AirflowConfigManager


class AirflowTools:
    """
    Класс с инструментами для LLM, позволяющими взаимодействовать с Airflow
    """

    def __init__(self, airflow_client: AirflowClient, config_manager: AirflowConfigManager, dag_mapping: Dict[str, Any]):
        self.airflow_client = airflow_client
        self.config_manager = config_manager  # This is used as a default config manager
        self.dag_mapping = dag_mapping

    def get_available_dags(self) -> List[Dict[str, Any]]:
        """
        Возвращает список доступных DAG'ов с их описанием и связанными таблицами.
        Полезно, когда пользователь спрашивает, какие DAG'и доступны для запуска.
        """
        dags_info = []
        for dag_info in self.dag_mapping.get('dags', []):
            dags_info.append({
                'dag_name': dag_info['dag_name'],
                'config_variable': dag_info['config_variable'],
                'tables': dag_info['tables'],
                'description': dag_info['description']
            })
        return dags_info

    def trigger_dag_with_params(self, dag_name: str, start_date: str, end_date: str, tables: List[str] = None) -> str:
        """
        Запускает DAG с заданными параметрами.

        Args:
            dag_name (str): Имя DAG'а для запуска
            start_date (str): Дата начала в формате YYYY-MM-DD
            end_date (str): Дата окончания в формате YYYY-MM-DD
            tables (List[str]): Список таблиц для обновления (опционально)

        Returns:
            str: Результат запуска DAG'а
        """
        try:
            # Проверяем, существует ли такой DAG в маппинге
            dag_info = None
            for dag in self.dag_mapping.get('dags', []):
                if dag['dag_name'] == dag_name:
                    dag_info = dag
                    break

            if not dag_info:
                return f"Ошибка: DAG '{dag_name}' не найден в конфигурации."

            # Если таблицы не указаны, используем таблицы из маппинга
            if tables is None:
                tables = dag_info['tables']

            # Создаем временный config manager для нужной переменной конфигурации
            config_variable_name = dag_info.get('config_variable', 'pg_load_dm_enr_config')  # default fallback
            temp_config_manager = AirflowConfigManager(
                airflow_client=self.airflow_client,
                config_variable_name=config_variable_name
            )

            # Обновляем конфигурацию в переменных Airflow с учетом полной структуры
            temp_config_manager.update_config(start_date, end_date, tables)

            # Запускаем DAG
            dag_run_response = self.airflow_client.trigger_dag(
                dag_id=dag_name,
                conf={
                    "load_start_date": start_date,
                    "load_end_date": end_date,
                    "only_load_tables": tables
                }
            )

            dag_run_id = dag_run_response.get('dag_run_id')

            # ВАЖНО: Не восстанавливаем конфигурацию сразу, чтобы DAG мог использовать обновленные параметры
            # Вместо этого, пользователь может вызвать восстановление вручную при необходимости
            # или использовать отдельную логику для восстановления по завершении DAG
            return f"DAG '{dag_name}' успешно запущен с ID запуска: {dag_run_id}. Период: {start_date} - {end_date}. Таблицы: {', '.join(tables)}"

        except Exception as e:
            # Восстанавливаем конфигурацию в случае ошибки
            try:
                if 'temp_config_manager' in locals():
                    temp_config_manager.restore_config()
            except:
                pass  # Игнорируем ошибки восстановления конфига при обработке другой ошибки

            return f"Ошибка при запуске DAG '{dag_name}': {str(e)}"

    def get_dag_run_status(self, dag_name: str, dag_run_id: str) -> str:
        """
        Получает статус запуска DAG'а.
        
        Args:
            dag_name (str): Имя DAG'а
            dag_run_id (str): ID запуска DAG'а
            
        Returns:
            str: Статус запуска DAG'а
        """
        try:
            status_response = self.airflow_client.get_dag_run_status(dag_name, dag_run_id)
            state = status_response.get('state')
            duration = status_response.get('end_date', '') - status_response.get('start_date', '') if status_response.get('start_date') and status_response.get('end_date') else 'N/A'
            return f"Статус DAG '{dag_name}' (ID: {dag_run_id}): {state}. Длительность: {duration}"
        except Exception as e:
            return f"Ошибка при получении статуса DAG '{dag_name}': {str(e)}"

    def get_dag_info(self, dag_name: str) -> Dict[str, Any]:
        """
        Возвращает информацию о конкретном DAG'е.

        Args:
            dag_name (str): Имя DAG'а

        Returns:
            Dict: Информация о DAG'е
        """
        for dag_info in self.dag_mapping.get('dags', []):
            if dag_info['dag_name'] == dag_name:
                return {
                    'dag_name': dag_info['dag_name'],
                    'config_variable': dag_info['config_variable'],
                    'tables': dag_info['tables'],
                    'description': dag_info['description']
                }
        return {
            'dag_name': dag_name,
            'config_variable': "unknown",
            'tables': [],
            'description': "DAG не найден в конфигурации"
        }

    def restore_config(self, config_variable_name: str = "pg_load_dm_enr_config") -> str:
        """
        Восстанавливает исходную конфигурацию в переменной Airflow.
        Используется для ручного восстановления конфигурации после завершения DAG.

        Args:
            config_variable_name (str): Название переменной конфигурации для восстановления

        Returns:
            str: Результат восстановления
        """
        try:
            # Создаем временный config manager для нужной переменной конфигурации
            temp_config_manager = AirflowConfigManager(
                airflow_client=self.airflow_client,
                config_variable_name=config_variable_name
            )
            temp_config_manager.restore_config()
            return f"Конфигурация для переменной '{config_variable_name}' успешно восстановлена в исходное состояние"
        except Exception as e:
            return f"Ошибка при восстановлении конфигурации: {str(e)}"

# Создаем инструменты как отдельные функции с сохранением внутреннего контекста
def create_airflow_tools(airflow_client: AirflowClient, config_manager: AirflowConfigManager, dag_mapping: Dict[str, Any]):
    """Создает инструменты Airflow с правильно настроенным контекстом"""
    tools_instance = AirflowTools(airflow_client, config_manager, dag_mapping)

    # Создаем инструменты с правильным контекстом
    @tool
    def get_available_dags() -> List[Dict[str, Any]]:
        """Возвращает список доступных DAG'ов с их описанием и связанными таблицами."""
        return tools_instance.get_available_dags()

    @tool
    def trigger_dag_with_params(dag_name: str, start_date: str, end_date: str, tables: List[str] = None) -> str:
        """
        Запускает DAG с заданными параметрами.
        """
        return tools_instance.trigger_dag_with_params(dag_name, start_date, end_date, tables)

    @tool
    def get_dag_run_status(dag_name: str, dag_run_id: str) -> str:
        """
        Получает статус запуска DAG'а.
        """
        return tools_instance.get_dag_run_status(dag_name, dag_run_id)

    @tool
    def get_dag_info(dag_name: str) -> Dict[str, Any]:
        """
        Возвращает информацию о конкретном DAG'е.
        """
        return tools_instance.get_dag_info(dag_name)

    @tool
    def restore_config(config_variable_name: str = "pg_load_dm_enr_config") -> str:
        """
        Восстанавливает исходную конфигурацию в переменной Airflow.
        Используется для ручного восстановления конфигурации после завершения DAG.
        """
        return tools_instance.restore_config(config_variable_name)

    return [get_available_dags, trigger_dag_with_params, get_dag_run_status, get_dag_info, restore_config]