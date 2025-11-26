import requests
from typing import Optional, Dict, Any
import logging
import urllib3

class AirflowClient:
    """
    Базовый класс для взаимодействия с Apache Airflow через REST API.
    Позволяет запускать DAG'и вручную.
    """
    
    def __init__(
        self, 
        airflow_url: str, 
        username: Optional[str] = None, 
        password: Optional[str] = None, 
        token: Optional[str] = None,
        verify_ssl: bool = True,
        ssl_cert_path: Optional[str] = None
    ):
        """
        Инициализация клиента Airflow.
        
        :param airflow_url: URL инстанса Airflow (например, http://localhost:8080)
        :param username: Имя пользователя для аутентификации (если используется Basic Auth)
        :param password: Пароль для аутентификации (если используется Basic Auth)
        :param token: Токен для аутентификации (если используется Token Auth)
        :param verify_ssl: Проверять SSL сертификаты (False для самоподписанных сертификатов)
        :param ssl_cert_path: Путь к SSL сертификату (опционально)
        """
        self.airflow_url = airflow_url.rstrip('/')
        self.session = requests.Session()
        
        # Настройка SSL
        if not verify_ssl:
            self.session.verify = False
            # Отключаем предупреждения о небезопасных SSL запросах
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        elif ssl_cert_path:
            self.session.verify = ssl_cert_path
        
        # Установка заголовков аутентификации
        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})
        elif username and password:
            self.session.auth = (username, password)
        
        # Установка заголовков по умолчанию
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Настройка логирования
        self.logger = logging.getLogger(__name__)
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Запуск DAG вручную.
        
        :param dag_id: ID DAG'а для запуска
        :param conf: Конфигурационные параметры для DAG (опционально)
        :param run_id: Уникальный идентификатор запуска (опционально)
        :return: Ответ от API Airflow
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {}
        if conf:
            payload['conf'] = conf
        if run_id:
            payload['dag_run_id'] = run_id
            
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            self.logger.info(f"Successfully triggered DAG {dag_id} with run_id {result.get('dag_run_id')}")
            return result
        except requests.exceptions.SSLError as e:
            self.logger.error(f"SSL error occurred while triggering DAG {dag_id}: {str(e)}")
            self.logger.error("Consider setting verify_ssl=False or providing proper SSL certificate")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to trigger DAG {dag_id}: {str(e)}")
            raise
    
    def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Получение статуса конкретного запуска DAG.
        
        :param dag_id: ID DAG'а
        :param dag_run_id: ID запуска DAG
        :return: Ответ от API Airflow с информацией о статусе
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            result = response.json()
            self.logger.info(f"Retrieved status for DAG {dag_id}, run {dag_run_id}: {result['state']}")
            return result
        except requests.exceptions.SSLError as e:
            self.logger.error(f"SSL error occurred while getting DAG status {dag_id}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get status for DAG {dag_id}, run {dag_run_id}: {str(e)}")
            raise
    
    def get_dag_runs(self, dag_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        Получение списка запусков DAG.
        
        :param dag_id: ID DAG'а
        :param limit: Максимальное количество возвращаемых записей
        :return: Ответ от API Airflow со списком запусков
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns?limit={limit}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            result = response.json()
            self.logger.info(f"Retrieved {len(result.get('dag_runs', []))} runs for DAG {dag_id}")
            return result
        except requests.exceptions.SSLError as e:
            self.logger.error(f"SSL error occurred while getting DAG runs {dag_id}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get runs for DAG {dag_id}: {str(e)}")
            raise

# Пример использования:
# client = AirflowClient(
#     airflow_url="http://localhost:8080",
#     username="admin",
#     password="admin"
# )
# 
# # Запуск DAG без дополнительных параметров
# result = client.trigger_dag("example_dag")
# print(result)
# 
# # Запуск DAG с конфигурацией
# conf = {"param1": "value1", "param2": "value2"}
# result = client.trigger_dag("example_dag", conf=conf)
# print(result)
# 
# # Получение статуса конкретного запуска
# status = client.get_dag_run_status("example_dag", "manual__2023-01-01T00:00:00+00:00")
# print(status)