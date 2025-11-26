import json
import logging
from typing import List

logger = logging.getLogger(__name__)

class AirflowConfigManager:
    """Класс для управления конфигурацией в переменных Airflow"""
    
    def __init__(self, airflow_client, config_variable_name: str):
        self.airflow_client = airflow_client
        self.config_variable_name = config_variable_name
        self.original_config = None
        
    def backup_config(self):
        """Получение и сохранение исходной конфигурации"""
        try:
            # Получаем текущую конфигурацию из переменных Airflow
            url = f"{self.airflow_client.airflow_url}/api/v1/variables/{self.config_variable_name}"
            response = self.airflow_client.session.get(url)
            response.raise_for_status()
            variable_data = response.json()
            self.original_config = json.loads(variable_data.get('value', '{}'))
            logger.info("Config backup created from Airflow variable")
        except Exception as e:
            logger.error(f"Failed to backup config from Airflow variable: {str(e)}")
            raise
            
    def restore_config(self):
        """Восстановление исходной конфигурации в переменных Airflow"""
        if self.original_config is not None:
            try:
                # Обновляем переменную в Airflow с исходной конфигурацией
                url = f"{self.airflow_client.airflow_url}/api/v1/variables/{self.config_variable_name}"
                payload = {
                    "key": self.config_variable_name,
                    "value": json.dumps(self.original_config, ensure_ascii=False)
                }
                response = self.airflow_client.session.patch(url, json=payload)
                response.raise_for_status()
                logger.info("Config restored in Airflow variable")
            except Exception as e:
                logger.error(f"Failed to restore config in Airflow variable: {str(e)}")
                raise
        else:
            logger.warning("No original config to restore")
                
    def update_config(self, start_date: str, end_date: str, tables: List[str]):
        """Обновление конфигурации в переменных Airflow"""
        # Создаем резервную копию перед изменением
        self.backup_config()
        
        try:
            # Получаем текущую конфигурацию
            url = f"{self.airflow_client.airflow_url}/api/v1/variables/{self.config_variable_name}"
            response = self.airflow_client.session.get(url)
            response.raise_for_status()
            variable_data = response.json()
            config = json.loads(variable_data.get('value', '{}'))
            
            # Обновляем параметры
            config['load_start_date'] = start_date
            config['load_end_date'] = end_date
            config['only_load_tables'] = tables
            
            # Сохраняем обновленную конфигурацию в переменной Airflow
            payload = {
                "key": self.config_variable_name,
                "value": json.dumps(config, ensure_ascii=False)
            }
            response = self.airflow_client.session.patch(url, json=payload)
            response.raise_for_status()
            logger.info(f"Config updated in Airflow variable with start_date={start_date}, end_date={end_date}, tables={tables}")
        except Exception as e:
            logger.error(f"Failed to update config in Airflow variable: {str(e)}")
            raise