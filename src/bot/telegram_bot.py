import os
import logging
from datetime import datetime
from typing import List
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from src.airflow.airflow_client import AirflowClient
from src.bot.airflow_config_manager import AirflowConfigManager
from src.bot.llm_parser import LLMMessageParser

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramBot:
    """Класс для управления Telegram ботом"""

    def __init__(self, token: str, airflow_client: AirflowClient, allowed_user_ids: List[int], llm_parser: LLMMessageParser):
        self.token = token
        self.airflow_client = airflow_client
        self.allowed_user_ids = allowed_user_ids
        self.llm_parser = llm_parser
        self.application = Application.builder().token(token).build()
        self.setup_handlers()
        
    def setup_handlers(self):
        """Настройка обработчиков сообщений"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
    def is_user_allowed(self, user_id: int) -> bool:
        """Проверка, разрешено ли пользователю использовать бота"""
        return user_id in self.allowed_user_ids
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /start"""
        user_id = update.effective_user.id

        # Проверяем, разрешено ли пользователю использовать бота
        if not self.is_user_allowed(user_id):
            await update.message.reply_text("Извините, но вам не разрешено использовать этого бота.")
            return

        welcome_message = (
            "Привет! Я бот для запуска DAG в Airflow с использованием искусственного интеллекта.\n\n"
            "Просто отправь мне сообщение естественным языком, например:\n"
            "обнови таблицу hrgate_units_enr за февраль 2024-го"
        )
        await update.message.reply_text(welcome_message)
        
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /help"""
        user_id = update.effective_user.id

        # Проверяем, разрешено ли пользователю использовать бота
        if not self.is_user_allowed(user_id):
            await update.message.reply_text("Извините, но вам не разрешено использовать этого бота.")
            return

        help_message = (
            "Я использую искусственный интеллект для понимания ваших запросов.\n\n"
            "Просто напишите мне, что нужно обновить, и я сам извлеку все необходимые параметры.\n\n"
            "Примеры запросов:\n"
            "- обнови таблицу hrgate_units_enr за февраль 2024-го\n"
            "- запусти DAG для hrgate_employees_enr за март 2023\n"
            "- обнови hrgate_units_enr и hrgate_employees_enr за январь 2024"
        )
        await update.message.reply_text(help_message)
        
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений"""
        user_id = update.effective_user.id

        # Проверяем, разрешено ли пользователю использовать бота
        if not self.is_user_allowed(user_id):
            await update.message.reply_text("Извините, но вам не разрешено использовать этого бота.")
            return

        # Проверяем, что LLM-парсер доступен
        if self.llm_parser is None:
            await update.message.reply_text(
                "Извините, парсер LLM не настроен. Обратитесь к администратору."
            )
            return

        try:
            message_text = update.message.text.strip()

            # Используем LLM для парсинга сообщения
            params = self.llm_parser.parse_message(message_text)

            if params is None:
                await update.message.reply_text(
                    "Не удалось разобрать ваш запрос. Пожалуйста, уточните его."
                )
                return

            # Получаем извлеченные параметры
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            tables = params.get('tables')
            dag_name = params.get('dag_name')

            # Проверяем, что dag_name извлечен
            if not dag_name:
                await update.message.reply_text(
                    "Не удалось определить DAG для запуска. Убедитесь, что таблица, которую вы хотите обновить, соответствует одному из доступных DAG."
                )
                return

            # Проверяем, что все параметры извлечены
            if not all([start_date, end_date, tables]):
                await update.message.reply_text(
                    "Не удалось извлечь все необходимые параметры из запроса. Пожалуйста, уточните его."
                )
                return

            # Проверяем формат дат
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                await update.message.reply_text("Неверный формат дат. Пожалуйста, уточните даты в запросе.")
                return

            # Проверяем, что таблицы - это список
            if isinstance(tables, str):
                tables = [tables]

            # Создаем config manager для конкретного DAG
            dag_info = None
            for dag in self.llm_parser.dag_mapping.get('dags', []):
                if dag['dag_name'] == dag_name:
                    dag_info = dag
                    break

            if not dag_info:
                await update.message.reply_text(f"Не найдена конфигурация для DAG '{dag_name}' в файле сопоставления.")
                return

            config_variable_name = dag_info.get('config_variable')
            if not config_variable_name:
                await update.message.reply_text(f"Не найдена переменная конфигурации для DAG '{dag_name}'.")
                return

            # Создаем временный config manager для нужной переменной конфигурации
            from src.bot.airflow_config_manager import AirflowConfigManager
            config_manager = AirflowConfigManager(
                airflow_client=self.airflow_client,
                config_variable_name=config_variable_name
            )

            # Обновляем конфиг в переменных Airflow
            await update.message.reply_text("Обновляю конфигурацию в Airflow...")
            config_manager.update_config(start_date, end_date, tables)

            # Добавляем паузу, чтобы дать шедулеру время подхватить измененный конфиг
            await update.message.reply_text("Жду, пока шедулер подхватит обновленный конфиг...")
            await asyncio.sleep(30)  # Пауза 5 секунд

            # Запускаем DAG
            await update.message.reply_text("Запускаю DAG...")
            dag_run_response = self.airflow_client.trigger_dag(
                dag_id=dag_name,  # Используем DAG, определенный LLM или переданный в конструкторе
                conf={
                    "start_date": start_date,
                    "end_date": end_date,
                    "tables": tables
                }
            )

            dag_run_id = dag_run_response.get('dag_run_id')
            await update.message.reply_text(f"DAG запущен с ID запуска: {dag_run_id}")

            # Отслеживаем статус выполнения
            await update.message.reply_text("Отслеживаю статус выполнения...")
            status = await self.monitor_dag_run(dag_name, dag_run_id)

            if status == 'success':
                await update.message.reply_text("DAG успешно выполнен!")
            elif status == 'failed':
                await update.message.reply_text("DAG завершился с ошибкой!")
            else:
                await update.message.reply_text(f"DAG завершился со статусом: {status}")

            # Восстанавливаем конфиг в переменных Airflow
            await update.message.reply_text("Восстанавливаю исходную конфигурацию...")
            config_manager.restore_config()
            await update.message.reply_text("Конфигурация восстановлена.")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await update.message.reply_text(f"Произошла ошибка: {str(e)}")

            # Восстанавливаем конфиг в случае ошибки
            try:
                # Создаем config manager для конкретного DAG
                dag_info = None
                for dag in self.llm_parser.dag_mapping.get('dags', []):
                    if dag['dag_name'] == dag_name:
                        dag_info = dag
                        break

                if dag_info:
                    config_variable_name = dag_info.get('config_variable')
                    if config_variable_name:
                        from src.bot.airflow_config_manager import AirflowConfigManager
                        config_manager = AirflowConfigManager(
                            airflow_client=self.airflow_client,
                            config_variable_name=config_variable_name
                        )
                        config_manager.restore_config()
                        await update.message.reply_text("Конфигурация восстановлена после ошибки.")
                    else:
                        await update.message.reply_text("Не удалось восстановить конфигурацию после ошибки - не найдена переменная конфигурации.")
                else:
                    await update.message.reply_text("Не удалось восстановить конфигурацию после ошибки - не найден DAG в сопоставлении.")
            except Exception as restore_error:
                logger.error(f"Error restoring config: {str(restore_error)}")
                await update.message.reply_text("Не удалось восстановить конфигурацию после ошибки.")
                
    async def monitor_dag_run(self, dag_name: str, dag_run_id: str, check_interval: int = 30) -> str:
        """Мониторинг статуса выполнения DAG"""
        while True:
            try:
                status_response = self.airflow_client.get_dag_run_status(dag_name, dag_run_id)
                state = status_response.get('state')

                if state in ['success', 'failed', 'skipped']:
                    return state

                # Ждем перед следующей проверке
                await asyncio.sleep(check_interval)
            except Exception as e:
                logger.error(f"Error monitoring DAG run: {str(e)}")
                return 'unknown'
                
    def run(self):
        """Запуск бота"""
        logger.info("Starting Telegram bot...")
        self.application.run_polling()

# Пример использования:
# if __name__ == "__main__":
#     # Инициализация компонентов
#     airflow_client = AirflowClient(
#         airflow_url="http://localhost:8080",
#         username="admin",
#         password="admin"
#     )
#
#     # Создание LLM-парсера (обязательно)
#     from src.bot.llm_parser import LLMMessageParser
#     dag_mapping_path = "src/bot/config/dag_mapping.json"
#     llm_parser = LLMMessageParser(dag_mapping_path)
#
#     # Создание и запуск бота
#     bot = TelegramBot(
#         token="YOUR_TELEGRAM_BOT_TOKEN",
#         airflow_client=airflow_client,
#         allowed_user_ids=[123456789, 987654321],
#         llm_parser=llm_parser  # Обязательно передаем LLM-парсер
#     )
#
#     bot.run()