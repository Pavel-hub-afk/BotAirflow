# Тестовый Airflow для проверки Telegram бота

Этот проект содержит минимальную конфигурацию Airflow для тестирования работы Telegram бота.

## Структура проекта

```
test_airflow/
├── docker-compose.yml     # Конфигурация Docker Compose
├── dags/                  # Папка для DAG файлов
│   └── test_config_dag.py # Тестовый DAG для проверки конфигурации
├── logs/                  # Логи Airflow
└── config/                # Конфигурационные файлы
```

## Запуск Airflow

1. Перейдите в папку `test_airflow`:
   ```bash
   cd test_airflow
   ```

2. Запустите Airflow с помощью Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Дождитесь запуска всех сервисов (это может занять несколько минут):
   ```bash
   docker-compose ps
   ```

## Инициализация Airflow

1. Создайте учетную запись администратора:
   ```bash
   docker-compose run --rm airflow-webserver airflow users create \
       --username admin \
       --firstname admin \
       --lastname admin \
       --role Admin \
       --email admin@example.com
   ```

2. Войдите в веб-интерфейс Airflow по адресу: http://localhost:8080
   - Логин: admin
   - Пароль: admin (или другой, который вы установили при создании пользователя)

3. Создайте переменную конфигурации:
   - Перейдите в раздел Admin → Variables
   - Нажмите "Create"
   - Key: `pg_load_dm_enr_config`
   - Value: (вставьте JSON конфигурацию, которая была в файле)
   - Нажмите "Save"

## Тестирование Telegram бота

1. Убедитесь, что тестовый DAG `test_config_dag` появился в интерфейсе
2. Запустите Telegram бот с параметрами подключения к этому Airflow инстансу
3. Отправьте команду в Telegram бот для изменения конфигурации
4. Проверьте, что конфигурация в переменной Airflow изменилась
5. Запустите DAG `test_config_dag` вручную и проверьте логи на предмет изменений

## Остановка Airflow

Для остановки всех сервисов выполните:
```bash
docker-compose down
```

Для остановки с удалением данных (вolumes):
```bash
docker-compose down -v
```