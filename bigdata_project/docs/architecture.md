# Архитектура решения

## 1. Источники
- CoinGecko Market Chart API (исторические цены BTC, шаг до часа).
- Airflow-задача `extract` сохраняет JSON в `data/raw` и таблицу `raw_bitcoin_prices`.

## 2. Оркестрация (Airflow)
- Развёрнут в Docker с LocalExecutor.
- DAG `bitcoin_etl` (расписание каждые 6 часов):
  1. `extract_bitcoin_prices` – выгрузка API и запись RAW.
  2. `transform_with_spark` – запуск PySpark job, генерация Parquet.
  3. `load_to_postgres` – апсерты дневной витрины `bitcoin_daily_metrics`.
- `max_active_runs=1`, ретраи = 1, таймаут = 30 мин.

## 3. PySpark
- Файл `spark_jobs/bitcoin_transform.py`.
- Создаёт `SparkSession`, читает JSON, разворачивает массив `prices`.
- Оконные функции рассчитывают open/close, агрегаты AVG/MIN/MAX и количество точек.
- Результат сохраняется в Parquet (`data/processed/bitcoin_<ds>`).

## 4. Хранилище
- Контейнер `postgres-dwh` (Postgres 15).
- Таблицы:
  - `raw_bitcoin_prices` – аудит JSON payload.
  - `bitcoin_daily_metrics` – фактовая витрина (PK `event_date`).
- Airflow и Streamlit используют одни и те же креды.

## 5. Визуализация
- `streamlit_app/app.py` подключается к DWH.
- Возможности: фильтр по датам, KPI, график Plotly, таблица и экспорт CSV.
- Кеширование (`@st.cache_data`) снижает нагрузку на БД.

## 6. Контейнеризация
- `docker-compose.yml` поднимает: `airflow-db`, `airflow-init`, `airflow-scheduler`, `airflow-webserver`, `postgres-dwh`, `streamlit`.
- Общие volume’ы (`./data`, `./spark_jobs`) монтируются в Airflow и Spark-job.
- Порты: Airflow `8080`, Streamlit `8501`, DWH `5433`.

## 7. Конфигурация и безопасность
- `airflow.env` хранит UID/GID и Fernet key.
- Переменные API/БД передаются через compose, при необходимости редактируются.
- Логи доступны на хосте (`airflow/logs`), что упрощает отладку.

## 8. Дальнейшее развитие
- Вынести сырые данные в объектное хранилище (MinIO/S3).
- Подключить Kafka как источник и Spark Structured Streaming.
- Настроить CI/CD: тесты DAG, линтеры, авторазвёртывание.
- Добавить мониторинг (Prometheus + Grafana) и алерты.


