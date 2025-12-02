from __future__ import annotations

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json, execute_values

sys.path.append("/opt/airflow")

from spark_jobs.bitcoin_transform import run_transform_job


RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")
DEFAULT_API_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
DEFAULT_API_KEY = "CG-DemoAPIKey"

logger = logging.getLogger(__name__)


def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("BITCOIN_DB_HOST", "postgres-dwh"),
        port=os.getenv("BITCOIN_DB_PORT", "5432"),
        dbname=os.getenv("BITCOIN_DB_NAME", "bitcoin"),
        user=os.getenv("BITCOIN_DB_USER", "analytics"),
        password=os.getenv("BITCOIN_DB_PASSWORD", "analytics"),
    )


def _ensure_raw_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_bitcoin_prices (
            id SERIAL PRIMARY KEY,
            logical_date DATE NOT NULL,
            payload JSONB NOT NULL,
            inserted_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )


def _ensure_metrics_table(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bitcoin_daily_metrics (
            event_date DATE PRIMARY KEY,
            open_price_usd NUMERIC,
            close_price_usd NUMERIC,
            avg_price_usd NUMERIC,
            max_price_usd NUMERIC,
            min_price_usd NUMERIC,
            samples_per_day INTEGER,
            processed_at TIMESTAMPTZ
        );
        """
    )


def _fetch_bitcoin_payload() -> Dict[str, Any]:
    days = os.getenv("BITCOIN_LOOKBACK_DAYS", "30")
    url = os.getenv("BITCOIN_API_URL", DEFAULT_API_URL)
    # Убрали interval=hourly - требует платный API ключ CoinGecko
    params = {"vs_currency": "usd", "days": days}
    api_key = os.getenv("COINGECKO_API_KEY", DEFAULT_API_KEY)
    headers = {"accept": "application/json"}
    if api_key and api_key != "CG-DemoAPIKey":
        headers["x-cg-demo-api-key"] = api_key
    logger.info("Запрашиваю биткойн-данные: %s %s", url, params)
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_bitcoin_prices(**context) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload = _fetch_bitcoin_payload()
    logical_date = context["ds"]
    raw_path = RAW_DIR / f"bitcoin_{logical_date}.json"
    with raw_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp)
    logger.info("Сырые данные сохранены по пути %s", raw_path)

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            _ensure_raw_table(cur)
            cur.execute(
                """
                INSERT INTO raw_bitcoin_prices (logical_date, payload)
                VALUES (%s, %s);
                """,
                (logical_date, Json(payload)),
            )

    return str(raw_path)


def transform_with_spark(**context) -> str:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="extract_bitcoin_prices")
    if not raw_path:
        raise RuntimeError("Не удалось получить путь до сырых данных из XCom")

    logical_date = context["ds"]
    parquet_dir = PROCESSED_DIR / f"bitcoin_{logical_date}"
    result_path = run_transform_job(raw_path=raw_path, output_path=str(parquet_dir))
    logger.info("Spark-преобразование завершено: %s", result_path)
    return result_path


def _prepare_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    for row in df.itertuples(index=False):
        # event_date может быть datetime.date или pd.Timestamp
        event_date = row.event_date
        if hasattr(event_date, 'date'):
            event_date = event_date.date()
        yield (
            event_date,
            float(row.open_price_usd),
            float(row.close_price_usd),
            float(row.avg_price_usd),
            float(row.max_price_usd),
            float(row.min_price_usd),
            int(row.samples_per_day),
            row.processed_at.to_pydatetime() if pd.notna(row.processed_at) else None,
        )


def load_to_postgres(**context) -> str:
    ti = context["ti"]
    processed_path = ti.xcom_pull(task_ids="transform_with_spark")
    if not processed_path:
        raise RuntimeError("Не удалось получить путь к итоговому Parquet из XCom")

    df = pd.read_parquet(processed_path)
    if df.empty:
        logger.warning("Parquet %s пустой, загрузка пропущена", processed_path)
        return "no-data"

    rows = list(_prepare_rows(df))
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            _ensure_metrics_table(cur)
            insert_sql = """
                INSERT INTO bitcoin_daily_metrics (
                    event_date,
                    open_price_usd,
                    close_price_usd,
                    avg_price_usd,
                    max_price_usd,
                    min_price_usd,
                    samples_per_day,
                    processed_at
                )
                VALUES %s
                ON CONFLICT (event_date) DO UPDATE SET
                    open_price_usd = EXCLUDED.open_price_usd,
                    close_price_usd = EXCLUDED.close_price_usd,
                    avg_price_usd = EXCLUDED.avg_price_usd,
                    max_price_usd = EXCLUDED.max_price_usd,
                    min_price_usd = EXCLUDED.min_price_usd,
                    samples_per_day = EXCLUDED.samples_per_day,
                    processed_at = EXCLUDED.processed_at;
            """
            execute_values(cur, insert_sql, rows)
    logger.info("В Postgres загружено %s строк", len(rows))
    return f"loaded-{len(rows)}"


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bitcoin_etl",
    default_args=default_args,
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["bitcoin", "spark", "etl"],
) as dag:
    extract = PythonOperator(
        task_id="extract_bitcoin_prices",
        python_callable=extract_bitcoin_prices,
    )

    transform = PythonOperator(
        task_id="transform_with_spark",
        python_callable=transform_with_spark,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract >> transform >> load


