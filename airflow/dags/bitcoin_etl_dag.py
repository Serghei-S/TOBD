from __future__ import annotations

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json, execute_values

sys.path.append("/opt/airflow")

from spark_jobs.bitcoin_transform import run_transform_job

# Конфигурация криптовалют
SUPPORTED_COINS = {
    "bitcoin": {"id": "bitcoin", "symbol": "BTC", "name": "Bitcoin"},
    "ethereum": {"id": "ethereum", "symbol": "ETH", "name": "Ethereum"},
    "binancecoin": {"id": "binancecoin", "symbol": "BNB", "name": "BNB"},
    "solana": {"id": "solana", "symbol": "SOL", "name": "Solana"},
    "cardano": {"id": "cardano", "symbol": "ADA", "name": "Cardano"},
}

RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")
DEFAULT_API_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
FEAR_GREED_API = "https://api.alternative.me/fng/?limit=30"
DEFAULT_LOOKBACK_DAYS = "365"

logger = logging.getLogger(__name__)


def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("BITCOIN_DB_HOST", "postgres-dwh"),
        port=os.getenv("BITCOIN_DB_PORT", "5432"),
        dbname=os.getenv("BITCOIN_DB_NAME", "bitcoin"),
        user=os.getenv("BITCOIN_DB_USER", "analytics"),
        password=os.getenv("BITCOIN_DB_PASSWORD", "analytics"),
    )


def _ensure_tables(cursor) -> None:
    """Создание всех необходимых таблиц"""
    # Таблица сырых данных
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto_prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            logical_date DATE NOT NULL,
            payload JSONB NOT NULL,
            inserted_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_raw_crypto_coin_date 
        ON raw_crypto_prices(coin_id, logical_date);
    """)
    
    # Расширенная таблица метрик с объёмами и market cap
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_daily_metrics (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            coin_symbol VARCHAR(10) NOT NULL,
            coin_name VARCHAR(50) NOT NULL,
            event_date DATE NOT NULL,
            open_price_usd NUMERIC,
            close_price_usd NUMERIC,
            avg_price_usd NUMERIC,
            max_price_usd NUMERIC,
            min_price_usd NUMERIC,
            volume_usd NUMERIC,
            market_cap_usd NUMERIC,
            samples_per_day INTEGER,
            processed_at TIMESTAMPTZ,
            UNIQUE(coin_id, event_date)
        );
        CREATE INDEX IF NOT EXISTS idx_crypto_metrics_coin_date 
        ON crypto_daily_metrics(coin_id, event_date);
    """)
    
    # Таблица Fear & Greed Index
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fear_greed_index (
            id SERIAL PRIMARY KEY,
            event_date DATE UNIQUE NOT NULL,
            value INTEGER NOT NULL,
            value_classification VARCHAR(50),
            inserted_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


def _fetch_crypto_payload(coin_id: str) -> Dict[str, Any]:
    """Загрузка данных для конкретной криптовалюты"""
    days = os.getenv("BITCOIN_LOOKBACK_DAYS", DEFAULT_LOOKBACK_DAYS)
    url = DEFAULT_API_URL.format(coin_id=coin_id)
    params = {"vs_currency": "usd", "days": days}
    headers = {"accept": "application/json"}
    
    logger.info("Запрашиваю данные %s: %s %s", coin_id, url, params)
    response = requests.get(url, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def _fetch_fear_greed() -> List[Dict]:
    """Загрузка Fear & Greed Index"""
    try:
        response = requests.get(FEAR_GREED_API, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except Exception as e:
        logger.error("Ошибка загрузки Fear & Greed: %s", e)
        return []


def extract_crypto_prices(**context) -> str:
    """Извлечение данных для всех криптовалют + Fear & Greed"""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logical_date = context["ds"]
    results = {}
    
    # Загрузка данных криптовалют
    for coin_id, coin_info in SUPPORTED_COINS.items():
        try:
            payload = _fetch_crypto_payload(coin_id)
            raw_path = RAW_DIR / f"{coin_id}_{logical_date}.json"
            
            with raw_path.open("w", encoding="utf-8") as fp:
                json.dump(payload, fp)
            
            logger.info("Данные %s сохранены: %s", coin_id, raw_path)
            results[coin_id] = str(raw_path)
            
            # Сохранение в PostgreSQL
            with _pg_conn() as conn:
                with conn.cursor() as cur:
                    _ensure_tables(cur)
                    cur.execute(
                        """INSERT INTO raw_crypto_prices (coin_id, logical_date, payload)
                           VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;""",
                        (coin_id, logical_date, Json(payload)),
                    )
            
            import time
            time.sleep(2)  # Rate limiting
            
        except Exception as e:
            logger.error("Ошибка загрузки %s: %s", coin_id, e)
            results[coin_id] = None
    
    # Загрузка Fear & Greed Index
    try:
        fear_greed_data = _fetch_fear_greed()
        if fear_greed_data:
            with _pg_conn() as conn:
                with conn.cursor() as cur:
                    _ensure_tables(cur)
                    for item in fear_greed_data:
                        from datetime import datetime
                        ts = int(item.get("timestamp", 0))
                        event_date = datetime.fromtimestamp(ts).date()
                        cur.execute(
                            """INSERT INTO fear_greed_index (event_date, value, value_classification)
                               VALUES (%s, %s, %s) ON CONFLICT (event_date) DO UPDATE 
                               SET value = EXCLUDED.value, value_classification = EXCLUDED.value_classification;""",
                            (event_date, int(item.get("value", 0)), item.get("value_classification", ""))
                        )
            logger.info("Fear & Greed Index загружен: %d записей", len(fear_greed_data))
    except Exception as e:
        logger.error("Ошибка Fear & Greed: %s", e)
    
    return json.dumps(results)


def transform_with_spark(**context) -> str:
    """Трансформация данных для всех криптовалют"""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ti = context["ti"]
    raw_paths_json = ti.xcom_pull(task_ids="extract_crypto_prices")
    
    if not raw_paths_json:
        raise RuntimeError("Не удалось получить пути из XCom")
    
    raw_paths = json.loads(raw_paths_json)
    logical_date = context["ds"]
    results = {}
    
    for coin_id, raw_path in raw_paths.items():
        if raw_path is None:
            continue
            
        try:
            parquet_dir = PROCESSED_DIR / f"{coin_id}_{logical_date}"
            result_path = run_transform_job(
                raw_path=raw_path, 
                output_path=str(parquet_dir),
                coin_id=coin_id
            )
            logger.info("Spark %s завершено: %s", coin_id, result_path)
            results[coin_id] = result_path
        except Exception as e:
            logger.error("Ошибка трансформации %s: %s", coin_id, e)
            results[coin_id] = None
    
    return json.dumps(results)


def _prepare_rows(df: pd.DataFrame, coin_info: dict) -> Iterable[Tuple]:
    for row in df.itertuples(index=False):
        event_date = row.event_date
        if hasattr(event_date, 'date'):
            event_date = event_date.date()
        
        # Получаем volume и market_cap если есть
        volume = getattr(row, 'volume_usd', None) or 0
        market_cap = getattr(row, 'market_cap_usd', None) or 0
        
        yield (
            coin_info["id"],
            coin_info["symbol"],
            coin_info["name"],
            event_date,
            float(row.open_price_usd),
            float(row.close_price_usd),
            float(row.avg_price_usd),
            float(row.max_price_usd),
            float(row.min_price_usd),
            float(volume),
            float(market_cap),
            int(row.samples_per_day),
            row.processed_at.to_pydatetime() if pd.notna(row.processed_at) else None,
        )


def load_to_postgres(**context) -> str:
    """Загрузка данных в PostgreSQL"""
    ti = context["ti"]
    processed_paths_json = ti.xcom_pull(task_ids="transform_with_spark")
    
    if not processed_paths_json:
        raise RuntimeError("Не удалось получить пути из XCom")
    
    processed_paths = json.loads(processed_paths_json)
    total_loaded = 0
    
    for coin_id, processed_path in processed_paths.items():
        if processed_path is None:
            continue
            
        try:
            df = pd.read_parquet(processed_path)
            if df.empty:
                continue
            
            coin_info = SUPPORTED_COINS[coin_id]
            rows = list(_prepare_rows(df, coin_info))
            
            with _pg_conn() as conn:
                with conn.cursor() as cur:
                    _ensure_tables(cur)
                    insert_sql = """
                        INSERT INTO crypto_daily_metrics (
                            coin_id, coin_symbol, coin_name, event_date,
                            open_price_usd, close_price_usd, avg_price_usd,
                            max_price_usd, min_price_usd, volume_usd, market_cap_usd,
                            samples_per_day, processed_at
                        )
                        VALUES %s
                        ON CONFLICT (coin_id, event_date) DO UPDATE SET
                            open_price_usd = EXCLUDED.open_price_usd,
                            close_price_usd = EXCLUDED.close_price_usd,
                            avg_price_usd = EXCLUDED.avg_price_usd,
                            max_price_usd = EXCLUDED.max_price_usd,
                            min_price_usd = EXCLUDED.min_price_usd,
                            volume_usd = EXCLUDED.volume_usd,
                            market_cap_usd = EXCLUDED.market_cap_usd,
                            samples_per_day = EXCLUDED.samples_per_day,
                            processed_at = EXCLUDED.processed_at;
                    """
                    execute_values(cur, insert_sql, rows)
            
            total_loaded += len(rows)
            logger.info("Загружено %s строк для %s", len(rows), coin_id)
            
        except Exception as e:
            logger.error("Ошибка загрузки %s: %s", coin_id, e)
    
    return f"loaded-{total_loaded}"


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_etl",
    default_args=default_args,
    schedule=timedelta(hours=6),
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["crypto", "bitcoin", "ethereum", "spark", "etl", "ml"],
) as dag:
    extract = PythonOperator(
        task_id="extract_crypto_prices",
        python_callable=extract_crypto_prices,
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
