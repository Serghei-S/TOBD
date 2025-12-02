import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    explode,
    first,
    from_unixtime,
    last,
    lit,
    max,
    min,
    sum as spark_sum,
    to_date,
    to_timestamp,
)


logger = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CryptoTransformJob")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def run_transform_job(raw_path: str, output_path: str, coin_id: Optional[str] = None) -> str:
    """
    Читает сырые данные CoinGecko, агрегирует дневные метрики и сохраняет Parquet.
    Включает: цены, объёмы торгов, market cap
    """
    raw_file = Path(raw_path)
    if not raw_file.exists():
        raise FileNotFoundError(f"Файл с данными не найден: {raw_file}")

    spark = _build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("multiline", "true").json(str(raw_file))
        if df.rdd.isEmpty():
            raise ValueError(f"Во входном файле {raw_file} отсутствуют данные")

        # Обработка цен
        price_points = (
            df.select(explode("prices").alias("price"))
            .select(
                (col("price")[0] / 1000).alias("event_ts"),
                col("price")[1].alias("price_usd"),
            )
            .withColumn("event_time", to_timestamp(from_unixtime(col("event_ts"))))
            .drop("event_ts")
            .filter(col("price_usd").isNotNull())
            .orderBy("event_time")
            .withColumn("event_date", to_date(col("event_time")))
        )

        # Обработка объёмов торгов
        try:
            volume_points = (
                df.select(explode("total_volumes").alias("vol"))
                .select(
                    (col("vol")[0] / 1000).alias("event_ts"),
                    col("vol")[1].alias("volume"),
                )
                .withColumn("event_date", to_date(to_timestamp(from_unixtime(col("event_ts")))))
                .groupBy("event_date")
                .agg(avg("volume").alias("volume_usd"))
            )
        except Exception:
            volume_points = None

        # Обработка market cap
        try:
            mcap_points = (
                df.select(explode("market_caps").alias("mcap"))
                .select(
                    (col("mcap")[0] / 1000).alias("event_ts"),
                    col("mcap")[1].alias("market_cap"),
                )
                .withColumn("event_date", to_date(to_timestamp(from_unixtime(col("event_ts")))))
                .groupBy("event_date")
                .agg(avg("market_cap").alias("market_cap_usd"))
            )
        except Exception:
            mcap_points = None

        window_spec = (
            Window.partitionBy("event_date")
            .orderBy("event_time")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        enriched = (
            price_points.withColumn("open_price_usd", first("price_usd").over(window_spec))
            .withColumn("close_price_usd", last("price_usd").over(window_spec))
            .cache()
        )

        daily_metrics = (
            enriched.groupBy("event_date", "open_price_usd", "close_price_usd")
            .agg(
                avg("price_usd").alias("avg_price_usd"),
                max("price_usd").alias("max_price_usd"),
                min("price_usd").alias("min_price_usd"),
                count("*").alias("samples_per_day"),
            )
            .withColumn("processed_at", current_timestamp())
            .orderBy("event_date")
        )

        # Добавляем volume
        if volume_points is not None:
            daily_metrics = daily_metrics.join(volume_points, "event_date", "left")
        else:
            daily_metrics = daily_metrics.withColumn("volume_usd", lit(0.0))

        # Добавляем market cap
        if mcap_points is not None:
            daily_metrics = daily_metrics.join(mcap_points, "event_date", "left")
        else:
            daily_metrics = daily_metrics.withColumn("market_cap_usd", lit(0.0))

        # Заполняем NULL значения
        daily_metrics = daily_metrics.fillna(0, subset=["volume_usd", "market_cap_usd"])
        
        # Добавляем coin_id
        if coin_id:
            daily_metrics = daily_metrics.withColumn("coin_id", lit(coin_id))

        target_dir = Path(output_path)
        target_dir.mkdir(parents=True, exist_ok=True)
        daily_metrics.write.mode("overwrite").parquet(str(target_dir))
        logger.info("Parquet сохранён: %s", target_dir)

        return str(target_dir)
    finally:
        spark.stop()
