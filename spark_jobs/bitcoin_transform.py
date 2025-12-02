import logging
from pathlib import Path

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
    max,
    min,
    to_date,
    to_timestamp,
)


logger = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("BitcoinTransformJob")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_transform_job(raw_path: str, output_path: str) -> str:
    """
    Читает сырые данные CoinGecko, агрегирует дневные метрики и сохраняет Parquet.
    """
    raw_file = Path(raw_path)
    if not raw_file.exists():
        raise FileNotFoundError(f"Файл с данными не найден: {raw_file}")

    spark = _build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("multiline", "true").json(str(raw_file))
    if df.rdd.isEmpty():
        spark.stop()
        raise ValueError(f"Во входном файле {raw_file} отсутствуют данные")

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

    target_dir = Path(output_path)
    target_dir.mkdir(parents=True, exist_ok=True)
    daily_metrics.write.mode("overwrite").parquet(str(target_dir))
    logger.info("Parquet-датасет сохранён в %s", target_dir)

    spark.stop()
    return str(target_dir)


