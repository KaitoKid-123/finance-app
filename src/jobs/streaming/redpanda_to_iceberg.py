"""Redpanda -> Spark Structured Streaming -> Iceberg.

This job is designed to be safe for the current cluster:
- isolated namespace
- one topic
- one Iceberg target table
- micro-batch checkpointing
- merge/upsert semantics to reduce duplicates on retries
"""
from __future__ import annotations

import os
from typing import Dict

import structlog
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.utils.iceberg_helper import ensure_namespace, upsert_iceberg
from src.utils.spark_session import create_spark_session

logger = structlog.get_logger()

TOPIC = os.environ.get("REDPANDA_TOPIC", "finance.transactions.v1")
TARGET_TABLE = os.environ.get("ICEBERG_TARGET_TABLE", "streaming.financial_transactions_events")
CHECKPOINT_LOCATION = os.environ.get(
    "CHECKPOINT_LOCATION",
    "s3a://team-finance/checkpoints/redpanda-to-iceberg/",
)
REDPANDA_BOOTSTRAP = os.environ.get(
    "REDPANDA_BOOTSTRAP_SERVERS",
    "redpanda.platform-streaming.svc.cluster.local:9092",
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
])


def read_stream(spark: SparkSession) -> DataFrame:
    logger.info("starting_stream_read", topic=TOPIC, bootstrap=REDPANDA_BOOTSTRAP)
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_batch(df: DataFrame) -> DataFrame:
    """Parse Kafka payload and normalize it before Iceberg write."""
    parsed = (
        df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json(F.col("json"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("transaction_date", F.to_date(F.col("event_time")))
        .withColumn("event_year", F.year(F.col("event_time")))
        .withColumn("event_month", F.month(F.col("event_time")))
        .dropna(subset=["event_id", "transaction_id", "account_id", "event_time"])
        .dropDuplicates(["event_id"])
    )

    parsed = parsed.withColumn(
        "currency",
        F.upper(F.coalesce(F.col("currency"), F.lit("USD"))),
    )
    return parsed


def ensure_target(spark: SparkSession) -> None:
    ensure_namespace(spark, "streaming")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS iceberg.{TARGET_TABLE} (
            event_id STRING,
            transaction_id STRING,
            account_id STRING,
            amount DOUBLE,
            currency STRING,
            event_type STRING,
            event_time TIMESTAMP,
            ingested_at TIMESTAMP,
            transaction_date DATE,
            event_year INT,
            event_month INT
        ) USING iceberg
        PARTITIONED BY (days(transaction_date))
        """
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    logger.info("processing_batch", batch_id=batch_id, rows=batch_df.count())
    if batch_df.rdd.isEmpty():
        logger.info("empty_batch", batch_id=batch_id)
        return

    spark = batch_df.sparkSession
    ensure_target(spark)

    normalized = transform_batch(batch_df)
    result: Dict[str, int] = upsert_iceberg(
        spark=spark,
        source_df=normalized,
        target_table=TARGET_TABLE,
        merge_keys=["event_id"],
        partition_cols=["transaction_date"],
    )
    logger.info("batch_written", batch_id=batch_id, result=result)


def main() -> None:
    spark = create_spark_session(app_name="redpanda-to-iceberg-streaming")
    try:
        ensure_target(spark)
        stream = read_stream(spark)
        query = (
            stream.writeStream.foreachBatch(write_batch)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime="30 seconds")
            .start()
        )
        query.awaitTermination()
    except Exception as exc:
        logger.error("stream_failed", error=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
