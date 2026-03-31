"""
Finance Daily Revenue ETL Job.

Flow:
  1. Extract raw transactions từ Ceph S3 (bronze layer)
  2. Transform: deduplicate, normalize currency, calculate metrics
  3. Quality checks: null rate, row count, value range
  4. Load vào Iceberg silver layer
  5. Compact Iceberg files

Usage:
    spark-submit --master k8s://... finance_daily_revenue_etl.py \
        --date 2024-01-15 \
        --source-bucket team-finance \
        --source-prefix raw/transactions
"""
import argparse
import sys
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, BooleanType
)
import structlog

from src.utils.spark_session import create_spark_session
from src.utils.iceberg_helper import ensure_namespace, upsert_iceberg, compact_table
from src.transforms.currency import normalize_to_usd, flag_currency_issues
from src.transforms.deduplication import dedup_by_latest

logger = structlog.get_logger()

RAW_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True),
    StructField("metadata", StringType(), nullable=True),
])


def extract(spark, source_path: str):
    """Extract raw Parquet files từ S3 bronze layer."""
    logger.info("extracting", source=source_path)
    df = spark.read \
        .schema(RAW_SCHEMA) \
        .option("mergeSchema", "true") \
        .parquet(source_path)
    record_count = df.count()
    logger.info("extracted", records=record_count)
    return df


def transform(df):
    """Transform raw data: clean, normalize, enrich."""
    logger.info("transforming")

    # 1. Dedup by transaction_id, giữ record mới nhất
    df = dedup_by_latest(df, id_cols=["transaction_id"], timestamp_col="updated_at")

    # 2. Filter chỉ giữ COMPLETED transactions
    df = df.filter(F.col("status") == "COMPLETED")

    # 3. Normalize currency về USD
    df = normalize_to_usd(df, "amount", "currency", "amount_usd")

    # 4. Flag các currency issue
    df = flag_currency_issues(df, "currency")

    # 5. Thêm partition columns
    df = df \
        .withColumn("transaction_date", F.to_date(F.col("created_at"))) \
        .withColumn("transaction_year", F.year(F.col("created_at"))) \
        .withColumn("transaction_month", F.month(F.col("created_at"))) \
        .withColumn("_etl_loaded_at", F.current_timestamp())

    # 6. Drop rows thiếu thông tin bắt buộc
    df = df.dropna(subset=["transaction_id", "account_id", "created_at"])

    logger.info("transform_complete", output_records=df.count())
    return df


def quality_check(df) -> bool:
    """
    Kiểm tra data quality trước khi load.
    Return True nếu pass, raise Exception nếu fail.
    """
    logger.info("running_quality_checks")

    total = df.count()
    if total == 0:
        raise ValueError("Quality check FAILED: 0 records after transform")

    # Check 1: Null rate cho các cột quan trọng
    null_checks = [
        ("amount_usd", 0.05),   # Cho phép tối đa 5% null
        ("account_id", 0.00),   # Không chấp nhận null
        ("transaction_type", 0.01),
    ]
    for col_name, max_null_rate in null_checks:
        if col_name not in df.columns:
            continue
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_rate = null_count / total
        if null_rate > max_null_rate:
            raise ValueError(
                f"Quality check FAILED: {col_name} null rate = "
                f"{null_rate:.2%} > threshold {max_null_rate:.2%}"
            )
        logger.info("quality_check_passed",
                    column=col_name, null_rate=f"{null_rate:.2%}")

    # Check 2: amount_usd không âm
    negative_amounts = df.filter(
        F.col("amount_usd").isNotNull() & (F.col("amount_usd") < 0)
    ).count()
    if negative_amounts > 0:
        logger.warning("negative_amounts_found", count=negative_amounts)
        # Warning only, không fail (có thể là refund)

    # Check 3: Currency issues không quá 2%
    if "has_currency_issue" in df.columns:
        currency_issues = df.filter(F.col("has_currency_issue")).count()
        issue_rate = currency_issues / total
        if issue_rate > 0.02:
            raise ValueError(
                f"Quality check FAILED: currency issues = {issue_rate:.2%} > 2%"
            )

    logger.info("all_quality_checks_passed", total_records=total)
    return True


def load(spark, df, target_date: str) -> dict:
    """Load dữ liệu vào Iceberg silver table."""
    logger.info("loading", target_date=target_date)

    ensure_namespace(spark, "finance")

    result = upsert_iceberg(
        spark=spark,
        source_df=df,
        target_table="finance.transactions_silver",
        merge_keys=["transaction_id"],
        partition_cols=["transaction_date"],
    )

    logger.info("load_complete", result=result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Finance Daily Revenue ETL")
    parser.add_argument("--date", required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--source-bucket", default="team-finance")
    parser.add_argument("--source-prefix", default="raw/transactions")
    parser.add_argument("--compact", action="store_true",
                        help="Run Iceberg compaction after load")
    args = parser.parse_args()

    processing_date = args.date
    source_path = (
        f"s3a://{args.source_bucket}/{args.source_prefix}/"
        f"date={processing_date}/"
    )

    spark = create_spark_session(
        app_name=f"finance-daily-revenue-etl-{processing_date}"
    )

    try:
        # ETL pipeline
        raw_df = extract(spark, source_path)
        transformed_df = transform(raw_df)
        quality_check(transformed_df)
        result = load(spark, transformed_df, processing_date)

        if args.compact:
            compact_table(spark, "finance.transactions_silver")

        logger.info("etl_success",
                    date=processing_date,
                    rows_processed=result)
        sys.exit(0)

    except Exception as e:
        logger.error("etl_failed", date=processing_date, error=str(e))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
