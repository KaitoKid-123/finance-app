"""
Monthly Reconciliation Job.
So sánh transactions_silver với source_of_truth để phát hiện discrepancies.
"""
import argparse
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import structlog

from src.utils.spark_session import create_spark_session
from src.utils.iceberg_helper import ensure_namespace, upsert_iceberg

logger = structlog.get_logger()


def reconcile(spark, year: int, month: int) -> dict:
    """So sánh tổng tiền theo account giữa silver và source of truth."""

    # Read silver layer
    silver_df = spark.read.table("iceberg.finance.transactions_silver") \
        .filter(
            (F.year("transaction_date") == year) &
            (F.month("transaction_date") == month)
        )

    # Aggregate by account
    silver_agg = silver_df.groupBy("account_id").agg(
        F.count("transaction_id").alias("tx_count"),
        F.sum("amount_usd").alias("total_amount_usd"),
        F.countDistinct("transaction_id").alias("unique_tx_count"),
    )

    # Read source of truth (e.g., from external system)
    # Trong thực tế, thường đọc từ database trực tiếp hoặc API
    sot_path = f"s3a://team-finance/source-of-truth/year={year}/month={month:02d}/"
    try:
        sot_df = spark.read.parquet(sot_path).groupBy("account_id").agg(
            F.sum("amount_usd").alias("sot_total_usd"),
        )
    except Exception:
        logger.warning("sot_not_found", path=sot_path)
        return {"status": "skipped", "reason": "source_of_truth_not_found"}

    # Join và tìm discrepancies
    reconcile_df = silver_agg.join(sot_df, on="account_id", how="full") \
        .withColumn("diff_usd",
                    F.abs(F.col("total_amount_usd") - F.col("sot_total_usd"))) \
        .withColumn("has_discrepancy",
                    F.col("diff_usd") > 0.01)  # Tolerance 1 cent

    discrepancy_count = reconcile_df.filter("has_discrepancy").count()

    # Lưu kết quả reconcile vào Iceberg
    ensure_namespace(spark, "finance")
    upsert_iceberg(
        spark=spark,
        source_df=reconcile_df.withColumn(
            "reconcile_month",
            F.lit(f"{year}-{month:02d}")
        ),
        target_table="finance.monthly_reconcile",
        merge_keys=["account_id", "reconcile_month"],
    )

    return {
        "year": year,
        "month": month,
        "accounts_checked": reconcile_df.count(),
        "discrepancy_count": discrepancy_count,
        "status": "failed" if discrepancy_count > 0 else "passed"
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    spark = create_spark_session(
        f"finance-monthly-reconcile-{args.year}-{args.month:02d}"
    )

    try:
        result = reconcile(spark, args.year, args.month)
        logger.info("reconcile_complete", result=result)
        if result.get("status") == "failed":
            sys.exit(1)  # Airflow sẽ retry
        sys.exit(0)
    except Exception as e:
        logger.error("reconcile_error", error=str(e))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()