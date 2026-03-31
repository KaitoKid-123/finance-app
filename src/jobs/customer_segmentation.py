"""
Customer Segmentation Job (RFM Analysis).

Tính RFM score (Recency, Frequency, Monetary) cho từng customer
và gán segment: Champions, Loyal, At Risk, Lost.

Usage:
    spark-submit customer_segmentation.py \
        --snapshot-date 2024-01-31 \
        --lookback-days 365
"""
import argparse
import sys
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import structlog

from src.utils.spark_session import create_spark_session
from src.utils.iceberg_helper import ensure_namespace, upsert_iceberg

logger = structlog.get_logger()


def compute_rfm(spark, snapshot_date: str, lookback_days: int):
    """
    Tính RFM metrics từ transactions_silver.

    Returns:
        DataFrame với các cột:
        account_id, recency_days, frequency, monetary_usd,
        r_score, f_score, m_score, rfm_score, segment
    """
    logger.info("computing_rfm",
                snapshot_date=snapshot_date,
                lookback_days=lookback_days)

    cutoff = F.date_sub(F.lit(snapshot_date).cast("date"), lookback_days)

    txn = spark.read.table("iceberg.finance.transactions_silver") \
        .filter(F.col("transaction_date") >= cutoff) \
        .filter(F.col("transaction_date") <= F.lit(snapshot_date).cast("date")) \
        .filter(F.col("amount_usd").isNotNull())

    rfm_base = txn.groupBy("account_id").agg(
        F.datediff(
            F.lit(snapshot_date).cast("date"),
            F.max("transaction_date")
        ).alias("recency_days"),
        F.countDistinct("transaction_id").alias("frequency"),
        F.sum("amount_usd").alias("monetary_usd"),
    )

    # Scoring: chia thành 5 quintiles, cao hơn = tốt hơn
    w = Window.orderBy
    rfm_scored = rfm_base \
        .withColumn(
            "r_score",
            F.ntile(5).over(Window.orderBy(F.col("recency_days").asc()))
            .cast(IntegerType())
        ) \
        .withColumn(
            "f_score",
            F.ntile(5).over(Window.orderBy(F.col("frequency").desc()))
            .cast(IntegerType())
        ) \
        .withColumn(
            "m_score",
            F.ntile(5).over(Window.orderBy(F.col("monetary_usd").desc()))
            .cast(IntegerType())
        ) \
        .withColumn(
            "rfm_score",
            F.col("r_score") + F.col("f_score") + F.col("m_score")
        )

    # Segment mapping dựa trên rfm_score
    segmented = rfm_scored.withColumn(
        "segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 10, "Loyal")
         .when(
             (F.col("rfm_score") >= 7) & (F.col("r_score") >= 3),
             "Potential"
         )
         .when(
             (F.col("rfm_score") >= 6) & (F.col("r_score") <= 2),
             "At Risk"
         )
         .when(F.col("rfm_score") <= 5, "Lost")
         .otherwise("Needs Attention")
    ) \
    .withColumn("snapshot_date", F.lit(snapshot_date).cast("date")) \
    .withColumn("_etl_loaded_at", F.current_timestamp())

    return segmented


def main():
    parser = argparse.ArgumentParser(
        description="Finance Customer Segmentation (RFM)"
    )
    parser.add_argument(
        "--snapshot-date",
        required=True,
        help="Analysis snapshot date YYYY-MM-DD"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Number of days to look back from snapshot date"
    )
    args = parser.parse_args()

    spark = create_spark_session(
        f"finance-customer-segmentation-{args.snapshot_date}"
    )

    try:
        rfm_df = compute_rfm(spark, args.snapshot_date, args.lookback_days)

        # Quality check
        total = rfm_df.count()
        if total == 0:
            raise ValueError(
                f"No customers found for snapshot {args.snapshot_date}"
            )

        segments = rfm_df.groupBy("segment").count().collect()
        logger.info("segments",
                    snapshot=args.snapshot_date,
                    total_customers=total,
                    segments={r["segment"]: r["count"] for r in segments})

        ensure_namespace(spark, "finance")
        upsert_iceberg(
            spark=spark,
            source_df=rfm_df,
            target_table="finance.customer_segments",
            merge_keys=["account_id", "snapshot_date"],
            partition_cols=["snapshot_date"],
        )

        logger.info("segmentation_complete",
                    date=args.snapshot_date,
                    customers=total)
        sys.exit(0)

    except Exception as e:
        logger.error("segmentation_failed",
                     date=args.snapshot_date,
                     error=str(e))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
