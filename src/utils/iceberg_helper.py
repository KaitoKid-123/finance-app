"""Helper functions cho Iceberg table operations."""
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import structlog

logger = structlog.get_logger()


def ensure_namespace(spark: SparkSession, namespace: str) -> None:
    """Tạo Iceberg namespace nếu chưa tồn tại."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
    logger.info("namespace_ensured", namespace=namespace)


def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    """Kiểm tra table có tồn tại trong Iceberg catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE iceberg.{full_table_name}")
        return True
    except Exception:
        return False


def upsert_iceberg(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    partition_cols: Optional[List[str]] = None,
) -> dict:
    """
    Upsert (MERGE INTO) vào Iceberg table.

    Args:
        source_df: DataFrame chứa dữ liệu mới
        target_table: Tên table dạng 'namespace.table_name'
        merge_keys: Các column dùng để join merge
        partition_cols: Các column partition (chỉ dùng khi tạo table mới)

    Returns:
        dict với rows_inserted, rows_updated
    """
    full_name = f"iceberg.{target_table}"

    # Tạo table nếu chưa tồn tại
    if not table_exists(spark, target_table):
        logger.info("creating_table", table=full_name)
        if partition_cols:
            # partitionedBy() requires column references, not SQL strings.
            # Iceberg uses day granularity for DATE columns automatically.
            partition_cols_ref = [F.col(c) for c in partition_cols]
            source_df.writeTo(full_name) \
                .partitionedBy(*partition_cols_ref) \
                .using("iceberg") \
                .createOrReplace()
        else:
            source_df.writeTo(full_name).using("iceberg").createOrReplace()
        row_count = source_df.count()
        return {"rows_inserted": row_count, "rows_updated": 0}

    # Build merge condition
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    # Get all columns for update
    all_cols = source_df.columns
    update_set = ", ".join(
        [f"target.{c} = source.{c}" for c in all_cols if c not in merge_keys]
    )
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"source.{c}" for c in all_cols])

    # Create temp view for merge source
    source_df.createOrReplaceTempView("merge_source")

    merge_sql = f"""
        MERGE INTO {full_name} AS target
        USING merge_source AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    logger.info("upsert_complete", table=full_name)
    return {"rows_inserted": -1, "rows_updated": -1}  # Spark MERGE doesn't return counts


def compact_table(spark: SparkSession, full_table_name: str) -> None:
    """
    Compact small files trong Iceberg table.
    Chạy sau ETL load để tối ưu read performance.
    """
    spark.sql(f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{full_table_name}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-input-files', '5'
            )
        )
    """)
    # Expire old snapshots (giữ lại 3 ngày) — compute timestamp in Python
    from datetime import datetime, timedelta
    expire_ts = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => '{full_table_name}',
            older_than => TIMESTAMP '{expire_ts}',
            retain_last => 5
        )
    """)
    logger.info("table_compacted", table=full_table_name)
