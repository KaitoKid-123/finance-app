import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from src.transforms.deduplication import dedup_by_latest, count_duplicates


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test-dedup") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def test_dedup_by_latest_keeps_newest(spark):
    """Giữ record mới nhất cho mỗi ID."""
    data = [
        ("tx1", "old_value", datetime(2024, 1, 1, 10, 0)),
        ("tx1", "new_value", datetime(2024, 1, 1, 12, 0)),  # Newest
        ("tx2", "only_value", datetime(2024, 1, 1, 11, 0)),
    ]
    schema = StructType([
        StructField("tx_id", StringType()),
        StructField("value", StringType()),
        StructField("updated_at", TimestampType()),
    ])
    df = spark.createDataFrame(data, schema)
    result = dedup_by_latest(df, ["tx_id"], "updated_at")

    assert result.count() == 2
    tx1_row = result.filter("tx_id = 'tx1'").first()
    assert tx1_row["value"] == "new_value"


def test_count_duplicates(spark):
    """count_duplicates trả về đúng số."""
    data = [
        ("tx1",), ("tx1",), ("tx2",), ("tx3",), ("tx3",)
    ]
    schema = StructType([StructField("tx_id", StringType())])
    df = spark.createDataFrame(data, schema)
    assert count_duplicates(df, ["tx_id"]) == 2
