import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.transforms.currency import normalize_to_usd, flag_currency_issues


@pytest.fixture(scope="session")
def spark():
    """Spark session chạy local cho unit tests."""
    return SparkSession.builder \
        .appName("test-currency") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("tx_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
    ])
    data = [
        ("tx1", 100.0, "USD"),
        ("tx2", 92.0, "EUR"),    # 92 EUR = ~100 USD
        ("tx3", 200.0, "INVALID"),
        ("tx4", None, "USD"),
    ]
    return spark.createDataFrame(data, schema)


def test_normalize_usd_passthrough(spark, sample_df):
    """USD amount giữ nguyên."""
    result = normalize_to_usd(sample_df, "amount", "currency")
    usd_row = result.filter("tx_id = 'tx1'").first()
    assert usd_row["amount_usd"] == pytest.approx(100.0)


def test_normalize_eur_conversion(spark, sample_df):
    """EUR amount được convert đúng."""
    result = normalize_to_usd(sample_df, "amount", "currency")
    eur_row = result.filter("tx_id = 'tx2'").first()
    # 92 EUR / 0.92 rate = 100 USD
    assert eur_row["amount_usd"] == pytest.approx(100.0, rel=0.01)


def test_normalize_invalid_currency_null(spark, sample_df):
    """Currency không hợp lệ trả về NULL."""
    result = normalize_to_usd(sample_df, "amount", "currency")
    invalid_row = result.filter("tx_id = 'tx3'").first()
    assert invalid_row["amount_usd"] is None


def test_normalize_null_amount(spark, sample_df):
    """NULL amount giữ NULL."""
    result = normalize_to_usd(sample_df, "amount", "currency")
    null_row = result.filter("tx_id = 'tx4'").first()
    assert null_row["amount_usd"] is None


def test_flag_currency_issues(spark, sample_df):
    """Chỉ flag currency không hợp lệ."""
    result = flag_currency_issues(sample_df, "currency")
    issues = result.filter("has_currency_issue = true")
    assert issues.count() == 1
    assert issues.first()["tx_id"] == "tx3"