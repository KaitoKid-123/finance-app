"""Currency normalization transforms."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Exchange rates tĩnh từ USD (cập nhật daily qua config)
EXCHANGE_RATES_DEFAULT = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.79,
    "VND": 24800.0,
    "JPY": 149.5,
    "SGD": 1.34,
}


def normalize_to_usd(
    df: DataFrame,
    amount_col: str,
    currency_col: str,
    output_col: str = "amount_usd",
    exchange_rates: dict = None,
) -> DataFrame:
    """
    Chuyển đổi các giá trị tiền tệ về USD.

    Args:
        df: Input DataFrame
        amount_col: Tên cột chứa số tiền
        currency_col: Tên cột chứa mã tiền tệ (USD, EUR, ...)
        output_col: Tên cột output
        exchange_rates: Dict exchange rates, mặc định dùng EXCHANGE_RATES_DEFAULT

    Returns:
        DataFrame với cột output_col mới
    """
    rates = exchange_rates or EXCHANGE_RATES_DEFAULT

    # Build CASE WHEN expression
    expr = F.when(F.col(currency_col) == "USD", F.col(amount_col).cast(DoubleType()))
    for currency, rate in rates.items():
        if currency != "USD":
            expr = expr.when(
                F.col(currency_col) == currency,
                (F.col(amount_col).cast(DoubleType()) / F.lit(rate))
            )
    expr = expr.otherwise(None)  # NULL nếu currency không hợp lệ

    return df.withColumn(output_col, expr)


def flag_currency_issues(df: DataFrame, currency_col: str) -> DataFrame:
    """Flag các row có currency code không hợp lệ."""
    valid_currencies = list(EXCHANGE_RATES_DEFAULT.keys())
    return df.withColumn(
        "has_currency_issue",
        ~F.col(currency_col).isin(valid_currencies)
    )