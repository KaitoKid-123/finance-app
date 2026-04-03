from typing import List
"""Deduplication strategies cho data pipeline."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedup_by_latest(
    df: DataFrame,
    id_cols: List[str],
    timestamp_col: str,
) -> DataFrame:
    """
    Giữ lại record mới nhất cho mỗi ID.
    Dung cho CDC (Change Data Capture) data.

    Args:
        df: Input DataFrame
        id_cols: List các cột tạo nên unique ID
        timestamp_col: Cột timestamp để xác định record mới nhất

    Returns:
        DataFrame sau khi dedup
    """
    window = Window.partitionBy(id_cols).orderBy(F.col(timestamp_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def dedup_exact(
    df: DataFrame,
    subset_cols: List[str] = None,
) -> DataFrame:
    """
    Loại bỏ exact duplicate rows.

    Args:
        df: Input DataFrame
        subset_cols: Chỉ xét các cột này khi check duplicate.
                     Nếu None, xét tất cả cột.
    """
    if subset_cols:
        return df.dropDuplicates(subset_cols)
    return df.dropDuplicates()


def count_duplicates(df: DataFrame, id_cols: List[str]) -> int:
    """Trả về số lượng duplicate records."""
    total = df.count()
    unique = df.dropDuplicates(id_cols).count()
    return total - unique
