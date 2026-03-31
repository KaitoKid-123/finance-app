"""Spark session factory với Iceberg và S3 config chuẩn."""
import os
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    iceberg_catalog_uri: str = None,
    s3_endpoint: str = None,
) -> SparkSession:
    """
    Tạo SparkSession với Iceberg REST catalog và Ceph S3.

    Args:
        app_name: Tên application hiển thị trong Spark UI
        iceberg_catalog_uri: URI của Iceberg REST Catalog
        s3_endpoint: Endpoint của S3-compatible storage

    Returns:
        SparkSession configured for Iceberg + S3
    """
    iceberg_uri = iceberg_catalog_uri or os.environ.get(
        "ICEBERG_REST_URI",
        "http://iceberg-rest.platform-storage:8181"
    )
    s3_ep = s3_endpoint or os.environ.get(
        "S3_ENDPOINT",
        "http://minio.platform-storage:9000"
    )
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # Iceberg REST Catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", iceberg_uri)
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_ep)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        # S3A filesystem
        .config("spark.hadoop.fs.s3a.endpoint", s3_ep)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # Iceberg write config
        .config("spark.sql.catalog.iceberg.write.target-file-size-bytes",
                str(128 * 1024 * 1024))  # 128MB per file
    )
    return builder.getOrCreate()
