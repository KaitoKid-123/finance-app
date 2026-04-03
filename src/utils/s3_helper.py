from typing import List
"""Helper functions cho S3/Ceph operations."""
import os
import boto3
from botocore.config import Config
import structlog

logger = structlog.get_logger()


def get_s3_client():
    """Tạo boto3 S3 client trỏ đến Ceph RGW."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get(
            "S3_ENDPOINT",
            "http://rook-ceph-rgw-data-store-external.rook-ceph:80"
        ),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def file_exists(bucket: str, key: str) -> bool:
    """Kiểm tra file có tồn tại trong S3."""
    try:
        get_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def list_files(bucket: str, prefix: str) -> List[str]:
    """Liệt kê các file trong S3 bucket với prefix."""
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    return files
