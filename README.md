# Finance App

PySpark application xử lý dữ liệu tài chính — gồm các ETL jobs, data transforms và utilities.

## Kiến thức nền tảng

### PySpark trên Kubernetes
- Code này **không chạy trực tiếp** trên máy local. Nó được đóng gói thành Docker image và chạy như **SparkApplication** trên K8s.
- **Spark Operator** nhận SparkApplication manifest → tạo driver pod → driver tạo executor pods → xong tự xóa.
- Image base là `apache/spark:3.5.0-python3`, đã có sẵn Spark runtime.

### Iceberg Table Format
- Iceberg là **table format** cho data lake, hỗ trợ ACID transactions, time travel, schema evolution.
- Data files lưu trên S3 (MinIO), metadata lưu trên PostgreSQL qua **Iceberg REST Catalog**.
- Spark đọc/ghi Iceberg qua catalog plugin (`SparkCatalog` + REST API).

### Medallion Architecture (Bronze → Silver → Gold)
- **Bronze**: Dữ liệu thô từ source (Parquet trên S3).
- **Silver**: Đã làm sạch, deduplicate, normalize currency → Iceberg tables.
- **Gold**: (planned) Aggregated views cho báo cáo.

## Cấu trúc thư mục

```
finance-app/
├── src/
│   ├── jobs/                          # Các ETL batch jobs
│   │   ├── daily_revenue_etl.py       #   ETL hàng ngày: raw → silver
│   │   ├── monthly_reconcile.py       #   Đối soát hàng tháng
│   │   └── customer_segmentation.py   #   Phân khúc khách hàng (WIP)
│   ├── transforms/                     # Data transformation modules
│   │   ├── currency.py                 #   Chuyển đổi tiền tệ về USD
│   │   └── deduplication.py            #   Loại bỏ bản ghi trùng lặp
│   └── utils/                          # Shared utilities
│       ├── spark_session.py            #   Factory tạo SparkSession + Iceberg + S3
│       ├── iceberg_helper.py           #   upsert_iceberg(), compact_table()
│       └── s3_helper.py                #   S3 file operations
├── tests/
│   └── unit/
│       ├── test_currency.py            #   Unit test cho currency transform
│       └── test_deduplication.py       #   Unit test cho dedup
├── .github/workflows/
│   ├── ci.yaml                        #   CI: test → build → push GHCR → update platform-dags
│   └── pr-check.yaml                   #   PR check: lint + test
├── Dockerfile                         #   Image: Spark 3.5.0 + Python deps + Iceberg JARs
├── requirements.txt                    #   Python dependencies
├── pyproject.toml                      #   Tool configs (mypy, flake8, pytest)
└── CODEOWNERS                          #   Code ownership rules
```

## ETL Jobs

### `daily_revenue_etl.py`
ETL hàng ngày xử lý transaction data:

```
Extract (S3 Parquet) → Dedup → Filter COMPLETED → Normalize USD → Quality Check → Load Iceberg
```

- **Input**: `s3a://team-finance/raw/transactions/date=YYYY-MM-DD/`
- **Output**: Iceberg table `finance.transactions_silver`
- **Quality checks**: null rate, negative amounts, currency issues

### `monthly_reconcile.py`
Đối soát hàng tháng — so sánh `transactions_silver` với source of truth:

- **Output**: Iceberg table `finance.monthly_reconcile`
- Tìm discrepancies > $0.01 giữa hai nguồn

### `customer_segmentation.py`
Phân khúc khách hàng theo hành vi giao dịch (WIP).

## CI/CD Pipeline

```
Push to main
  → GitHub Actions CI
      ├── Lint (flake8)
      └── Unit tests (pytest, coverage ≥ 70%)
                    ↓
         Build Docker image
                    ↓
         Push to GHCR (ghcr.io/KaitoKid-123/finance-app/etl:<git-sha>)
                    ↓
         Update image tag in platform-dags/dags/finance/spark-apps/*.yaml
                    ↓
         Push platform-dags (triggers ArgoCD sync của Airflow)
                    ↓
         Airflow git-sync picks up new YAML
                    ↓
         Next DAG run uses new image
```

- **Registry**: `ghcr.io/KaitoKid-123/finance-app/etl:<git-sha>`
- **Trigger**: Push to `main` thay đổi `src/`, `tests/`, `Dockerfile`, `requirements.txt`.
- **Coverage**: Tối thiểu 70%.
- **Auto-update**: CI tự động cập nhật image tag trong SparkApp YAMLs.

## Dependencies chính

| Package | Version | Vai trò |
|---------|---------|---------|
| pyspark | 3.5.0 | Spark SQL engine |
| pyiceberg | 0.6.0 | Iceberg Python client |
| boto3 | 1.34.0 | AWS S3 SDK |
| great-expectations | 0.18.8 | Data validation framework |
| pandas | 2.1.4 | Data manipulation |
| pyarrow | 14.0.2 | Columnar data format |
| structlog | 24.1.0 | Structured logging |

## Chạy local (dev)

```bash
# Cài dependencies
pip install -r requirements.txt

# Chạy unit tests
pytest tests/unit/ -v

# Lint
flake8 src/ tests/ --max-line-length=120

# Type check
mypy src/ --ignore-missing-imports
```

> **Lưu ý**: Các job cần Spark cluster và S3/Iceberg để chạy thật. Unit tests mock các dependency này.
