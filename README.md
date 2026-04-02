# Finance App

PySpark application xu ly du lieu tai chinh — gom cac ETL jobs, data transforms va utilities.

## Kien thuc nen biet

### PySpark tren Kubernetes
- Code nay **khong chay truc tiep** tren may local. No duoc dong goi thanh Docker image va chay nhu **SparkApplication** tren K8s.
- **Spark Operator** nhan SparkApplication manifest → tao driver pod → driver tao executor pods → chay xong tu xoa pods.
- Image base la `apache/spark:3.5.0-python3`, da co san Spark runtime.

### Iceberg Table Format
- Iceberg la **table format** cho data lake, ho tro ACID transactions, time travel, schema evolution.
- Data files luu tren S3 (MinIO), metadata luu tren PostgreSQL thong qua **Iceberg REST Catalog**.
- Spark doc/ghi Iceberg thong qua catalog plugin (`SparkCatalog` + REST).

### Medallion Architecture (Bronze → Silver → Gold)
- **Bronze**: Du lieu tho tu source (Parquet tren S3).
- **Silver**: Da lam sach, deduplicate, normalize currency → Iceberg tables.
- **Gold**: Aggregated cho report (chua implement).

## Cau truc thu muc

```
finance-app/
+-- src/
|   +-- jobs/                      # Cac ETL batch jobs
|   |   +-- daily_revenue_etl.py   #   ETL hang ngay: raw transactions → silver
|   |   +-- monthly_reconcile.py   #   Doi soat hang thang voi source of truth
|   |   +-- customer_segmentation.py # Phan khuc khach hang (WIP)
|   +-- transforms/                # Data transformation modules
|   |   +-- currency.py            #   Chuyen doi tien te ve USD
|   |   +-- deduplication.py       #   Loai bo ban ghi trung lap
|   +-- utils/                     # Shared utilities
|       +-- spark_session.py       #   Factory tao SparkSession voi Iceberg + S3
|       +-- iceberg_helper.py      #   CRUD operations cho Iceberg tables
|       +-- s3_helper.py           #   S3 file operations
+-- tests/
|   +-- unit/
|       +-- test_currency.py       # Unit test cho currency transform
|       +-- test_deduplication.py  # Unit test cho dedup
+-- .github/workflows/
|   +-- ci.yaml                    # CI: test → build → push to GHCR → update platform-dags
|   +-- pr-check.yaml              # PR check: lint + test
+-- Dockerfile                     # Image: Spark 3.5.0 + Python deps + Iceberg JARs
+-- requirements.txt               # Python dependencies
+-- pyproject.toml                 # Tool configs (mypy, flake8, pytest)
+-- CODEOWNERS                     # Code ownership rules
```

## ETL Jobs

### daily_revenue_etl.py
ETL hang ngay xu ly transaction data:

```
Extract (S3 Parquet) → Dedup → Filter COMPLETED → Normalize USD → Quality Check → Load Iceberg
```

- **Input**: `s3a://team-finance/raw/transactions/date=YYYY-MM-DD/`
- **Output**: Iceberg table `finance.transactions_silver`
- **Quality checks**: null rate, negative amounts, currency issues

### monthly_reconcile.py
Doi soat hang thang — so sanh `transactions_silver` voi source of truth:

- **Output**: Iceberg table `finance.monthly_reconcile`
- Tim discrepancies > $0.01 giua 2 nguon

## CI/CD Pipeline

```
Push to main → Lint (flake8) → Unit tests (70% coverage)
                                                    ↓
                                    Build Docker image → Push to GHCR
                                                              ↓
                                          Update image tag in platform-dags/spark-apps/
```

- **Registry**: `ghcr.io/KaitoKid-123/finance-app/etl:<git-sha>`
- **Trigger**: Push to `main` thay doi `src/`, `tests/`, `Dockerfile`, `requirements.txt`.
- **Coverage**: Toi thieu 70%.
- **Auto-update**: CI tu dong cap nhat image tag trong `platform-dags/dags/finance/spark-apps/*.yaml`.

## Dependencies chinh

| Package | Version | Vai tro |
|---------|---------|---------|
| pyspark | 3.5.0 | Spark SQL engine |
| pyiceberg | 0.6.0 | Iceberg Python client |
| boto3 | 1.34.0 | AWS S3 SDK |
| great-expectations | 0.18.8 | Data validation framework |
| pandas | 2.1.4 | Data manipulation |
| pyarrow | 14.0.2 | Columnar data format |
| structlog | 24.1.0 | Structured logging |

## Chay local (dev)

```bash
# Cai dependencies
pip install -r requirements.txt

# Chay unit tests
pytest tests/unit/ -v

# Lint
flake8 src/ tests/ --max-line-length=120

# Type check
mypy src/ --ignore-missing-imports
```

> **Luu y**: Cac job can Spark cluster va S3/Iceberg de chay that. Unit tests mock cac dependency nay.
