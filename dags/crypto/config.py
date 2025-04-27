from datetime import datetime

# Настройки S3 (MinIO)
S3_BUCKET = 'crypto-data'
S3_PREFIX = ''
S3_ENDPOINT = 'http://minio:9000'  
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'

# Настройки ClickHouse
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'secret'
CLICKHOUSE_DB = 'crypto'

# Настройки Airflow
AIRFLOW_DAG_ID = 'etl_minio_to_clickhouse'
AIRFLOW_START_DATE = datetime(2025, 4, 25)
AIRFLOW_SCHEDULE_INTERVAL = '@hourly'
AIRFLOW_CATCHUP = False