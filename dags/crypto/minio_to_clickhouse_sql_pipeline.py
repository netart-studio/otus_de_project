from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client

# Настройки MinIO
MINIO_SERVER = 'http://minio:9000'
BUCKET_NAME = 'crypto-data'
FILE_PATH = 'parquet/*.parquet'
#FILE_PATH = 'parquet/2025-04-25-20-55-51.parquet'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'

# Настройки ClickHouse
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'secret'
CLICKHOUSE_DB = 'crypto'

# Создаем клиента для ClickHouse
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

def load_data_from_s3():
    """
    Загружает данные из MinIO (S3) в ClickHouse с помощью SQL-запроса.
    """
    s3_uri = f"{MINIO_SERVER}/{BUCKET_NAME}/{FILE_PATH}"
    print(f"Executing query with S3 URI: {s3_uri}")

    insert_query = f"""
    INSERT INTO crypto_prices
    SELECT *
    FROM s3(
        '{s3_uri}',
        '{MINIO_ACCESS_KEY}',
        '{MINIO_SECRET_KEY}',
        'Parquet'
    )
    WHERE timestamp >= now() - INTERVAL 24 HOUR;
    """
    try:
        clickhouse_client.execute(insert_query)
        print("Данные за последние 24 часа успешно загружены из MinIO в ClickHouse.")
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        raise

# Определение DAG
with DAG(
    'minio_to_clickhouse_sql_pipeline',
    default_args={'owner': 'airflow'},
    description='Pipeline to load Parquet data from MinIO to ClickHouse using SQL',
    schedule_interval='* * * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:

    # Задача: Загрузка данных из MinIO в ClickHouse
    load_data_task = PythonOperator(
        task_id='load_data_from_s3',
        python_callable=load_data_from_s3
    )

    # Порядок выполнения задач
    load_data_task