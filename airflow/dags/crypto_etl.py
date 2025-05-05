from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import os
import boto3
from botocore.client import Config
from io import BytesIO

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 9000,
    'user': 'default',
    'password': 'secret',
    'database': 'crypto'
}

MINIO_CONFIG = {
    'endpoint': 'http://minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket': 'crypto-data'
}

KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'crypto_consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'earliest'}
}

def create_clickhouse_client():
    """Создание клиента ClickHouse"""
    return Client(**CLICKHOUSE_CONFIG)

def create_minio_client():
    """Создание клиента MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_CONFIG['endpoint'],
        aws_access_key_id=MINIO_CONFIG['access_key'],
        aws_secret_access_key=MINIO_CONFIG['secret_key'],
        config=Config(signature_version='s3v4')
    )

def create_table(**kwargs):
    """Создание таблицы в ClickHouse если её нет"""
    clickhouse_client = create_clickhouse_client()
    
    create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_trades (
            symbol String,
            price Float64,
            quantity Float64,
            trade_time DateTime
        ) ENGINE = MergeTree()
        ORDER BY trade_time
    """
    
    try:
        clickhouse_client.execute(create_table_query)
        logger.info("Таблица crypto_trades создана успешно")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {str(e)}")
        raise

def save_to_s3(**kwargs):
    """Сохранение данных из Kafka в S3 в формате Parquet"""
    minio_client = create_minio_client()
    
    # Проверяем существование бакета
    try:
        minio_client.head_bucket(Bucket=MINIO_CONFIG['bucket'])
    except Exception as e:
        logger.error(f"Ошибка при проверке бакета: {str(e)}")
        try:
            minio_client.create_bucket(Bucket=MINIO_CONFIG['bucket'])
            logger.info(f"Бакет {MINIO_CONFIG['bucket']} создан успешно")
        except Exception as e:
            logger.error(f"Ошибка при создании бакета: {str(e)}")
            raise
    
    # Создание Kafka consumer
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['binance_trades'])
    
    # Сбор данных
    data = []
    try:
        # Читаем сообщения в течение 10 секунд или пока не получим 1000 сообщений
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < 10 and len(data) < 1000:
            msg = consumer.poll(1.0)
            if msg is None:
                logger.info("Нет новых сообщений")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Достигнут конец раздела")
                    continue
                else:
                    logger.error(f"Ошибка при чтении из Kafka: {msg.error()}")
                    break
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                # Преобразуем данные в нужный формат
                data.append({
                    'symbol': value['symbol'],
                    'price': float(value['price']),
                    'quantity': float(value['quantity']),
                    'trade_time': datetime.fromtimestamp(value['trade_time'] / 1000)
                })
                logger.info(f"Получено сообщение: {value}")
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {str(e)}")
                continue
    except Exception as e:
        logger.error(f"Ошибка при чтении из Kafka: {str(e)}")
        raise
    finally:
        consumer.close()
    
    if data:
        try:
            # Создание PyArrow таблицы
            schema = pa.schema([
                ('symbol', pa.string()),
                ('price', pa.float64()),
                ('quantity', pa.float64()),
                ('trade_time', pa.timestamp('us'))
            ])
            
            # Преобразование данных в формат PyArrow
            arrays = [
                pa.array([d['symbol'] for d in data]),
                pa.array([d['price'] for d in data]),
                pa.array([d['quantity'] for d in data]),
                pa.array([d['trade_time'] for d in data])
            ]
            
            table = pa.Table.from_arrays(arrays, schema=schema)
            
            # Сохранение в Parquet
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Загрузка в MinIO
            timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
            key = f'parquet/{timestamp}.parquet'
            
            minio_client.put_object(
                Bucket=MINIO_CONFIG['bucket'],
                Key=key,
                Body=buffer.getvalue()
            )
            logger.info(f"Данные успешно сохранены в S3: {key}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в S3: {str(e)}")
            raise
    else:
        logger.info("Нет новых данных для сохранения")

def load_from_s3_to_clickhouse(**kwargs):
    """Загрузка данных из S3 в ClickHouse"""
    clickhouse_client = create_clickhouse_client()
    minio_client = create_minio_client()
    
    # Получение списка файлов в бакете
    response = minio_client.list_objects_v2(
        Bucket=MINIO_CONFIG['bucket'],
        Prefix='parquet/'
    )
    
    # Получение последнего времени загрузки
    last_load_time = kwargs['ti'].xcom_pull(key='last_load_time')
    if last_load_time:
        last_load_time = datetime.fromisoformat(last_load_time)
    else:
        last_load_time = datetime.min
    
    # Загрузка новых файлов
    for obj in response.get('Contents', []):
        # Пропускаем уже обработанные файлы
        file_timestamp = datetime.strptime(obj['Key'].split('/')[-1].split('.')[0], '%Y-%m-%d-%H-%M-%S-%f')
        if file_timestamp <= last_load_time:
            logger.info(f"Пропуск уже обработанного файла {obj['Key']}")
            continue
            
        try:
            # Загрузка файла из MinIO во временный буфер
            response = minio_client.get_object(
                Bucket=MINIO_CONFIG['bucket'],
                Key=obj['Key']
            )
            
            # Чтение данных из Parquet
            buffer = BytesIO(response['Body'].read())
            table = pq.read_table(buffer)
            
            # Преобразование данных в список кортежей для вставки
            data = []
            for i in range(len(table)):
                row = table.slice(i, 1)
                data.append((
                    row.column('symbol')[0].as_py(),
                    row.column('price')[0].as_py(),
                    row.column('quantity')[0].as_py(),
                    row.column('trade_time')[0].as_py()
                ))
            
            # Вставка данных в ClickHouse
            insert_query = "INSERT INTO crypto_trades (symbol, price, quantity, trade_time) VALUES"
            clickhouse_client.execute(insert_query, data)
            logger.info(f"Данные успешно загружены из файла {obj['Key']}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из файла {obj['Key']}: {str(e)}")
            raise
    
    # Сохранение времени последней загрузки
    kwargs['ti'].xcom_push(
        key='last_load_time',
        value=datetime.now().isoformat()
    )

# Создание DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 1),
}

dag = DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for crypto data',
    schedule_interval='*/5 * * * *',
    catchup=False
)

# Определение задач
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

save_to_s3_task = PythonOperator(
    task_id='save_to_s3',
    python_callable=save_to_s3,
    dag=dag
)

load_from_s3_task = PythonOperator(
    task_id='load_from_s3_to_clickhouse',
    python_callable=load_from_s3_to_clickhouse,
    dag=dag
)

# Определение зависимостей
create_table_task >> save_to_s3_task >> load_from_s3_task 