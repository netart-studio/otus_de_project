import json
import logging
import os
import signal
from datetime import datetime
from typing import List, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

# Настройка логирования
def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# Загрузка переменных окружения
load_dotenv()
setup_logging()

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
print(KAFKA_BOOTSTRAP_SERVERS)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "binance_consumer_group")

# Настройки S3
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")

# Параметры буфера
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

# Инициализация Kafka Consumer
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

# Инициализация S3 клиента
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# Буфер для пакетной записи
batch_buffer = []

def save_to_s3(data: List[Tuple[str, float, datetime]]):
    try:
        # Создание DataFrame
        df = pd.DataFrame(data, columns=['symbol', 'price', 'timestamp'])

        # Преобразование в Parquet
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # Генерация уникального имени файла
        timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S-%f")
        s3_key = f"{S3_PREFIX}{timestamp}.parquet"

        # Загрузка в S3
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
        logging.info(f"Данные успешно сохранены в S3: {s3_key}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в S3: {e}")

def commit_offsets():
    try:
        consumer.commit(asynchronous=False)
        logging.debug("Офсеты Kafka зафиксированы")
    except Exception as e:
        logging.warning(f"Не удалось зафиксировать офсеты: {e}")

def signal_handler(sig, frame):
    logging.info("Получен сигнал завершения. Сохранение оставшихся данных...")
    if batch_buffer:
        save_to_s3(batch_buffer)
    commit_offsets()
    consumer.close()
    logging.info("Работа завершена.")
    exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logging.info("Начало чтения из Kafka...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().str() == 'PARTITION_EOF':
                    logging.debug("Конец партиции")
                else:
                    raise KafkaException(msg.error())
            else:
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    symbol = value['symbol']
                    price = float(value['price'])
                    trade_time = datetime.utcfromtimestamp(value['trade_time'] / 1000)

                    batch_buffer.append((symbol, price, trade_time))

                    if len(batch_buffer) >= BATCH_SIZE:
                        save_to_s3(batch_buffer)
                        batch_buffer.clear()
                        commit_offsets()
                except json.JSONDecodeError as e:
                    logging.error(f"Ошибка парсинга JSON: {e}")
                except KeyError as e:
                    logging.error(f"Отсутствует поле в сообщении: {e}")
                except Exception as e:
                    logging.error(f"Ошибка обработки сообщения: {e}")
    finally:
        if batch_buffer:
            save_to_s3(batch_buffer)
        commit_offsets()
        consumer.close()

if __name__ == "__main__":
    main()