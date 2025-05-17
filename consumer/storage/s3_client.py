import os
import logging
import io
from datetime import datetime
import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self):
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
        self.access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bucket = os.getenv('MINIO_BUCKET', 'crypto-trades')
        self.batch_timeout = int(os.getenv('MINIO_BATCH_TIMEOUT_SECONDS', 60))
        self.client = None
        self.batch = []
        self.last_save_time = None

    def init_client(self):
        """Инициализация MinIO клиента и создание бакета"""
        logger.info(f"Initializing MinIO client with endpoint: {self.endpoint}")
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False
        )
        
        # Создаем бакет, если он не существует
        logger.info(f"Checking if bucket {self.bucket} exists")
        if not self.client.bucket_exists(self.bucket):
            logger.info(f"Creating bucket {self.bucket}")
            self.client.make_bucket(self.bucket)
            logger.info(f"Created bucket {self.bucket}")
        else:
            logger.info(f"Bucket {self.bucket} already exists")
        
        self.last_save_time = datetime.now()
        return self.client

    def add_to_batch(self, record):
        """Добавление записи в батч"""
        self.batch.append(record)

    def should_save(self):
        """Проверка необходимости сохранения батча"""
        if not self.last_save_time:
            return False
        return (datetime.now() - self.last_save_time).total_seconds() >= self.batch_timeout

    def save_batch(self):
        """Сохранение батча в MinIO"""
        if not self.batch:
            return

        try:
            # Create DataFrame from batch
            df = pd.DataFrame(self.batch)
            logger.info(f"Saving batch of {len(self.batch)} records to MinIO")

            # Преобразуем trade_time в datetime, если это не datetime
            if 'trade_time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['trade_time']):
                try:
                    df['trade_time'] = pd.to_datetime(df['trade_time'], unit='ms')
                except Exception as e:
                    logger.error(f"Failed to convert trade_time to datetime: {e}")
                    logger.error(f"Batch content: {df.head()} (columns: {df.columns})")
                    raise

            # Create object name with year/month structure and timestamp
            timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            year = df['trade_time'].iloc[0].strftime('%Y') if 'trade_time' in df.columns else 'unknown_year'
            month = df['trade_time'].iloc[0].strftime('%m') if 'trade_time' in df.columns else 'unknown_month'
            object_name = f"{year}/{month}/binance_crypto_{timestamp}.parquet"

            # Convert DataFrame to parquet bytes
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Upload to MinIO
            self.client.put_object(
                self.bucket,
                object_name,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            logger.info(f"Saved data to MinIO: {object_name}")

            # Clear batch and update last save time
            self.batch = []
            self.last_save_time = datetime.now()

        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")
            logger.error(f"Batch content: {self.batch}") 