import os
import json
import logging
import time
import sys
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from storage.s3_client import S3Client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'binance_trades')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'crypto_consumer_group')

def wait_for_service(service_name, max_retries=30, retry_interval=1):
    """Ожидание доступности сервиса"""
    for i in range(max_retries):
        try:
            if service_name == 'kafka':
                consumer = Consumer({
                    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                    'group.id': KAFKA_GROUP_ID,
                    'auto.offset.reset': 'earliest'
                })
                consumer.subscribe([KAFKA_TOPIC])
                consumer.close()
            elif service_name == 'minio':
                s3_client = S3Client()
                s3_client.init_client()
                s3_client.client.list_buckets()
            logger.info(f"{service_name} is ready")
            return True
        except Exception as e:
            logger.warning(f"Waiting for {service_name}... ({i+1}/{max_retries})")
            time.sleep(retry_interval)
    return False

def process_messages():
    """Process messages from Kafka and save to S3"""
    # Initialize S3 client
    s3_client = S3Client()
    s3_client.init_client()
    
    # Initialize Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_TOPIC])
    
    logger.info("Consumer started and waiting for messages...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Error: {msg.error()}")
                    break
            
            try:
                # Parse message
                value = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Получено сообщение: {value}")
                
                # Add to batch
                s3_client.add_to_batch(value)
                
                # Check if we should save
                if s3_client.should_save():
                    s3_client.save_batch()
                
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        # Save remaining data
        if s3_client.batch:
            s3_client.save_batch()
        
        consumer.close()

def main():
    # Ожидание доступности сервисов
    services = ['kafka', 'minio']
    for service in services:
        if not wait_for_service(service):
            logger.error(f"Service {service} is not available")
            sys.exit(1)
    
    # Start processing messages
    process_messages()

if __name__ == "__main__":
    main() 