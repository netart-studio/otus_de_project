import os
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Получение переменных окружения
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'secret')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'crypto')

def create_clickhouse_table():
    """Создание таблицы в ClickHouse, если она не существует"""
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crypto_trades (
        symbol String,
        price Float64,
        quantity Float64,
        trade_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY (symbol, trade_time)
    """
    
    client.execute(create_table_query)
    logger.info("Table crypto_trades created or already exists")

def main():
    # Создаем таблицу в ClickHouse
    create_clickhouse_table()
    
    # Инициализация ClickHouse клиента
    clickhouse_client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )
    
    # Инициализация Kafka consumer
    consumer = KafkaConsumer(
        'binance_trades',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='crypto_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info("Consumer started and waiting for messages...")
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")
            
            # Convert timestamp from milliseconds to datetime
            trade_time = datetime.fromtimestamp(data['trade_time'] / 1000)
            
            # Подготовка данных для вставки
            insert_data = [
                (
                    data['symbol'],
                    float(data['price']),
                    float(data['quantity']),
                    trade_time
                )
            ]
            
            # Вставка данных в ClickHouse
            clickhouse_client.execute(
                'INSERT INTO crypto_trades (symbol, price, quantity, trade_time) VALUES',
                insert_data
            )
            logger.info(f"Data inserted into ClickHouse: {data}")
            
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        consumer.close()
        clickhouse_client.disconnect()

if __name__ == "__main__":
    main() 