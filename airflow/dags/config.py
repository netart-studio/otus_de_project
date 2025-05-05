from datetime import datetime, timedelta
from typing import Dict, Any

# ClickHouse settings
CLICKHOUSE_SETTINGS = {
    'host': 'clickhouse',
    'port': 9000,
    'user': 'default',
    'password': 'secret',
    'database': 'crypto'
}

# MinIO settings
MINIO_SETTINGS = {
    'endpoint': 'http://minio:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket': 'crypto-data',
    'parquet_path': 'parquet/'
}

# Kafka settings
KAFKA_SETTINGS = {
    'bootstrap_servers': 'kafka:29092',
    'topic': 'crypto_data',
    'group_id': 'crypto_consumer'
}

# DAG settings
DAG_SETTINGS = {
    'default_args': {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2025, 5, 1),
    },
    'schedule_interval': '*/5 * * * *',
    'catchup': False
}

# Table settings
TABLE_SETTINGS = {
    'crypto_data': {
        'table_name': 'crypto_data',
        'columns': [
            'symbol String',
            'price Float64',
            'volume Float64',
            'timestamp DateTime',
            'event_time DateTime'
        ],
        'engine': 'MergeTree()',
        'order_by': 'timestamp'
    },
    'crypto_aggregated': {
        'table_name': 'crypto_aggregated',
        'columns': [
            'symbol String',
            'avg_price Float64',
            'min_price Float64',
            'max_price Float64',
            'total_volume Float64',
            'timestamp DateTime'
        ],
        'engine': 'MergeTree()',
        'order_by': 'timestamp'
    }
}

# SQL queries
SQL_QUERIES = {
    'create_crypto_data_table': '''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        ) ENGINE = {engine}
        ORDER BY {order_by}
    ''',
    'create_aggregated_table': '''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        ) ENGINE = {engine}
        ORDER BY {order_by}
    ''',
    'insert_aggregated_data': '''
        INSERT INTO {table_name}
        SELECT 
            symbol,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price,
            sum(volume) as total_volume,
            toStartOfMinute(timestamp) as timestamp
        FROM {source_table}
        WHERE timestamp >= now() - INTERVAL 1 HOUR
        GROUP BY symbol, toStartOfMinute(timestamp)
    '''
} 