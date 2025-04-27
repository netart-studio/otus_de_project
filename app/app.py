import websocket
import json
import threading
from datetime import datetime
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Настройки S3
S3_BUCKET = 'crypto-data'
S3_PREFIX = 'parquet/'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'

# Инициализация клиента S3
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# Буфер для пакетной записи данных
batch_buffer = []
BATCH_SIZE = 100 # Размер пакета для записи

# Функция для сохранения данных в S3 в формате Parquet
def save_to_s3(data):
    try:
        # Преобразуем данные в DataFrame
        df = pd.DataFrame(data, columns=['symbol', 'price', 'timestamp'])
        
        # Создаем Parquet-таблицу
        table = pa.Table.from_pandas(df)
        
        # Сохраняем в буфер
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Записываем данные в S3
        s3_key = f"{S3_PREFIX}{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer)
        logging.info(f"Данные успешно сохранены в S3: {s3_key}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных в S3: {e}")

# Обработчик сообщений от Binance WebSocket
def on_message(ws, message):
    global batch_buffer
    try:
        data = json.loads(message)
        
        # Извлекаем данные из сообщения
        symbol = data['s']
        price = float(data['p'])
        timestamp = datetime.utcfromtimestamp(data['E'] / 1000)  # Преобразуем время в формат datetime
        
        logging.info(f"Получено: {symbol}, Цена: {price}, Время: {timestamp}")
        
        # Добавляем данные в буфер
        batch_buffer.append((symbol, price, timestamp))
        
        # Если буфер достиг заданного размера, выполняем пакетную запись
        if len(batch_buffer) >= BATCH_SIZE:
            save_to_s3(batch_buffer)  # Сохраняем в S3
            batch_buffer = []  # Очищаем буфер после записи
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения: {e}")

# Обработчик ошибок
def on_error(ws, error):
    logging.error(f"Ошибка WebSocket: {error}")

# Обработчик закрытия соединения
def on_close(ws, close_status_code, close_msg):
    logging.info("WebSocket соединение закрыто")
    # При закрытии соединения записываем оставшиеся данные из буфера
    if batch_buffer:
        save_to_s3(batch_buffer)

# Обработчик открытия соединения
def on_open(ws):
    logging.info("WebSocket соединение открыто")
    # Подписываемся на потоки данных для нескольких пар валют
    symbols = ["btcusdt", "ethusdt", "xrpusdt"]  # Список пар валют
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@trade" for symbol in symbols],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    logging.info(f"Подписаны на потоки: {symbols}")

# Основная функция
def main():
    # URL для подключения к Binance WebSocket
    socket_url = "wss://stream.binance.com:9443/ws"

    # Создаем WebSocket-клиент
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open

    # Запускаем WebSocket в отдельном потоке
    threading.Thread(target=ws.run_forever).start()

if __name__ == "__main__":
    main()