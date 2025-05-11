import websocket
import json
import threading
from datetime import datetime
from clickhouse_driver import Client
import logging
import os
from dotenv import load_dotenv
import time
import backoff

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Настройки ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'secret')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'crypto')

@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

# Создаем клиента для ClickHouse
clickhouse_client = get_clickhouse_client()

# Проверка наличия базы данных и её создание
def check_and_create_database():
    try:
        # Проверяем, существует ли база данных
        databases = clickhouse_client.execute("SHOW DATABASES")
        if CLICKHOUSE_DB not in [db[0] for db in databases]:
            logging.info(f"База данных '{CLICKHOUSE_DB}' не найдена. Создаем её...")
            clickhouse_client.execute(f"CREATE DATABASE {CLICKHOUSE_DB}")
            logging.info(f"База данных '{CLICKHOUSE_DB}' успешно создана.")
        else:
            logging.info(f"База данных '{CLICKHOUSE_DB}' уже существует.")
    except Exception as e:
        logging.error(f"Ошибка при проверке/создании базы данных: {e}")
        raise


# Создаем таблицу в ClickHouse (если она еще не создана)
def create_table():
    query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.crypto_prices (
        symbol String,
        price Float64,
        bid_price Float64,
        ask_price Float64,
        spread Float64,
        timestamp DateTime
    ) ENGINE = MergeTree()
    ORDER BY (symbol, timestamp)
    """
    try:
        clickhouse_client.execute(query)
        logging.info(f"Таблица '{CLICKHOUSE_DB}.crypto_prices' создана или уже существует.")
    except Exception as e:
        logging.error(f"Ошибка при создании таблицы: {e}")
        raise


# Буфер для пакетной вставки данных
batch_buffer = []
BATCH_SIZE = 10  # Размер пакета для вставки


# Пакетная вставка данных в ClickHouse
def insert_batch():
    global batch_buffer
    if not batch_buffer:
        return
    try:
        insert_query = f"INSERT INTO {CLICKHOUSE_DB}.crypto_prices (symbol, price, bid_price, ask_price, spread, timestamp) VALUES"
        clickhouse_client.execute(insert_query, batch_buffer)
        logging.info(f"Вставлено {len(batch_buffer)} записей в ClickHouse.")
        batch_buffer = []  # Очищаем буфер после вставки
    except Exception as e:
        logging.error(f"Ошибка при вставке данных в ClickHouse: {e}")
        # Не очищаем буфер при ошибке, чтобы попробовать вставить данные позже


# Обработчик сообщений от Binance WebSocket
def on_message(ws, message):
    global batch_buffer
    try:
        data = json.loads(message)
        
        # Проверяем, что это сообщение с данными о ценах
        if 'data' in data:
            data = data['data']
        
        # Извлекаем данные из сообщения
        symbol = data.get('s', '').lower()
        if not symbol:
            return
            
        # Получаем цены
        bid_price = float(data.get('b', 0))
        ask_price = float(data.get('a', 0))
        price = (bid_price + ask_price) / 2
        spread = ask_price - bid_price
        
        # Получаем временную метку
        event_time = int(data.get('E', 0))
        if event_time == 0:
            event_time = int(time.time() * 1000)  # Используем текущее время, если метка отсутствует
        timestamp = datetime.fromtimestamp(event_time / 1000)

        logging.info(f"Получено: {symbol}, Цена: {price}, Спред: {spread}, Время: {timestamp}")

        # Добавляем данные в буфер
        batch_buffer.append((symbol, price, bid_price, ask_price, spread, timestamp))

        # Если буфер достиг заданного размера, выполняем пакетную вставку
        if len(batch_buffer) >= BATCH_SIZE:
            insert_batch()
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения: {e}")


# Обработчик ошибок
def on_error(ws, error):
    logging.error(f"Ошибка WebSocket: {error}")


# Обработчик закрытия соединения
def on_close(ws, close_status_code, close_msg):
    logging.info("WebSocket соединение закрыто")
    # При закрытии соединения вставляем оставшиеся данные из буфера
    insert_batch()


# Обработчик открытия соединения
def on_open(ws):
    logging.info("WebSocket соединение открыто")
    # Подписываемся на потоки данных для нескольких пар валют
    symbols = ["btcusdt", "ethusdt", "xrpusdt"]  # Список пар валют
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@bookTicker" for symbol in symbols],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    logging.info(f"Подписаны на потоки: {symbols}")


# Основная функция
def main():
    while True:
        try:
            # Проверяем наличие базы данных и создаём её, если её нет
            check_and_create_database()

            # Создаем таблицу в ClickHouse
            create_table()

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
            break
        except Exception as e:
            logging.error(f"Ошибка при инициализации: {e}")
            time.sleep(5)  # Ждем 5 секунд перед повторной попыткой


if __name__ == "__main__":
    main()