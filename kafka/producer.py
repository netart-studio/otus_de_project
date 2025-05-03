import json
import logging
import os
import signal
import threading
import time
from datetime import datetime

import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance_trades")
KAFKA_POLL_INTERVAL = float(os.getenv("KAFKA_POLL_INTERVAL", "0"))

if not KAFKA_BOOTSTRAP_SERVERS:
    raise ValueError("Не задана переменная KAFKA_BOOTSTRAP_SERVERS в .env")

# Пары валют
SYMBOLS = os.getenv("SYMBOLS", "btcusdt").lower().split(",")
SYMBOLS = [s.strip() for s in SYMBOLS]

# Инициализация Kafka Producer
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
})

# Callback для проверки успешной доставки
def delivery_report(err, msg):
    if err:
        logging.error(f"Ошибка доставки сообщения в Kafka: {err}")
    else:
        logging.debug(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")

# Функция обработки сообщений
def on_message(ws, message):
    try:
        data = json.loads(message)
        # Проверяем, что это сделка
        if 'e' in data and data['e'] == 'trade':
            message = {
                'symbol': data['s'],
                'price': float(data['p']),
                'quantity': float(data['q']),
                'trade_time': data['T']
            }
            # Отправка в Kafka
            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps(message).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(KAFKA_POLL_INTERVAL)
            logging.info(f"Отправлено в Kafka: {message}")
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения: {e}")

def on_error(ws, error):
    logging.error(f"WebSocket ошибка: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.info("WebSocket соединение закрыто. Перезапуск через 5 сек...")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    logging.info("WebSocket соединение открыто")
    # Подписываемся на потоки для нескольких пар
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol}@trade" for symbol in SYMBOLS],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    logging.info(f"Подписаны на потоки: {[f'{s}@trade' for s in SYMBOLS]}")

# Функция запуска WebSocket
def start_websocket():
    ws_url = "wss://stream.binance.com:9443/ws"
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

# Обработчик сигналов завершения
def signal_handler(sig, frame):
    logging.info("Получен сигнал завершения. Очистка ресурсов...")
    producer.flush(timeout=5)
    logging.info("Ресурсы освобождены. Выход.")
    exit(0)

# Основная точка входа
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logging.info("Запуск WebSocket клиента для Binance...")
    websocket_thread = threading.Thread(target=start_websocket)
    websocket_thread.start()
    websocket_thread.join()