from binance import AsyncClient, BinanceSocketManager
from confluent_kafka import Producer
import asyncio
import json

# Kafka Config
conf = {
    'bootstrap.servers': 'kafka:29092',  # Docker service name
}
producer = Producer(conf)

async def binance_to_kafka(symbol='BTCUSDT'):
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.trade_socket(symbol)
    
    async with ts as tscm:
        while True:
            data = await tscm.recv()
            # Форматируем данные для Kafka
            message = {
                'symbol': data['s'],
                'price': float(data['p']),
                'quantity': float(data['q']),
                'trade_time': data['T']
            }
            # Отправка в Kafka
            producer.produce(
                topic='binance_trades',
                value=json.dumps(message).encode('utf-8')
            )
            producer.flush()
            print(f"Sent to Kafka: {message}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(binance_to_kafka())