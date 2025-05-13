# Crypto ETL Service

Сервис для сбора данных о криптовалютных сделках с Binance и сохранения их в ClickHouse.

## Требования

- Go 1.21 или выше
- ClickHouse 22.0 или выше
- Доступ к Binance WebSocket API

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd crypto-etl
```

2. Установите зависимости:
```bash
go mod download
```

3. Создайте файл `.env` в директории `etl` и настройте параметры:
```env
# Binance WebSocket settings
BINANCE_WS_URL=wss://stream.binance.com:9443/ws

# ClickHouse settings
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_TABLE=crypto_trades

# Symbols to track (comma-separated)
SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT

# Log level (debug, info, warn, error)
LOG_LEVEL=info

# Batch settings
BATCH_SIZE=100  # Размер пакета для записи в ClickHouse
```

4. Создайте таблицу в ClickHouse:
```sql
CREATE TABLE IF NOT EXISTS crypto_trades (
    symbol String,
    price Float64,
    quantity Float64,
    trade_time DateTime
) ENGINE = MergeTree()
ORDER BY trade_time
```

## Запуск

```bash
go run main.go
```

## Функциональность

- Подключение к WebSocket API Binance
- Подписка на потоки сделок для указанных символов
- Пакетная запись данных в ClickHouse с настраиваемым размером пакета
- Принудительная отправка пакета каждую секунду для обеспечения актуальности данных
- Логирование операций
- Обработка ошибок и автоматическое переподключение

## Мониторинг

Сервис выводит логи в консоль с указанием времени и уровня важности. Уровень логирования можно настроить в файле `.env`.

## Структура данных

Каждая запись в ClickHouse содержит:
- symbol: Символ торговой пары
- price: Цена сделки
- quantity: Объем сделки
- trade_time: Время сделки

## Настройка производительности

Размер пакета для записи в ClickHouse можно настроить через параметр `BATCH_SIZE` в файле `.env`. По умолчанию используется значение 100 записей. Пакет также будет отправлен принудительно, если прошла 1 секунда с момента последней отправки, даже если он не заполнен полностью. 