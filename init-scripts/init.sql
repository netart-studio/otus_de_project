-- Создаем движок Kafka для чтения данных
CREATE TABLE IF NOT EXISTS kafka_trades
(
    symbol String,
    price Float64,
    quantity Float64,
    trade_time DateTime64(3)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'binance_trades',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow',
         kafka_max_block_size = 1000;

-- Создаем материализованное представление для сохранения данных
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_trades_mv TO crypto_trades AS
SELECT
    symbol,
    price,
    quantity,
    trade_time
FROM kafka_trades;

-- Создаем таблицу для хранения данных
CREATE TABLE IF NOT EXISTS crypto_trades
(
    symbol String,
    price Float64,
    quantity Float64,
    trade_time DateTime64(3)
)
ENGINE = MergeTree()
ORDER BY (symbol, trade_time); 