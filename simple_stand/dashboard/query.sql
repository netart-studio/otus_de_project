WITH minute_prices AS (
    SELECT
        toDateTime(intDiv(toUInt32(trade_time), 10) * 10) as minute,
        avgIf(toFloat64(price), symbol = 'BTCUSDT') as btc_price,
        avgIf(toFloat64(price), symbol = 'ETHUSDT') as eth_price,
        avgIf(toFloat64(price), symbol = 'XRPUSDT') as xrp_price
    FROM {table}
    WHERE trade_time >= now() - toIntervalHour({interval})
    GROUP BY minute
    ORDER BY minute
),
btc_data AS (
    SELECT
        toDateTime(minute, 'Europe/Moscow') as minute,
        btc_price,
        avg(btc_price) OVER (ORDER BY minute ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) as btc_ma
    FROM minute_prices
),
eth_data AS (
    SELECT
        toDateTime(minute, 'Europe/Moscow') as minute,
        eth_price,
        avg(eth_price) OVER (ORDER BY minute ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) as eth_ma
    FROM minute_prices
),
xrp_data AS (
    SELECT
        toDateTime(minute, 'Europe/Moscow') as minute,
        xrp_price,
        avg(xrp_price) OVER (ORDER BY minute ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) as xrp_ma
    FROM minute_prices
),
hourly_stats AS (
    SELECT
        countIf(symbol = 'BTCUSDT') as btc_count,
        countIf(symbol = 'ETHUSDT') as eth_count,
        countIf(symbol = 'XRPUSDT') as xrp_count
    FROM {table}
    WHERE trade_time >= now() - toIntervalHour(1)
)
SELECT
    b.minute,
    b.btc_price,
    b.btc_ma,
    e.eth_price,
    e.eth_ma,
    x.xrp_price,
    x.xrp_ma,
    b.btc_price - e.eth_price as btc_eth_spread,
    b.btc_price - x.xrp_price as btc_xrp_spread,
    h.btc_count,
    h.eth_count,
    h.xrp_count
FROM btc_data b
JOIN eth_data e ON b.minute = e.minute
JOIN xrp_data x ON b.minute = x.minute
CROSS JOIN hourly_stats h
ORDER BY b.minute 