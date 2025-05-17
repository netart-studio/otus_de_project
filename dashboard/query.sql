 WITH minute_prices AS (
        SELECT
            toStartOfMinute(trade_time) AS minute,
            symbol,
            avg(price) AS price
        FROM crypto_trades
        WHERE trade_time >= now() - INTERVAL {time_interval} AND price is not NULL
        GROUP BY minute, symbol
    ),
    btc_data AS (
        SELECT 
            minute, 
            price AS btc_price,
            avg(price) OVER (ORDER BY minute ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS btc_ma
        FROM minute_prices 
        WHERE symbol = 'BTCUSDT'
    ),
    eth_data AS (
        SELECT 
            minute, 
            price AS eth_price,
            avg(price) OVER (ORDER BY minute ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS eth_ma
        FROM minute_prices 
        WHERE symbol = 'ETHUSDT'
    ),
    xrp_data AS (
        SELECT 
            minute, 
            price AS xrp_price,
            avg(price) OVER (ORDER BY minute ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS xrp_ma
        FROM minute_prices 
        WHERE symbol = 'XRPUSDT'
    )
    SELECT
        b.minute,
        b.btc_price,
        b.btc_ma,
        e.eth_price,
        e.eth_ma,
        x.xrp_price,
        x.xrp_ma,
        b.btc_price - e.eth_price AS btc_eth_spread,
        b.btc_price - x.xrp_price AS btc_xrp_spread
    FROM btc_data b
    LEFT JOIN eth_data e ON b.minute = e.minute
    LEFT JOIN xrp_data x ON b.minute = x.minute
    WHERE e.eth_price IS NOT NULL AND x.xrp_price IS NOT NULL
    ORDER BY b.minute