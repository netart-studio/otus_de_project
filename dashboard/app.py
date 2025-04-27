import streamlit as st
import pandas as pd
from clickhouse_driver import Client
import plotly.express as px
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Должен быть ПЕРВЫМ вызовом Streamlit в скрипте
st.set_page_config(layout="wide")

# Затем загружаем остальные настройки
load_dotenv()

# Настройки подключения к ClickHouse из .env
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'secret')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'crypto')
REFRESH_INTERVAL = int(os.getenv('REFRESH_INTERVAL', 60))

def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

def get_time_filter_interval(period):
    """Возвращает интервал времени в зависимости от выбранного периода"""
    intervals = {
        '1 час': '1 HOUR',
        '1 день': '24 HOUR',
        '1 неделя': '168 HOUR'  # 7 дней * 24 часа
    }
    return intervals.get(period, '24 HOUR')

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_data(period):
    time_interval = get_time_filter_interval(period)
    client = get_clickhouse_client()
    query = f"""
    WITH minute_prices AS (
        SELECT
            toStartOfMinute(timestamp) AS minute,
            symbol,
            avg(price) AS price
        FROM crypto_prices
        WHERE timestamp >= now() - INTERVAL {time_interval}
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
    """
    try:
        result = client.execute(query)
        columns = ['minute', 'btc_price', 'btc_ma', 'eth_price', 'eth_ma', 
                 'xrp_price', 'xrp_ma', 'btc_eth_spread', 'btc_xrp_spread']
        df = pd.DataFrame(result, columns=columns)
        return df
    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {e}")
        return pd.DataFrame()

def create_price_chart(data, price_col, ma_col, title):
    fig = px.line(data, x='minute', y=[price_col, ma_col],
                 title=title,
                 labels={'value': 'Price', 'minute': 'Time'})
    fig.update_traces(line=dict(width=2), selector={'name': price_col})
    fig.update_traces(line=dict(width=1.5, dash='dot'), selector={'name': ma_col})
    fig.update_layout(
        legend_title_text='Metrics',
        margin=dict(l=20, r=20, t=40, b=20),
        height=300
    )
    return fig

def create_spread_chart(data, spread_col, title):
    fig = px.line(data, x='minute', y=spread_col,
                 title=title,
                 labels={'value': 'Spread', 'minute': 'Time'})
    fig.update_traces(line=dict(width=2))
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        height=300
    )
    return fig

def main():
    st.title("Криптовалютный дашборд")
    
    # Добавляем селектор периода времени
    time_period = st.radio(
        "Период отображения данных:",
        options=['1 час', '1 день', '1 неделя'],
        horizontal=True,
        index=1  # По умолчанию выбран 1 день
    )
    
    if st.button("Обновить данные"):
        st.cache_data.clear()
    
    df = load_data(time_period)

    if not df.empty:
        st.write(f"Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        st.write(f"Отображаемый период: {time_period}")
        
        # Получаем последние значения
        last_row = df.iloc[-1]
        
        # Графики цен с метриками
        st.subheader(f"Цены криптовалют со скользящим средним (1 час) - период: {time_period}")
        
        # BTC
        col1, col2 = st.columns([3, 1])
        with col1:
            fig_btc = create_price_chart(df, 'btc_price', 'btc_ma', 'BTC/USDT')
            st.plotly_chart(fig_btc, use_container_width=True)
        with col2:
            st.metric("Текущая цена", f"{last_row['btc_price']:.2f} USD")
            st.metric("Скользящее среднее", f"{last_row['btc_ma']:.2f} USD")
            delta_btc = last_row['btc_price'] - last_row['btc_ma']
            st.metric("Отклонение от MA", f"{delta_btc:.2f} USD", 
                      delta_color="inverse" if delta_btc < 0 else "normal")
        
        # ETH
        col1, col2 = st.columns([3, 1])
        with col1:
            fig_eth = create_price_chart(df, 'eth_price', 'eth_ma', 'ETH/USDT')
            st.plotly_chart(fig_eth, use_container_width=True)
        with col2:
            st.metric("Текущая цена", f"{last_row['eth_price']:.2f} USD")
            st.metric("Скользящее среднее", f"{last_row['eth_ma']:.2f} USD")
            delta_eth = last_row['eth_price'] - last_row['eth_ma']
            st.metric("Отклонение от MA", f"{delta_eth:.2f} USD", 
                      delta_color="inverse" if delta_eth < 0 else "normal")
        
        # XRP
        col1, col2 = st.columns([3, 1])
        with col1:
            fig_xrp = create_price_chart(df, 'xrp_price', 'xrp_ma', 'XRP/USDT')
            st.plotly_chart(fig_xrp, use_container_width=True)
        with col2:
            st.metric("Текущая цена", f"{last_row['xrp_price']:.2f} USD")
            st.metric("Скользящее среднее", f"{last_row['xrp_ma']:.2f} USD")
            delta_xrp = last_row['xrp_price'] - last_row['xrp_ma']
            st.metric("Отклонение от MA", f"{delta_xrp:.2f} USD", 
                      delta_color="inverse" if delta_xrp < 0 else "normal")

        # Графики спредов с метриками
        st.subheader(f"Спреды между криптовалютами - период: {time_period}")
        
        # BTC-ETH Spread
        col1, col2 = st.columns([3, 1])
        with col1:
            fig_spread1 = create_spread_chart(df, 'btc_eth_spread', 'BTC-ETH Spread')
            st.plotly_chart(fig_spread1, use_container_width=True)
        with col2:
            st.metric("Текущий спред", f"{last_row['btc_eth_spread']:.2f} USD")
            st.metric("Средний спред", f"{df['btc_eth_spread'].mean():.2f} USD")
        
        # BTC-XRP Spread
        col1, col2 = st.columns([3, 1])
        with col1:
            fig_spread2 = create_spread_chart(df, 'btc_xrp_spread', 'BTC-XRP Spread')
            st.plotly_chart(fig_spread2, use_container_width=True)
        with col2:
            st.metric("Текущий спред", f"{last_row['btc_xrp_spread']:.2f} USD")
            st.metric("Средний спред", f"{df['btc_xrp_spread'].mean():.2f} USD")
    else:
        st.warning("Нет данных для отображения. Проверьте подключение к ClickHouse.")

if __name__ == "__main__":
    while True:
        main()
        time.sleep(REFRESH_INTERVAL)
        st.rerun()