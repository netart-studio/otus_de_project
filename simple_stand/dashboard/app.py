import streamlit as st
import pandas as pd
from clickhouse_driver import Client
import plotly.express as px
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np
import pytz

# Должен быть ПЕРВЫМ вызовом Streamlit в скрипте
st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="📈",
    layout="wide"
)

# Скрытие toolbar через CSS
hide_toolbar = """
<style>
    .stApp header {
        visibility: hidden;
    }
</style>
"""

st.markdown(hide_toolbar, unsafe_allow_html=True)

# Затем загружаем остальные настройки
load_dotenv()

# Настройки подключения к ClickHouse из .env
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'secret')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'crypto')
CLICKHOUSE_TABLE = os.getenv('CLICKHOUSE_TABLE', 'crypto_trades')
REFRESH_INTERVAL = 1  # Обновление каждую секунду

# Настройка часового пояса
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

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
        '1 HOUR': 1,
        '6 HOUR': 6,
        '12 HOUR': 12,
        '24 HOUR': 24
    }
    return intervals.get(period, 6)

def load_sql_query():
    """Загружает SQL запрос из файла"""
    try:
        with open('query.sql', 'r') as file:
            return file.read()
    except Exception as e:
        st.error(f"Ошибка при чтении SQL файла: {e}")
        return None

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_data(period):
    """Загружает данные из ClickHouse с учетом временного диапазона"""
    time_interval = get_time_filter_interval(period)
    client = get_clickhouse_client()
    
    # Загружаем SQL запрос из файла
    query_template = load_sql_query()
    if query_template is None:
        return pd.DataFrame()
    
    # Подставляем параметры в запрос
    query = query_template.format(
        table=CLICKHOUSE_TABLE,
        interval=time_interval
    )
    
    try:
        # Выполняем запрос и получаем результат
        result = client.execute(query)
        
        # Проверяем, что результат не пустой
        if not result:
            st.warning("Нет данных для отображения")
            return pd.DataFrame()
            
        # Получаем имена колонок из результата
        columns = ['minute', 'btc_price', 'btc_ma', 'eth_price', 'eth_ma', 
                 'xrp_price', 'xrp_ma', 'btc_eth_spread', 'btc_xrp_spread',
                 'btc_count', 'eth_count', 'xrp_count']
        
        # Создаем DataFrame
        df = pd.DataFrame(result, columns=columns)
        
        # Проверяем, что все необходимые колонки присутствуют
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            st.error(f"Отсутствуют колонки: {missing_columns}")
            return pd.DataFrame()
        
        # Преобразуем столбец minute в datetime
        df['minute'] = pd.to_datetime(df['minute'])
        
        return df
    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {e}")
        return pd.DataFrame()

def create_price_chart(data, price_col, ma_col, title):
    """Создает график цены с линией тренда"""
    # Конвертируем время в московский часовой пояс
    data = data.copy()
    if data['minute'].dt.tz is None:
        data['minute'] = data['minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Moscow')
    else:
        data['minute'] = data['minute'].dt.tz_convert('Europe/Moscow')
    
    fig = px.line(data, x='minute', y=[price_col, ma_col],
                 title=title,
                 labels={'value': 'Price', 'minute': 'Time'})
    
    # Настройка стилей линий
    fig.update_traces(line=dict(width=1))
    fig.data[0].line.color = 'blue'  # Цена
    fig.data[1].line.color = 'red'   # Скользящее среднее
    fig.data[1].line.dash = 'dash'   # Пунктирная линия для MA
    
    # Настройка осей
    fig.update_xaxes(
        title_text='Time (Moscow)',
        tickformat='%H:%M',
        tickangle=45,
        nticks=10,  # Количество меток на оси
        range=[data['minute'].min(), data['minute'].max()]  # Устанавливаем диапазон оси X
    )
    fig.update_yaxes(title_text='Price')
    
    # Настройка легенды
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=20, r=20, t=40, b=40)  # Увеличиваем отступ снизу для меток времени
    )
    
    return fig

def create_spread_chart(data, spread_col, title):
    # Конвертируем время в московский часовой пояс
    data = data.copy()
    if data['minute'].dt.tz is None:
        data['minute'] = data['minute'].dt.tz_localize('UTC').dt.tz_convert('Europe/Moscow')
    else:
        data['minute'] = data['minute'].dt.tz_convert('Europe/Moscow')
    
    fig = px.line(data, x='minute', y=spread_col,
                 title=title,
                 labels={'value': 'Spread', 'minute': 'Time'})
    fig.update_traces(line=dict(width=2))
    
    # Настройка осей
    fig.update_xaxes(
        title_text='Time (Moscow)',
        tickformat='%H:%M',
        tickangle=45,
        nticks=10,  # Количество меток на оси
        range=[data['minute'].min(), data['minute'].max()]  # Устанавливаем диапазон оси X
    )
    
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=40),  # Увеличиваем отступ снизу для меток времени
        height=300
    )
    return fig

def get_moscow_time():
    """Возвращает текущее время в московском часовом поясе"""
    return datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')

def create_histogram(data):
    """Создает гистограмму количества записей по криптовалютам за последний час"""
    # Создаем DataFrame для гистограммы
    hist_data = pd.DataFrame({
        'Cryptocurrency': ['BTC', 'ETH', 'XRP'],
        'Count': [
            data['btc_count'].iloc[0],
            data['eth_count'].iloc[0],
            data['xrp_count'].iloc[0]
        ]
    })
    
    # Создаем гистограмму
    fig = px.bar(hist_data, 
                 x='Cryptocurrency', 
                 y='Count',
                 color='Cryptocurrency',
                 title='Number of Records per Cryptocurrency (Last Hour)')
    
    # Настройка осей
    fig.update_xaxes(title_text='Cryptocurrency')
    fig.update_yaxes(title_text='Number of Records')
    
    # Настройка легенды
    fig.update_layout(
        showlegend=False,  # Скрываем легенду, так как она избыточна
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

def main():
    """Основная функция приложения"""
    # Загрузка переменных окружения
    load_dotenv()
    
    # Настройки подключения к ClickHouse
    clickhouse_settings = {
        'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
        'port': int(os.getenv('CLICKHOUSE_PORT', 9000)),
        'user': os.getenv('CLICKHOUSE_USER', 'default'),
        'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
        'database': os.getenv('CLICKHOUSE_DATABASE', 'default'),
        'table': os.getenv('CLICKHOUSE_TABLE', 'crypto_trades')
    }
    
    # Инициализация состояния сессии
    if 'last_update' not in st.session_state:
        st.session_state.last_update = None
    if 'data' not in st.session_state:
        st.session_state.data = None
    if 'auto_update' not in st.session_state:
        st.session_state.auto_update = True
    if 'update_interval' not in st.session_state:
        st.session_state.update_interval = 60
    
    # Создание боковой панели с настройками
    with st.sidebar:
        st.title("⚙️ Настройки")
        
        # Настройки автообновления
        auto_update = st.checkbox("Автообновление", value=True)
        if auto_update:
            update_interval = st.slider("Интервал обновления (сек)", 5, 300, 5)
            st.session_state.update_interval = update_interval
        
        st.session_state.auto_update = auto_update
        
        # Кнопка ручного обновления
        if st.button("Обновить данные"):
            st.session_state.last_update = None
            st.cache_data.clear()
            st.experimental_rerun()
    
    # Загрузка и обработка данных
    df = load_data('24 HOUR')  # Фиксированный период 24 часа
    
    # Проверяем, что данные загружены и не пустые
    if df is None or df.empty:
        st.warning("Нет данных для отображения")
        return
    
    st.session_state.data = df
    st.session_state.last_update = datetime.now()
    
    # Отображение времени последнего обновления
    if st.session_state.last_update:
        st.write(f"Последнее обновление: {st.session_state.last_update.strftime('%H:%M:%S')}")
    
    # Создание вкладок
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["BTC/USDT", "ETH/USDT", "XRP/USDT", "Spreads", "Statistics", "Debug"])
    
    # Вкладка BTC
    with tab1:
        container = st.empty()
        with container.container():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("BTC Price", f"${df['btc_price'].iloc[-1]:,.2f}")
            with col2:
                st.metric("BTC MA", f"${df['btc_ma'].iloc[-1]:,.2f}")
            st.plotly_chart(create_price_chart(df, 'btc_price', 'btc_ma', 'BTC/USDT Price'), use_container_width=True)
    
    # Вкладка ETH
    with tab2:
        container = st.empty()
        with container.container():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("ETH Price", f"${df['eth_price'].iloc[-1]:,.2f}")
            with col2:
                st.metric("ETH MA", f"${df['eth_ma'].iloc[-1]:,.2f}")
            st.plotly_chart(create_price_chart(df, 'eth_price', 'eth_ma', 'ETH/USDT Price'), use_container_width=True)
    
    # Вкладка XRP
    with tab3:
        container = st.empty()
        with container.container():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("XRP Price", f"${df['xrp_price'].iloc[-1]:,.2f}")
            with col2:
                st.metric("XRP MA", f"${df['xrp_ma'].iloc[-1]:,.2f}")
            st.plotly_chart(create_price_chart(df, 'xrp_price', 'xrp_ma', 'XRP/USDT Price'), use_container_width=True)
    
    # Вкладка Spreads
    with tab4:
        container = st.empty()
        with container.container():
            # BTC-ETH Spread
            st.metric("BTC-ETH Spread", f"${df['btc_eth_spread'].iloc[-1]:,.2f}")
            st.plotly_chart(create_spread_chart(df, 'btc_eth_spread', 'BTC-ETH Spread'), use_container_width=True)
            
            # Добавляем разделитель
            st.markdown("---")
            
            # BTC-XRP Spread
            st.metric("BTC-XRP Spread", f"${df['btc_xrp_spread'].iloc[-1]:,.2f}")
            st.plotly_chart(create_spread_chart(df, 'btc_xrp_spread', 'BTC-XRP Spread'), use_container_width=True)
    
    # Вкладка Statistics
    with tab5:
        container = st.empty()
        with container.container():
            st.header("Statistics")
            st.plotly_chart(create_histogram(df), use_container_width=True)
    
    # Вкладка Debug
    with tab6:
        st.header("Debug Information")
        
        # Информация о временном диапазоне
        st.subheader("Time Range")
        st.write("Фиксированный период: 24 HOUR")
        st.write("Числовой интервал:", get_time_filter_interval('24 HOUR'))
        
        # Информация о данных
        st.subheader("Data Information")
        st.write("Количество строк:", len(df))
        st.write("Доступные колонки:", df.columns.tolist())
        
        if not df.empty:
            st.write("Первая строка:", df.iloc[0].to_dict())
            st.write("Последняя строка:", df.iloc[-1].to_dict())
            st.write("Временной диапазон:", df['minute'].min(), "до", df['minute'].max())
            st.write("Разница во времени:", df['minute'].max() - df['minute'].min())
        
        # SQL запрос
        st.subheader("SQL Query")
        query_template = load_sql_query()
        if query_template:
            query = query_template.format(
                table=CLICKHOUSE_TABLE,
                interval=get_time_filter_interval('24 HOUR')
            )
            st.code(query, language='sql')
        
        # Таблица с данными
        st.subheader("Last 100 Records")
        if not df.empty:
            # Форматируем только временную метку
            display_df = df.copy()
            display_df['minute'] = display_df['minute'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Отображаем последние 100 записей
            st.dataframe(display_df.tail(100), use_container_width=True)

    # Автоматическое обновление
    if st.session_state.auto_update:
        time.sleep(st.session_state.update_interval)
        st.experimental_rerun()

if __name__ == "__main__":
    main()