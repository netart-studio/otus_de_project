import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime, timedelta
import pandas as pd
from clickhouse_driver import Client
import os
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Загрузка переменных окружения
load_dotenv()

# Настройки ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'crypto')

# Инициализация ClickHouse клиента
clickhouse_client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

# Инициализация Dash приложения
app = dash.Dash(__name__)

# Функция для получения данных из ClickHouse
def get_data(symbol, time_range):
    try:
        # Вычисляем временной диапазон
        query = f"""
        SELECT 
            timestamp,
            price,
            bid_price,
            ask_price,
            spread
        FROM crypto_prices
        WHERE symbol = '{symbol}'
        ORDER BY timestamp DESC
        LIMIT {time_range * 12}  # Примерно 5 точек в минуту
        """
        
        logging.info(f"Выполняем запрос: {query}")
        result = clickhouse_client.execute(query)
        
        if not result:
            logging.warning(f"Нет данных для {symbol}")
            return pd.DataFrame(columns=['timestamp', 'price', 'bid_price', 'ask_price', 'spread'])
            
        df = pd.DataFrame(result, columns=['timestamp', 'price', 'bid_price', 'ask_price', 'spread'])
        logging.info(f"Получено {len(df)} записей для {symbol}")
        logging.info(f"Диапазон данных: от {df['timestamp'].min()} до {df['timestamp'].max()}")
        logging.info(f"Пример данных:\n{df.head().to_string()}")
        
        # Сортируем по времени по возрастанию для графиков
        df = df.sort_values('timestamp')
        
        return df
    except Exception as e:
        logging.error(f"Ошибка при получении данных: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame(columns=['timestamp', 'price', 'bid_price', 'ask_price', 'spread'])

# Создание layout
app.layout = html.Div([
    html.H1('Криптовалютные котировки в реальном времени'),
    
    html.Div([
        html.Label('Выберите криптовалюту:'),
        dcc.Dropdown(
            id='symbol-dropdown',
            options=[
                {'label': 'BTC/USDT', 'value': 'btcusdt'},
                {'label': 'ETH/USDT', 'value': 'ethusdt'},
                {'label': 'XRP/USDT', 'value': 'xrpusdt'}
            ],
            value='btcusdt'
        ),
        
        html.Label('Временной интервал (минуты):'),
        dcc.Dropdown(
            id='time-range-dropdown',
            options=[
                {'label': '5 минут', 'value': 5},
                {'label': '15 минут', 'value': 15},
                {'label': '30 минут', 'value': 30},
                {'label': '1 час', 'value': 60}
            ],
            value=15
        )
    ], style={'width': '30%', 'margin': '20px'}),
    
    dcc.Graph(id='price-chart'),
    dcc.Graph(id='spread-chart'),
    
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # обновление каждые 5 секунд
        n_intervals=0
    )
])

# Callback для обновления графиков
@app.callback(
    [Output('price-chart', 'figure'),
     Output('spread-chart', 'figure')],
    [Input('symbol-dropdown', 'value'),
     Input('time-range-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_graphs(symbol, time_range, n):
    try:
        logging.info(f"Обновление графиков для {symbol}, интервал {time_range} минут")
        df = get_data(symbol, time_range)
        
        if df.empty:
            logging.warning("Получен пустой DataFrame")
            # Возвращаем пустые графики с сообщением об отсутствии данных
            empty_fig = {
                'data': [],
                'layout': go.Layout(
                    title=f'Нет данных для {symbol.upper()}',
                    xaxis={'title': 'Время'},
                    yaxis={'title': 'Цена (USDT)'}
                )
            }
            return empty_fig, empty_fig
        
        logging.info(f"Создаем графики для {len(df)} точек данных")
        # График цен
        price_fig = {
            'data': [
                go.Scatter(
                    x=df['timestamp'],
                    y=df['price'],
                    name='Цена',
                    line=dict(color='blue')
                ),
                go.Scatter(
                    x=df['timestamp'],
                    y=df['bid_price'],
                    name='Bid',
                    line=dict(color='green')
                ),
                go.Scatter(
                    x=df['timestamp'],
                    y=df['ask_price'],
                    name='Ask',
                    line=dict(color='red')
                )
            ],
            'layout': go.Layout(
                title=f'Котировки {symbol.upper()}',
                xaxis={'title': 'Время'},
                yaxis={'title': 'Цена (USDT)'},
                hovermode='x unified'
            )
        }
        
        # График спреда
        spread_fig = {
            'data': [
                go.Scatter(
                    x=df['timestamp'],
                    y=df['spread'],
                    name='Спред',
                    line=dict(color='purple')
                )
            ],
            'layout': go.Layout(
                title=f'Спред {symbol.upper()}',
                xaxis={'title': 'Время'},
                yaxis={'title': 'Спред (USDT)'},
                hovermode='x unified'
            )
        }
        
        return price_fig, spread_fig
    except Exception as e:
        logging.error(f"Ошибка при обновлении графиков: {e}")
        # Возвращаем пустые графики с сообщением об ошибке
        error_fig = {
            'data': [],
            'layout': go.Layout(
                title='Ошибка при обновлении данных',
                xaxis={'title': 'Время'},
                yaxis={'title': 'Цена (USDT)'}
            )
        }
        return error_fig, error_fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)