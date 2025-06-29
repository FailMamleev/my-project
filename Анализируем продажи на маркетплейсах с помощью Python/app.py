import warnings
warnings.filterwarnings("ignore", message="Parsing dates involving a day of month")

import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.express as px

# Генерация тестовых данных
def generate_data():
    np.random.seed(42)
    names = ['Смартфон', 'Ноутбук', 'Наушники', 'Телевизор', 'Планшет', 'Фотоаппарат']
    data = {
        'Название': [f"{names[i % len(names)]} {i+1}" for i in range(100)],
        'Цена': np.random.randint(5000, 50000, 100),
        'Рейтинг': np.round(np.random.uniform(3, 5, 100), 1),
        'Отзывы': np.random.randint(0, 500, 100)
    }
    df = pd.DataFrame(data)
    df['Цена со скидкой'] = (df['Цена'] * np.random.uniform(0.7, 0.95, 100)).astype(int)
    df['Скидка (%)'] = ((df['Цена'] - df['Цена со скидкой']) / df['Цена'] * 100).round(1)
    return df

# Инициализация приложения
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # Важно для деплоя

df = generate_data()

# Лейаут приложения
app.layout = dbc.Container([
    html.H1("Анализ товаров маркетплейса", className="mb-4"),
    
    # Фильтры
    dbc.Row([
        dbc.Col([
            html.Label("Диапазон цен:"),
            dcc.RangeSlider(
                id='price-slider',
                min=df['Цена'].min(),
                max=df['Цена'].max(),
                step=1000,
                value=[df['Цена'].min(), df['Цена'].max()],
                marks={i: f"{i//1000}k" for i in range(0, 50001, 10000)},
                tooltip={"placement": "bottom", "always_visible": True}
            )
        ], md=4),
        
        dbc.Col([
            html.Label("Минимальный рейтинг:"),
            dcc.Slider(
                id='rating-slider',
                min=3,
                max=5,
                step=0.1,
                value=4.0,
                marks={i: str(i) for i in [3, 3.5, 4, 4.5, 5]},
                tooltip={"placement": "bottom", "always_visible": True}
            )
        ], md=4),
        
        dbc.Col([
            html.Label("Минимальное количество отзывов:"),
            dcc.Input(
                id='reviews-input',
                type='number',
                min=0,
                max=500,
                value=100,
                step=10,
                className="form-control"
            )
        ], md=4)
    ], className="mb-4"),
    
    # Таблица
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(
                id='products-table',
                columns=[
                    {"name": "Название", "id": "Название"},
                    {"name": "Цена", "id": "Цена", "type": "numeric"},
                    {"name": "Цена со скидкой", "id": "Цена со скидкой", "type": "numeric"},
                    {"name": "Рейтинг", "id": "Рейтинг", "type": "numeric"},
                    {"name": "Отзывы", "id": "Отзывы", "type": "numeric"},
                    {"name": "Скидка (%)", "id": "Скидка (%)", "type": "numeric"}
                ],
                page_size=10,
                sort_action='native',
                style_table={'overflowX': 'auto'},
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px'
                },
                style_header={
                    'backgroundColor': 'lightgrey',
                    'fontWeight': 'bold'
                },
                filter_action='native'
            )
        ], width=12)
    ], className="mb-4"),
    
    # Графики
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='price-histogram')
        ], md=6),
        
        dbc.Col([
            dcc.Graph(id='discount-rating-plot')
        ], md=6)
    ])
], fluid=True)

# Callback для обновления данных
@app.callback(
    [Output('products-table', 'data'),
     Output('price-histogram', 'figure'),
     Output('discount-rating-plot', 'figure')],
    [Input('price-slider', 'value'),
     Input('rating-slider', 'value'),
     Input('reviews-input', 'value')]
)
def update_data(price_range, min_rating, min_reviews):
    filtered_df = df[
        (df['Цена'] >= price_range[0]) & 
        (df['Цена'] <= price_range[1]) & 
        (df['Рейтинг'] >= min_rating) & 
        (df['Отзывы'] >= min_reviews)
    ]
    
    # Гистограмма цен
    hist_fig = px.histogram(
        filtered_df,
        x='Цена',
        nbins=10,
        title='Распределение цен',
        labels={'Цена': 'Диапазон цен', 'count': 'Количество товаров'}
    )
    
    # График скидки vs рейтинг
    scatter_fig = px.scatter(
        filtered_df,
        x='Рейтинг',
        y='Скидка (%)',
        title='Зависимость скидки от рейтинга',
        trendline="lowess",
        hover_data=['Название', 'Цена']
    )
    
    return filtered_df.to_dict('records'), hist_fig, scatter_fig

if __name__ == '__main__':
    app.run(debug=True)  # Используем app.run() вместо app.run_server()