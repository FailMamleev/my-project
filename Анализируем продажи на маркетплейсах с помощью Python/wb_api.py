from flask import Flask, request, jsonify
import sqlite3
from functools import lru_cache
import json

app = Flask(__name__)

# Кэшированное подключение к БД
@lru_cache(maxsize=1)
def get_db_connection():
    conn = sqlite3.connect('wb_products.db')
    conn.row_factory = lambda cursor, row: {
        'id': row[0],
        'query': row[1],
        'page': row[2],
        'article': row[3],
        'name': row[4],
        'brand': row[5],
        'price': row[6],
        'sale_price': row[7],
        'discount': row[8],
        'rating': row[9],
        'feedbacks': row[10],
        'promo': row[11]
    }
    return conn

@app.route('/api/products/', methods=['GET'])
def get_filtered_products():
    """Эндпоинт для фильтрации товаров"""
    try:
        # Парсим параметры запроса
        filters = {
            'min_price': request.args.get('min_price', type=float),
            'max_price': request.args.get('max_price', type=float),
            'min_rating': request.args.get('min_rating', type=float),
            'min_feedbacks': request.args.get('min_feedbacks', type=int),
            'brand': request.args.get('brand', type=str),
            'page': max(1, request.args.get('page', 1, type=int)),
            'per_page': min(100, request.args.get('per_page', 20, type=int))
        }

        # Строим SQL-запрос
        query = """
        SELECT 
            id, query, page, article, name, brand, 
            price, sale_price, discount, rating, feedbacks, promo
        FROM products 
        WHERE 1=1
        """
        params = []
        
        # Условия фильтрации
        conditions = {
            'min_price': "price >= ?",
            'max_price': "price <= ?",
            'min_rating': "rating >= ?",
            'min_feedbacks': "feedbacks >= ?",
            'brand': "brand LIKE ?"
        }
        
        for key, condition in conditions.items():
            if filters[key] is not None:
                query += f" AND {condition}"
                params.append(filters[key] if key != 'brand' else f"%{filters[key]}%")

        # Пагинация
        query += " LIMIT ? OFFSET ?"
        params.extend([filters['per_page'], (filters['page']-1)*filters['per_page']])

        # Выполняем запрос
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        products = cursor.fetchall()

        # Получаем общее количество товаров
        count_query = "SELECT COUNT(*) FROM products" + query.split("FROM")[1].split("LIMIT")[0]
        total_count = conn.execute(count_query, params[:-2]).fetchone()[0]

        return jsonify({
            "success": True,
            "data": {
                "products": products,
                "pagination": {
                    "page": filters['page'],
                    "per_page": filters['per_page'],
                    "total_items": total_count,
                    "total_pages": (total_count + filters['per_page'] - 1) // filters['per_page']
                }
            }
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)