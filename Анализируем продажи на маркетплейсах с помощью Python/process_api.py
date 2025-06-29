import requests
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import seaborn as sns

# Устанавливаем тему seaborn
sns.set_theme(style="whitegrid", context="notebook", font_scale=1.1)

# 1. Получение данных
response = requests.get(
    "http://localhost:8000/api/products/",
    params={
        "min_price": 30000,
        "min_rating": 4.0,
        "limit": 100
    }
)

if response.status_code != 200:
    print(f"Ошибка {response.status_code}: {response.text}")
    exit()

# 2. Подготовка данных
data = response.json()
df = pd.DataFrame(data['data'])

# Удаление дубликатов и обработка пропусков
df = df.drop_duplicates(subset=['article'])
df = df.dropna(subset=['brand', 'price', 'rating'])

# 3. Создание графиков
plt.figure(figsize=(18, 14))

# Форматирование чисел
def format_price(x, pos):
    return f"{int(x/1000)}k" if x >= 10000 else str(int(x))

price_formatter = FuncFormatter(format_price)

# График 1: Распределение цен по брендам (боксплот)
plt.subplot(2, 2, 1)
sns.boxplot(data=df, x='brand', y='price', hue='brand',
           palette="Blues", dodge=False, legend=False,
           showmeans=True,
           meanprops={"marker":"o", "markerfacecolor":"white", "markeredgecolor":"red"})
plt.title('Распределение цен по брендам', fontsize=14, pad=20)
plt.suptitle('')
plt.xlabel('')
plt.ylabel('Цена, руб.', fontsize=12)
plt.xticks(fontsize=9, rotation=90)
plt.yticks(fontsize=10)
plt.gca().yaxis.set_major_formatter(price_formatter)

# График 2: Средний рейтинг по брендам
plt.subplot(2, 2, 2)
rating_data = df.groupby('brand')['rating'].mean().sort_values()
sns.barplot(x=rating_data.values, y=rating_data.index, hue=rating_data.index,
           palette="viridis", dodge=False, legend=False)
plt.title('Средний рейтинг по брендам', fontsize=14, pad=20)
plt.xlabel('Средний рейтинг (1-5)', fontsize=12)
plt.ylabel('')
plt.xticks(fontsize=10)
plt.yticks(fontsize=9)
plt.grid(axis='x', linestyle='--', alpha=0.6)
plt.xlim(3, 5)

# График 3: Доля брендов на рынке
plt.subplot(2, 2, 3)
brand_dist = df['brand'].value_counts(normalize=True)
explode = [0.1 if i < 3 else 0 for i in range(len(brand_dist))]
plt.pie(brand_dist, 
       labels=brand_dist.index,
       autopct=lambda p: f'{p:.1f}%\n({int(p/100*len(df))})', 
       textprops={'fontsize': 9}, 
       colors=sns.color_palette("tab20c"),
       explode=explode,
       startangle=90)
plt.title('Доля брендов на рынке\n(с количеством товаров)', fontsize=14, pad=20)
plt.ylabel('')

# График 4: Количество товаров по брендам
plt.subplot(2, 2, 4)
count_data = df['brand'].value_counts().sort_values()
sns.barplot(x=count_data.values, y=count_data.index, hue=count_data.index,
           palette="dark:salmon_r", dodge=False, legend=False)
plt.title('Количество товаров по брендам', fontsize=14, pad=20)
plt.xlabel('Количество товаров', fontsize=12)
plt.ylabel('')
plt.xticks(fontsize=10)
plt.yticks(fontsize=9)
plt.grid(axis='x', linestyle='--', alpha=0.6)

# 4. Сохранение и отображение
plt.tight_layout(pad=4.0, w_pad=3.0, h_pad=3.0)
plt.savefig('brand_analysis_report.png', dpi=300, bbox_inches='tight')
plt.show()

# 5. Дополнительный анализ
print("\n=== Аналитический отчет ===")
print(f"Всего товаров: {len(df)}")
print(f"Уникальных брендов: {df['brand'].nunique()}")
print(f"Средняя цена: {df['price'].mean():,.0f} руб.")
print(f"Средний рейтинг: {df['rating'].mean():.2f}")

top_brands = df.groupby('brand').agg({
    'price': 'mean',
    'rating': 'mean',
    'article': 'count'
}).sort_values('article', ascending=False).head(5)

print("\nТоп-5 брендов по количеству товаров:")
print(top_brands.to_string(float_format=lambda x: f"{x:,.1f}"))