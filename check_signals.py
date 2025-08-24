import pandas as pd

# Загружаем файл без заголовка
df = pd.read_csv('signals_log.csv', header=None)

# Назначаем имена столбцов строго по порядку!
df.columns = [
    'datetime', 'coin', 'price', 'direction', 'pattern',
    'col6', 'col7', 'volume', 'result', 'sl', 'tp'
]

print("Столбцы:")
print(df.columns)
print("\nПервые строки:")
print(df.head())

print("\nСтолбец result:")
print(df['result'])