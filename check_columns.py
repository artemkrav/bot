import pandas as pd

# Замените путь на ваш, если файл лежит не рядом со скриптом
csv_path = 'signals_log.csv'

# Загружаем файл в DataFrame
df = pd.read_csv(csv_path)

# Выводим названия столбцов
print("Список столбцов в signals_log.csv:")
print(df.columns)