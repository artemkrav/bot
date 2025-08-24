import pandas as pd
import json
from signal_patterns import (
    detect_choch_with_volume,
    detect_bos_and_zone,
    detect_rsi_divergence_with_volume
)
# Читаем логи сигналов
df = pd.read_csv('signals_log.csv', header=None)
df.columns = [
    'datetime', 'coin', 'price', 'direction',
    'pattern', 'rsi', 'ema', 'volume', 'result'
]

# Пример: ищем лучший порог разворота (reversal_threshold) — допустим, ты логируешь его как отдельную колонку
# Если такой колонки нет, можно анализировать по другим признакам (например, по rsi, pattern и т.д.)
# Ниже пример по паттерну

print("Средний профит по паттернам:")
pattern_stats = df.groupby('pattern')['result'].mean()
print(pattern_stats)

# --- Пример выбора лучшего значения параметра ---
# Допустим, ты анализировал reversal_threshold — например, вручную добавил такую колонку при логировании
# Если нет, то пока просто выбери лучший паттерн
best_pattern = pattern_stats.idxmax()
print(f"Лучший паттерн: {best_pattern}")

# --- Теперь, меняем параметр в config.json ---
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# Например, если хотим поменять фильтр pattern на best_pattern
config['best_pattern'] = best_pattern  # или другой параметр, который ты реально используешь

# Сохраняем обратно
with open('config.json', 'w', encoding='utf-8') as f:
    json.dump(config, f, indent=2)

print("Параметр best_pattern записан в config.json!")

def get_signals_stats():
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result'
        ]
        total = len(df)
        profit = len(df[df['result'] > 0])
        loss = len(df[df['result'] < 0])
        return f"Всего: {total} | Плюсовых: {profit} | Минусовых: {loss}"
    except Exception as e:
        return "Нет статистики"
        
def get_signals_stats():
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result'
        ]
        total = len(df)
        profit = len(df[df['result'] > 0])
        loss = len(df[df['result'] < 0])
        return f"Всего: {total} | Плюсовых: {profit} | Минусовых: {loss}"
    except Exception as e:
        return "Нет статистики"