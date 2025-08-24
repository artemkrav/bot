import pandas as pd
import json
import os

def analyze_and_tune():
    # 1. Чтение истории сигналов
    if not os.path.isfile('signals_log.csv'):
        print("signals_log.csv не найден!")
        return
    df = pd.read_csv('signals_log.csv')
    # Если файл без заголовка, назначаем их вручную
    if df.columns[0] != 'datetime':
        df.columns = ['datetime','coin','price','direction','pattern','rsi','ema','volume','result','sl','tp']

    # 2. Анализ статистики
    total = len(df)
    wins = len(df[df['result'].astype(float) > 0])
    losses = len(df[df['result'].astype(float) < 0])
    winrate = wins / total if total > 0 else 0
    avg_profit = df['result'].astype(float).mean() if total > 0 else 0

    print(f"Winrate: {winrate:.2%}, Avg profit: {avg_profit:.4f}")

    # 3. Автоподбор параметров
    # Если нет файла настроек — создаём шаблон
    if not os.path.isfile('user_settings.json'):
        users = {"default": {"rr_targets": [1.0,2.0], "sl_atr_mult": 1.0}}
    else:
        with open('user_settings.json', 'r', encoding='utf-8') as f:
            users = json.load(f)
    for uid, cfg in users.items():
        # Пример: если winrate < 50% — уменьшить rr_targets (быстрее тейк), иначе оставить стандартные
        if winrate < 0.5:
            cfg['rr_targets'] = [0.8, 1.2]
            cfg['sl_atr_mult'] = 0.8
        elif winrate > 0.7:
            cfg['rr_targets'] = [1.2, 2.5]
            cfg['sl_atr_mult'] = 1.2
        else:
            cfg['rr_targets'] = [1.0, 2.0]
            cfg['sl_atr_mult'] = 1.0
    with open('user_settings.json', 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)
    print("Параметры обновлены!")

if __name__ == '__main__':
    analyze_and_tune()