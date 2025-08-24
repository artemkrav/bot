import pandas as pd
from candles_cache import candle_cache_get
import ast
import time

def parse_tp(tp_str):
    """Парсит список тейк-профитов из строки (например: '[0.044106, 0.044551]')"""
    try:
        return ast.literal_eval(tp_str)
    except Exception:
        return []

async def update_signal_results():
    df = pd.read_csv('signals_log.csv', header=None)
    df.columns = ['datetime','coin','price','direction','pattern','rsi','ema','volume','result','sl','tp']
    updated = False

    for idx, row in df.iterrows():
        if row['result'] != 0:
            continue  # уже проверено
        coin = row['coin']
        entry_price = float(row['price'])
        direction = row['direction']
        sl = float(row['sl']) if 'sl' in row and pd.notna(row['sl']) else None
        tp_list = parse_tp(row['tp']) if 'tp' in row and pd.notna(row['tp']) else []
        tf = "1m" # Можно сделать динамическим
        # Получить свежие свечи после сигнала (например, 20)
        candles = await candle_cache_get('binance', 'spot', coin, tf, 20)
        if candles is None or len(candles) == 0:
            continue
        closes = candles['close']
        result = 0
        # Для long
        if direction == 'up':
            # TP сработал?
            if any([close >= min(tp_list) for close in closes]):
                result = 1
            # SL сработал?
            elif any([close <= sl for close in closes]):
                result = -1
        # Для short
        elif direction == 'down':
            if any([close <= max(tp_list) for close in closes]):
                result = 1
            elif any([close >= sl for close in closes]):
                result = -1
        # Обновить result
        if result != 0:
            df.at[idx, 'result'] = result
            updated = True

    if updated:
        df.to_csv('signals_log.csv', header=False, index=False)
        print("Статистика обновлена.")

        # Запустить автообучение
        import subprocess
        subprocess.run(["python3", "auto_tune.py"])

if __name__ == "__main__":
    import asyncio
    asyncio.run(update_signal_results())