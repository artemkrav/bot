import csv

def log_signal(data, filename="signals_log.csv"):
    """
    Записывает сигнал в CSV в правильном порядке.
    Ожидает data с ключами:
      'datetime', 'coin', 'price', 'direction', 'pattern', 'rsi', 'ema', 'volume', 'ai_result', 'sl', 'tp'
    ai_result -> result
    tp -> target_price (первый элемент) + target_range (строка)
    """
    row = [
        data.get('datetime'),                        # timestamp
        data.get('coin'),                            # symbol
        data.get('price'),                           # price
        data.get('direction'),                       # direction
        data.get('pattern'),                         # strategy
        data.get('rsi'),                             # rsi
        data.get('ema'),                             # ema
        data.get('volume'),                          # volume
        data.get('ai_result', -1),                   # result (AI метка: 1/0/-1)
        data.get('sl'),                              # stop-loss
        str(data.get('tp'))                          # TP всегда строкой!
    ]
    with open(filename, "a", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)