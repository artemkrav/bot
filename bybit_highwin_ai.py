import datetime
from candles_cache import candle_cache_get
from ai_trainer import predict_signal
from log_utils import log_signal

# --- Лучшие фильтры для высокого winrate ---
def detect_choch_with_volume(df, volume_mul=3, window=5):
    if df is None or len(df) < window + 3:
        return None
    prev_high = df['high'].iloc[-(window+3):-3].max()
    prev_low = df['low'].iloc[-(window+3):-3].min()
    cur_high = df['high'].iloc[-3:].max()
    cur_low = df['low'].iloc[-3:].min()
    choch_up = cur_high > prev_high and cur_low > prev_low
    choch_down = cur_low < prev_low and cur_high < prev_high
    avg_vol = df['volume'].iloc[-(window+3):-3].mean()
    last_vol = df['volume'].iloc[-1]
    volume_spike = last_vol > avg_vol * volume_mul
    if (choch_up or choch_down) and volume_spike:
        direction = "up" if choch_up else "down"
        msg = f"CHoCH ({direction}) + сильный объём: смена тренда {last_vol:.0f} (> {avg_vol:.0f})"
        return {
            "signal": "CHoCH+volume",
            "direction": direction,
            "msg": msg
        }
    return None

def detect_rsi_divergence_with_volume(df, rsi_period=21, volume_mul=2.5):
    if df is None or len(df) < rsi_period + 8:
        return None
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    roll_up = up.rolling(rsi_period).mean()
    roll_down = down.rolling(rsi_period).mean()
    rs = roll_up / (roll_down + 1e-6)
    rsi = 100 - (100 / (1 + rs))
    price_last = df['close'].iloc[-5:]
    rsi_last = rsi.iloc[-5:]
    bullish = price_last.iloc[-1] < price_last.iloc[0] and rsi_last.iloc[-1] > rsi_last.iloc[0]
    bearish = price_last.iloc[-1] > price_last.iloc[0] and rsi_last.iloc[-1] < rsi_last.iloc[0]
    avg_vol = df['volume'].iloc[-10:-5].mean()
    last_vol = df['volume'].iloc[-1]
    volume_spike = last_vol > avg_vol * volume_mul
    if bullish and volume_spike:
        return {
            "signal": "RSI_divergence_bull",
            "direction": "up",
            "msg": "Бычья дивергенция RSI + сильный объём: возможен разворот вверх"
        }
    elif bearish and volume_spike:
        return {
            "signal": "RSI_divergence_bear",
            "direction": "down",
            "msg": "Медвежья дивергенция RSI + сильный объём: возможен разворот вниз"
        }
    return None

def calculate_trade_levels(entry_price, direction, atr, rr_targets, sl_atr_mult=1.0):
    if direction == 'up':
        stop_loss = entry_price - sl_atr_mult * atr
        take_profits = [entry_price + rr * (entry_price - stop_loss) for rr in rr_targets]
    else:
        stop_loss = entry_price + sl_atr_mult * atr
        take_profits = [entry_price - rr * (stop_loss - entry_price) for rr in rr_targets]
    return round(stop_loss, 6), [round(tp, 6) for tp in take_profits]

async def strategy_bybit_highwin(context, chat_id, coin, tf, exchange, market_type, tv_url):
    candles_df = await candle_cache_get(exchange, market_type, coin.upper(), tf, 30)
    if candles_df is None or len(candles_df) < 20:
        print(f"Нет данных для Bybit стратегии {coin} {tf}")
        return

    sig1 = detect_choch_with_volume(candles_df)
    sig3 = detect_rsi_divergence_with_volume(candles_df)

    sl_atr_mult = 1.0
    rr_targets = [1.0, 2.0]
    atr_period = 14
    highs = candles_df['high']
    lows = candles_df['low']
    closes = candles_df['close']
    tr = (highs - lows).rolling(atr_period).mean()
    atr_val = tr.iloc[-1] if len(tr) > atr_period else 0.01
    entry_price = closes.iloc[-1]

    for sig in [sig1, sig3]:
        if sig:
            last_close = entry_price
            last_volume = candles_df['volume'].iloc[-1]
            rsi_val = float(candles_df['rsi'].iloc[-1]) if 'rsi' in candles_df.columns else None
            ema_val = float(candles_df['ema'].iloc[-1]) if 'ema' in candles_df.columns else None
            signal_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            direction = sig['direction']
            sl, tps = calculate_trade_levels(entry_price, direction, atr_val, rr_targets, sl_atr_mult)

            ai_result = None
            try:
                ai_result = predict_signal(
                    last_close,      # price
                    last_volume,     # volume
                    rsi_val,         # rsi
                    ema_val,         # ema
                    direction,       # direction
                    sig['signal'],   # strategy
                    coin,            # symbol
                    signal_time,     # timestamp
                    tp=tps           # tp (список TP)
                )
            except Exception as e:
                ai_result = f"AI ошибка: {e}"

            # Пропускаем, если AI не уверен (например, не 1)
            if ai_result != 1:
                print(f"AI не уверен по сигналу: {sig['signal']} - пропуск")
                continue

            signal_data = {
                'datetime': signal_time,
                'coin': coin,
                'price': last_close,
                'direction': direction,
                'pattern': sig['signal'],
                'rsi': rsi_val,
                'ema': ema_val,
                'volume': last_volume,
                'ai_result': ai_result,
                'sl': sl,
                'tp': tps
            }
            log_signal(signal_data)
            tp_text = ', '.join([str(tp) for tp in tps])
            risk_pct = abs((entry_price - sl) / entry_price * 100) if direction == 'up' else abs((sl - entry_price) / entry_price * 100)
            profit_pcts = [abs((tp - entry_price) / entry_price * 100) if direction == 'up' else abs((entry_price - tp) / entry_price * 100) for tp in tps]
            profit_pcts_text = ', '.join([f"{pp:.2f}" for pp in profit_pcts])
            trade_levels_text = (
                f"\nСтоп-лосс: {sl} ({risk_pct:.2f}%)"
                f"\nТейк-профит: {tp_text} ({profit_pcts_text}%)"
                f"\nATR: {atr_val:.6f}"
            )
            raw_message_text = (
                f"🚀 {coin} {tf}\n"
                f"🔔 Сигнал: {sig['msg']}\n"
                f"🎯 Стратегия: {sig['signal']}\n"
                f"Направление: {direction}\n"
                f"⏰ Время: {signal_time}\n"
                f"📦 Объем: {last_volume:.2f}\n"
                f"💵 Цена: {last_close:.4f}\n"
                f"🤖 AI прогноз: {ai_result}\n"
                f"{trade_levels_text}\n"
                f"📊 [TradingView]({tv_url})"
            )
            try:
                await context.bot.send_message(
                    chat_id,
                    raw_message_text,
                    parse_mode='Markdown'
                )
            except Exception as e:
                print(f"Ошибка при отправке уведомления: {e}")