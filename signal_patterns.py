import pandas as pd
import numpy as np
import datetime
import time
import re
from candles_cache import candle_cache_get
from log_utils import log_signal
from ai_trainer import predict_signal
from bybit_highwin_ai import strategy_bybit_highwin

# --- Супер-экранирование для MarkdownV2 ---
def md2_safe(text):
    # Экранирует все спецсимволы для MarkdownV2, включая минус, скобки, проценты, запятые, точки и т.д.
    return re.sub(r'([_\*\[\]\(\)\~\`\>\#\+\-\=\|\{\}\.\!\%\,\-])', r'\\\1', str(text))

last_signal_time = {}

def calculate_trade_levels(entry_price, direction, atr, rr_targets, sl_atr_mult=1.0):
    if direction == 'up':
        stop_loss = entry_price - sl_atr_mult * atr
        take_profits = [entry_price + rr * (entry_price - stop_loss) for rr in rr_targets]
    else:
        stop_loss = entry_price + sl_atr_mult * atr
        take_profits = [entry_price - rr * (stop_loss - entry_price) for rr in rr_targets]
    return round(stop_loss, 6), [round(tp, 6) for tp in take_profits]

def detect_choch_with_volume(df, volume_mul=2, window=5):
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
        msg = f"CHoCH ({direction}) + объём: смена тренда с объёмом {last_vol:.0f} (> {avg_vol:.0f})"
        return {
            "signal": "CHoCH+volume",
            "direction": direction,
            "msg": msg
        }
    return None

def detect_bos_and_zone(df, support_zones, resistance_zones, window=5):
    if df is None or len(df) < window + 3:
        return None
    prev_high = df['high'].iloc[-(window+3):-3].max()
    prev_low = df['low'].iloc[-(window+3):-3].min()
    cur_high = df['high'].iloc[-3:].max()
    cur_low = df['low'].iloc[-3:].min()
    last_close = df['close'].iloc[-1]
    bos_up = cur_high > prev_high
    bos_down = cur_low < prev_low
    zone_delta = last_close * 0.002
    in_support = any(abs(last_close - z) <= zone_delta for z in support_zones)
    in_resistance = any(abs(last_close - z) <= zone_delta for z in resistance_zones)
    if bos_up and in_resistance:
        msg = f"BOS вверх + вход в зону сопротивления ({last_close:.2f})"
        return {
            "signal": "BOS+resistance",
            "direction": "up",
            "msg": msg
        }
    elif bos_down and in_support:
        msg = f"BOS вниз + вход в зону поддержки ({last_close:.2f})"
        return {
            "signal": "BOS+support",
            "direction": "down",
            "msg": msg
        }
    return None

def detect_rsi_divergence_with_volume(df, rsi_period=14, volume_mul=2):
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
        msg = "Бычья дивергенция RSI + объём: возможен разворот вверх"
        return {
            "signal": "RSI_divergence_bull",
            "direction": "up",
            "msg": msg
        }
    elif bearish and volume_spike:
        msg = "Медвежья дивергенция RSI + объём: возможен разворот вниз"
        return {
            "signal": "RSI_divergence_bear",
            "direction": "down",
            "msg": msg
        }
    return None

async def check_signals(
    context,
    chat_id,
    coin,
    tf,
    exchange,
    market_type,
    tv_url,
    support_zones=None,
    resistance_zones=None,
    min_candles=10
):
    # --- Новый шаг: Если Bybit — используем AI-стратегию с высоким winrate ---
    if exchange.lower() == "bybit":
        await strategy_bybit_highwin(context, chat_id, coin, tf, exchange, market_type, tv_url)
        return
        
    candles_df = await candle_cache_get(exchange, market_type, coin.upper(), tf, 30)
    print(f"DEBUG: Запрос свечей {exchange} {market_type} {coin.upper()} {tf} -> {candles_df.shape if candles_df is not None else None}")
    # Исправлено: уменьшен порог для анализа сигналов
    if candles_df is None or len(candles_df) < min_candles:
        print(f"{coin} {tf}: получено {len(candles_df) if candles_df is not None else 0} свечей")
        print(f"Нет достаточных данных для {coin} {tf}")
        return

    sig1 = detect_choch_with_volume(candles_df)
    sig2 = detect_bos_and_zone(candles_df, support_zones or [], resistance_zones or [])
    sig3 = detect_rsi_divergence_with_volume(candles_df)

    settings = None
    try:
        settings = context.bot_data.get("user_settings", {}).get(chat_id)
    except Exception:
        pass
    if settings is None:
        sl_atr_mult = 1.0
        rr_targets = [1.0, 2.0]
    else:
        sl_atr_mult = settings.get('sl_atr_mult', 1.0)
        rr_targets = settings.get('rr_targets', [1.0, 2.0])

    atr_period = 14
    highs = candles_df['high']
    lows = candles_df['low']
    closes = candles_df['close']
    tr = (highs - lows).rolling(atr_period).mean()
    atr_val = tr.iloc[-1] if len(tr) > atr_period else 0.01
    entry_price = closes.iloc[-1]

    for sig in [sig1, sig2, sig3]:
        if sig:
            key = f"{coin}_{tf}_{sig['signal']}"
            now = time.time()
            if key in last_signal_time and now - last_signal_time[key] < 5 * 60:
                # Повторный сигнал — пропуск (антиспам)
                continue
            last_signal_time[key] = now

            direction = sig['direction']
            sl, tps = calculate_trade_levels(entry_price, direction, atr_val, rr_targets, sl_atr_mult)

            last_close = entry_price
            last_volume = candles_df['volume'].iloc[-1]
            signal_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            direction_emoji = "🟢" if sig['direction'] in ["up", "Buy"] else "🔴" if sig['direction'] in ["down", "Sell"] else "⚪️"
            pattern_emoji = {
                "CHoCH+volume": "🔄",
                "BOS+resistance": "🚦",
                "BOS+support": "🚦",
                "RSI_divergence_bull": "🧭",
                "RSI_divergence_bear": "🧭",
            }
            emoji = pattern_emoji.get(sig['signal'], "✨")

            # Проценты риска и прибыли
            if direction == 'up':
                risk_pct = abs((entry_price - sl) / entry_price * 100)
                profit_pcts = [abs((tp - entry_price) / entry_price * 100) for tp in tps]
            else:
                risk_pct = abs((sl - entry_price) / entry_price * 100)
                profit_pcts = [abs((entry_price - tp) / entry_price * 100) for tp in tps]

            # --- Получаем реальные индикаторы из candles_df ---
            rsi_val = float(candles_df['rsi'].iloc[-1]) if 'rsi' in candles_df.columns else None
            ema_val = float(candles_df['ema'].iloc[-1]) if 'ema' in candles_df.columns else None

            # --- AI прогноз ---
            ai_result = None  # Всегда объявляем заранее!
            try:
                ai_result = predict_signal(
                    last_close,         # price
                    last_volume,        # volume
                    last_close,         # target_price (или tp, если есть)
                    rsi_val,            # rsi
                    ema_val,            # ema
                    direction,          # up/down
                    sig['signal'],      # стратегия/паттерн
                    coin,               # symbol
                    signal_time         # timestamp
                )
            except Exception as e:
                ai_result = f"AI ошибка: {e}"

            if ai_result is None:
                ai_result = "Нет прогноза"

            # Логирование
            signal_data = {
                'datetime': signal_time,
                'coin': coin,
                'price': last_close,
                'direction': sig['direction'],
                'pattern': sig['signal'],
                'rsi': rsi_val,
                'ema': ema_val,
                'volume': last_volume,
                'ai_result': ai_result,
                'sl': sl,
                'tp': tps
            }
            log_signal(signal_data)

            # Формируем весь текст без экранирования
            tp_text = ', '.join([str(tp) for tp in tps])
            profit_pcts_text = ', '.join([f"{pp:.2f}" for pp in profit_pcts])
            trade_levels_text = (
                f"\nСтоп-лосс: {sl} ({risk_pct:.2f}\\%)"
                f"\nТейк-профит: {tp_text} ({profit_pcts_text}\\%)"
                f"\nATR: {atr_val:.6f}"
            )
            raw_message_text = (
                f"{emoji} {coin} {tf}\n"
                f"🔔 Сигнал: {sig['msg']}\n"
                f"🎯 Стратегия: {sig['signal']}\n"
                f"{direction_emoji} Направление: {sig['direction']}\n"
                f"⏰ Время: {signal_time}\n"
                f"📦 Объем: {last_volume:.2f}\n"
                f"💵 Цена: {last_close:.4f}\n"
                f"🤖 AI прогноз: {ai_result}\n"
                f"{trade_levels_text}\n"
                f"📊 [TradingView]({tv_url})"
            )
            message_text = md2_safe(raw_message_text)

            try:
                await context.bot.send_message(
                    chat_id,
                    message_text,
                    parse_mode='MarkdownV2'
                )
            except Exception as e:
                print(f"Ошибка при отправке уведомления: {e}")