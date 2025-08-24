import asyncio
import json
import subprocess
import datetime
import logging
import urllib.parse
import os
import traceback
import pandas as pd
import ast
from ai_trainer import predict_signal

ADMIN_ID = int(os.getenv('ADMIN_ID', '937124861'))

# ===================== PARSE TP =====================
def parse_tp(tp_str):
    try:
        return ast.literal_eval(str(tp_str))
    except Exception:
        return []

# ===================== SIGNALS AUTO-UPDATE =====================
async def auto_update_signal_results(context, interval_sec=300):
    import pandas as pd
    from candles_cache import candle_cache_get
    import subprocess

    while True:
        try:
            df = pd.read_csv('signals_log.csv', header=None)
            df.columns = ['datetime', 'coin', 'price', 'direction', 'pattern', 'rsi', 'ema', 'volume', 'result', 'sl', 'tp']
            updated = False

            for idx, row in df.iterrows():
                if row['result'] != 0:
                    continue
                coin = row['coin']
                entry_price = float(row['price'])
                direction = row['direction']
                sl = float(row['sl']) if 'sl' in row and pd.notna(row['sl']) else None
                tp_list = parse_tp(row['tp']) if 'tp' in row and pd.notna(row['tp']) else []
                tf = "1m"  # Можно сделать динамическим
                candles = await candle_cache_get('binance', 'spot', coin, tf, 20)
                if candles is None or len(candles) == 0:
                    continue
                closes = candles['close']
                result = 0
                if direction == 'up':
                    if any([close >= min(tp_list) for close in closes]):
                        result = 1
                    elif any([close <= sl for close in closes]):
                        result = -1
                elif direction == 'down':
                    if any([close <= max(tp_list) for close in closes]):
                        result = 1
                    elif any([close >= sl for close in closes]):
                        result = -1
                if result != 0:
                    df.at[idx, 'result'] = result
                    updated = True

            if updated:
                df.to_csv('signals_log.csv', header=False, index=False)
                print("Статистика по сигналам обновлена.")
                # Запустить автообучение
                subprocess.run(["python3", "auto_tune.py"])
        except Exception as e:
            print(f"Auto update signals error: {e}")
        await asyncio.sleep(interval_sec)

# ===================== UTILITY FUNCTIONS =====================
from pathlib import Path
from typing import Tuple, Optional, Dict, Any, List
from contextlib import suppress

# ===================== IMPORTS =====================
from users_manager import load_users, save_users, register_user, get_all_users
import httpx
import websockets
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.helpers import escape_markdown
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from signal_patterns import check_signals
from charting import generate_chart_image, analyze_extremes
from candles_cache import candle_cache_get, candle_cache_prefetch
from dynamic_symbols import get_binance_symbols, get_bybit_symbols


# --- Супер-экранирование для MarkdownV2 ---
def md2_safe(text):
    import re
    return re.sub(r'([_\*\[\]\(\)\~\`\>\#\+\-\=\|\{\}\.\!\%\,\-])', r'\\\1', str(text))

def get_signals_stats():
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result', 'sl', 'tp'
        ]
        total = len(df)
        profit = len(df[df['result'] > 0])
        loss = len(df[df['result'] < 0])
        return f"Всего сигналов: {total} | Плюсовых: {profit} | Минусовых: {loss}"
    except Exception as e:
        return "Нет статистики"

def get_last_signals(n=3):
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result', 'sl', 'tp'
        ]
        last = df.tail(n)
        lines = []
        for idx, row in last.iterrows():
            dir_text = "🟢" if row['direction'] == "up" else "🔴"
            # Исправлено: передаем корректные параметры в predict_signal
            try:
                ai_result = predict_signal(
                    row['price'],        # price
                    row['volume'],       # volume
                    row['rsi'],          # rsi
                    row['ema'],          # ema
                    row['direction'],    # direction
                    "SIGNAL",            # strategy
                    row['coin'],         # symbol
                    row['datetime'],     # timestamp
                    tp=row['tp']         # tp (опционально)
                )
            except Exception as e:
                ai_result = f"AI ошибка: {e}"
            lines.append(
                f"{row['datetime'][-8:]} {row['coin']} {dir_text} {row['pattern']} "
                f"SL:{row['sl']} TP:{row['tp']} V:{row['volume']} | Рез: {row['result']} | AI прогноз: {ai_result}"
            )
        return "\n".join(lines)
    except Exception as e:
        return "Нет данных по сигналам"
        # ===================== ОКРУЖЕНИЕ =====================
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TV_LAYOUT_ID = os.getenv('TV_LAYOUT_ID', '').strip()
ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

SUBSCRIPTIONS_ENABLED = os.getenv('SUBSCRIPTIONS_ENABLED', 'false').lower() == 'true'

# ===================== ЛОГИ =====================
logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("bot")

# ===================== КОНСТАНТЫ =====================
CONFIRM_THRESHOLD_DEFAULT = 5.0
FAST_THRESHOLD_DEFAULT = 3.0

CONFIRM_COOLDOWN = 10
FAST_COOLDOWN = 25
MIN_SECONDS_BETWEEN_FAST_AND_CONFIRM = 2

CHECK_INTERVAL_DEFAULT = 10
BINANCE_PING_INTERVAL = 20
BYBIT_PING_INTERVAL = 10

EXCHANGE_CHOICES = {'binance': 'Binance', 'bybit': 'Bybit'}
MARKET_TYPE_CHOICES = {'spot': 'Спот', 'futures': 'Фьючерс'}
ALLOWED_TF = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]
TIMEFRAME_DEFAULT = "1h"
CHART_CANDLES_LIMIT = 30
EXTREME_LOOKBACK = 60

# Повторы (дефолты и пределы)
REPEAT_STEP_DEFAULT = 1.0
REPEAT_COOLDOWN_DEFAULT = 5
REPEAT_STEP_MIN = 0.05
REPEAT_STEP_MAX = 25.0
REPEAT_COOLDOWN_MIN = 1
REPEAT_COOLDOWN_MAX = 3600

# Параметры +/- кнопок
REPEAT_STEP_FINE_DELTA = 0.1
REPEAT_STEP_COARSE_DELTA = 0.5
REPEAT_CD_FINE_DELTA = 1
REPEAT_CD_COARSE_DELTA = 5

BYBIT_WS_URLS = {
    'spot': 'wss://stream.bybit.com/v5/public/spot',
    'futures': 'wss://stream.bybit.com/v5/public/linear',
}
BYBIT_SPOT_TICKERS_URL = "https://api.bybit.com/v5/market/tickers?category=spot"
BYBIT_FUTURES_TICKERS_URL = "https://api.bybit.com/v5/market/tickers?category=linear"

BINANCE_SPOT_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_FUT_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

SUBSCRIPTIONS_FILE = "subscriptions.json"
SUBSCRIPTIONS: Dict[str, Dict[str, Any]] = {}
TRIAL_DURATION_SECONDS = 24 * 3600

USER_SETTINGS_FILE = "user_settings.json"

# ===================== СОСТОЯНИЕ =====================
user_settings: Dict[int, Dict[str, Any]] = {}
user_ws_tasks: Dict[int, asyncio.Task] = {}
base_volume_24h: Dict[int, Dict[str, float]] = {}
quote_volume_24h: Dict[int, Dict[str, float]] = {}
trade_count_24h: Dict[int, Dict[str, int]] = {}
active_menu_message_id: Dict[int, int] = {}

help_state: Dict[int, Dict[str, Any]] = {}
repeat_last_notified: Dict[int, Dict[str, Dict[str, Any]]] = {}

# Binance symbols (для TradingView URL подсказки)
BINANCE_SPOT_USDT = set()
BINANCE_FUT_USDTP = set()
BINANCE_SYMBOLS_READY = False
BINANCE_LAST_REFRESH: Optional[datetime.datetime] = None

# Меню — одно активное сообщение на чат
menu_locks: Dict[int, asyncio.Lock] = {}
ALWAYS_SINGLE_MENU = True

def get_menu_lock(chat_id: int) -> asyncio.Lock:
    lock = menu_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        menu_locks[chat_id] = lock
    return lock

async def safe_delete_menu_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    msg_id = active_menu_message_id.get(chat_id)
    if not msg_id:
        return
    with suppress(Exception):
        await context.bot.delete_message(chat_id, msg_id)
    active_menu_message_id.pop(chat_id, None)
    # ===================== HELP: ПОЛНЫЕ ТЕКСТЫ =====================

FULL_HELP_RU = """
📘 Полная инструкция

1. Назначение
Бот отслеживает краткосрочные импульсы цены (процентные изменения) и уведомляет двумя уровнями сигналов:
- FAST — ранний сигнал (без графика).
- CONFIRM — подтверждённый (с графиком, объёмом, экстремумом).

2. Логика порогов
Порог Fast < Confirm. FAST срабатывает, когда модуль процентного изменения (|Δ%|) достиг Fast, но ещё меньше Confirm.
CONFIRM — когда |Δ%| ≥ Confirm. Между сигналами действуют кулдауны.

3. Baseline
Базовая цена — точка отсчёта. В режиме сброса baseline обновляется после каждого Confirm. Иначе проценты копятся дольше.

4. Пары порогов
3/5 — частые сигналы, скальпинг.
5/8 — баланс.
6/10 — фильтрация шума.
8/12 — только крупные движения.
Соотношение обычно 1.5–2.2.

5. Команды
/fast X — установить Fast порог.
/confirm Y — установить Confirm порог.
/baselinereset on|off — сброс baseline.
/tf 1h — таймфрейм графика.
/reload_symbols — обновить символы Binance.
/debug_symbol BTC — информация по символу.
/status /trial /buy /grant — подписки.
/lang ru|en — язык.
/menu — меню.
/help — помощь.
/start — старт.
/repeat — настройки повторных оповещений (on|off / step / cooldown).

6. Таймфрейм графика
Не влияет на расчёт процентов. Только визуализация Confirm. Рекомендуемые:
1m/5m — быстрые импульсы, 15m/30m/1h — средний контекст, 4h/1d — макро.

7. Интерпретация сигналов
FAST — наблюдаете, готовитесь.
CONFIRM — движение подтвердилось (график + проверка экстремума).
Связка Fast→Confirm показывает качество импульса (скорость, устойчивость).

8. Кулдауны
Confirm: минимум 10с между Confirm по одной монете.
Fast (по направлению): 25с.
Минимум 2с между Fast и последующим Confirm (анти-слипание).

9. Экстремумы
Проверяется пробой локального high/low за lookback (60 свечей). Пишется: обновили максимум/минимум или остались в диапазоне.

10. Режим baseline reset
ON — после подтверждённого импульса baseline обновляется (циклы короче).
OFF — процент накапливается (можно ловить крупный продолжительный ход, но Confirm приходит позже).

11. Настройка порогов
Слишком много FAST → повышайте Fast.
Мало Confirm → понижайте Confirm или делайте пороги ближе.
Цель: Fast примерно в 3–6 раз чаще Confirm.

12. Объём
Vol24 и Trades24 помогают фильтровать ложные всплески на низкой ликвидности.

13. Типовые сценарии
Скальпинг: 3/5
Общий мониторинг: 5/8
Шум фильтрация: 6/10
Крупные движения / новости: 8/12

14. FAQ
Нет сигналов? Порог высок / рынок тих / мониторинг не запущен.
/start или кнопка запуска.
FAST есть, Confirm нет? Не дошёл до Confirm или cooldown.
Нет графика? Ошибка получения свечей / временный сбой.
Проценты «обнуляются»? baseline reset = on.

15. Рекомендации
Не торгуйте каждый FAST.
Смотрите цепочку Fast→Confirm + экстремум.
Сравнивайте % с объёмом и общим контекстом рынка.
Регулируйте пороги постепенно.

16. Безопасность
Никогда не публикуйте токен бота. При утечке — перевыпустить через @BotFather.

17. Повторы (REPEAT)
Повторное уведомление при каждом дополнительном шаге (по умолчанию 1%) в ту же сторону после последнего Fast/Confirm/Repeat. Настраивается через /repeat или в меню (шаг, кулдаун, вкл/выкл).

18. Роадмап (планы)
Тёмная тема графика, всплески объёма, breakout‑сигнал, EMA baseline, статистика по конверсии Fast→Confirm, фильтр минимального объёма, watchlist.

Удачных сигналов! 🚀
""".strip()

FULL_HELP_EN = """
📘 Full Guide

1. Purpose
Monitors short‑term price impulses (% change) with two tiers:
- FAST — early heads-up (no chart).
- CONFIRM — confirmed (chart, volume, extremes).

2. Threshold Logic
Fast < Confirm. FAST when |Δ%| ≥ Fast and < Confirm.
CONFIRM when |Δ%| ≥ Confirm. Cooldowns reduce noise.

3. Baseline
Reference price. With reset ON baseline updates after each Confirm; OFF accumulates % over longer moves.

4. Preset Pairs
3/5 frequent, 5/8 balanced, 6/10 noise filter, 8/12 large only.
Typical ratio 1.5–2.2.

5. Commands
/fast X
/confirm Y
/baselinereset on|off
/tf 1h
/reload_symbols
/debug_symbol BTC
/status /trial /buy /grant
/lang ru|en
/menu /help /start
/repeat (configure repeat alerts)

6. Timeframe
Only for visualization of Confirm (does not affect % math).

7. Signal Flow
FAST → observe / prepare.
CONFIRM → validated move (chart + extremes).
Sequence Fast→Confirm helps judge momentum quality & speed.

8. Cooldowns
Confirm: 10s per coin.
Fast (same direction): 25s.
Min 2s separation Fast→Confirm.

9. Extremes
Checks local high/low break within lookback (60 candles).

10. Baseline Reset
ON: fresh cycle each confirmed impulse.
OFF: cumulative trend; later Confirm.

11. Tuning
Too many FAST → raise Fast.
Few Confirms → lower Confirm or bring thresholds closer.
Aim Fast frequency ≈3–6× Confirm.

12. Volume Context
Vol24 & Trades24: low volume + sharp % = possible fake spike.

13. Scenarios
Scalping 3/5
General 5/8
Noise filter 6/10
News / big moves 8/12

14. FAQ
No signals? High thresholds / quiet market / not started.
/start or menu start button.
FAST without Confirm? Not reached threshold / cooldown.
No chart? Candle fetch error.
Percent “resets”? Baseline reset ON.

15. Tips
Don’t chase every FAST.
Focus on Fast→Confirm chain + extremes.
Combine with volume & structure.
Adjust gradually, gather stats.

16. Security
Keep your bot token secret. Regenerate via @BotFather if leaked.

17. Repeat Alerts
Emits extra “REPEAT” each additional configured step (default 1%) beyond last Fast/Confirm/Repeat in same direction (with cooldown & step controls).

18. Roadmap
Dark theme, volume spikes, breakout mode, EMA baseline, Fast→Confirm statistics, min volume filter, watchlist.

Good luck! 🚀
""".strip()

def split_full_text(txt: str, max_len: int = 3500) -> List[str]:
    paras = [p.strip() for p in txt.split("\n\n") if p.strip()]
    pages = []
    cur = ""
    for p in paras:
        block = p if cur == "" else cur + "\n\n" + p
        if len(block) <= max_len:
            cur = block
        else:
            if cur:
                pages.append(cur)
            if len(p) <= max_len:
                cur = p
            else:
                chunk = p
                while len(chunk) > max_len:
                    pages.append(chunk[:max_len])
                    chunk = chunk[max_len:]
                cur = chunk
    if cur:
        pages.append(cur)
    total = len(pages)
    return [f"{pg}\n\n[{i}/{total}]" for i, pg in enumerate(pages, 1)]

# Изначально — из встроенных текстов
FULL_PAGES_RU = split_full_text(FULL_HELP_RU)
FULL_PAGES_EN = split_full_text(FULL_HELP_EN)

def load_help_texts_from_files():
    global FULL_HELP_RU, FULL_HELP_EN, FULL_PAGES_RU, FULL_PAGES_EN
    try:
        ru = Path("resources/full_help_ru.md")
        if ru.exists():
            FULL_HELP_RU = ru.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning(f"[HELP RU] load error: {e}")
    try:
        en = Path("resources/full_help_en.md")
        if en.exists():
            FULL_HELP_EN = en.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning(f"[HELP EN] load error: {e}")
    FULL_PAGES_RU = split_full_text(FULL_HELP_RU)
    FULL_PAGES_EN = split_full_text(FULL_HELP_EN)

def get_help_pages(lang: str, mode: str) -> List[str]:
    if mode == 'full':
        return FULL_PAGES_RU if lang == 'ru' else FULL_PAGES_EN
    # Краткая помощь (6 страниц)
    HELP_PAGES_RU_SHORT: List[str] = [
        "Стр 1/6\nОбзор:\nБот отслеживает импульсы (%), шлёт FAST и CONFIRM (с графиком).\nКоманда /help для навигации.",
        "Стр 2/6\nПороги:\nFAST: |Δ%| ≥ Fast < Confirm.\nCONFIRM: |Δ%| ≥ Confirm.\nРаботают кулдауны.",
        "Стр 3/6\nBaseline:\nReset ON — обновляется после Confirm.\nOFF — проценты копятся.\n/baselinereset on|off.",
        "Стр 4/6\nКоманды:\n/fast X /confirm Y /tf 1h\n/reload_symbols /debug_symbol BTC\n/status /trial /buy /grant\n/menu /help /lang /repeat",
        "Стр 5/6\nПары:\n3/5 частые\n5/8 баланс\n6/10 фильтр шума\n8/12 крупные.",
        "Стр 6/6\nСоветы:\nFast→Confirm цепочка важна.\nСмотрите объём.\nЯзык: /lang ru|en."
    ]
    HELP_PAGES_EN_SHORT: List[str] = [
        "Pg 1/6\nOverview:\nBot watches % impulses.\nFAST and CONFIRM (chart).\nUse /help to navigate.",
        "Pg 2/6\nThresholds:\nFAST: |Δ%| ≥ Fast < Confirm.\nCONFIRM: |Δ%| ≥ Confirm.\nCooldowns active.",
        "Pg 3/6\nBaseline:\nReset ON — updates after Confirm.\nOFF — accumulates.\n/baselinereset on|off.",
        "Pg 4/6\nCommands:\n/fast X /confirm Y /tf 1h\n/reload_symbols /debug_symbol BTC\n/status /trial /buy /grant\n/menu /help /lang /repeat",
        "Pg 5/6\nPairs:\n3/5 frequent\n5/8 balanced\n6/10 noise filter\n8/12 large only.",
        "Pg 6/6\nTips:\nFast→Confirm chain matters.\nUse volume.\n/lang ru|en."
    ]
    return HELP_PAGES_RU_SHORT if lang == 'ru' else HELP_PAGES_EN_SHORT

# ===================== ПОДПИСКИ =====================
def load_subscriptions():
    if not SUBSCRIPTIONS_ENABLED:
        return
    global SUBSCRIPTIONS
    p = Path(SUBSCRIPTIONS_FILE)
    if p.exists():
        try:
            SUBSCRIPTIONS = json.load(p.open("r", encoding="utf-8"))
        except Exception as e:
            logger.error(f"[SUBS] load error: {e}")
            SUBSCRIPTIONS = {}

def save_subscriptions():
    if not SUBSCRIPTIONS_ENABLED:
        return
    p = Path(SUBSCRIPTIONS_FILE)
    try:
        json.dump(SUBSCRIPTIONS, p.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"[SUBS] save error: {e}")

def get_user_sub(chat_id: int):
    if not SUBSCRIPTIONS_ENABLED:
        return {"trial_used": False, "expiry_ts": 9999999999}
    return SUBSCRIPTIONS.get(str(chat_id), {"trial_used": False, "expiry_ts": 0})

def has_active_access(chat_id: int) -> bool:
    rec = get_user_sub(chat_id)
    return datetime.datetime.utcnow().timestamp() <= rec.get("expiry_ts", 0)

def formatted_expiry(chat_id: int) -> str:
    rec = get_user_sub(chat_id)
    ts = rec.get("expiry_ts", 0)
    if ts <= 0:
        return "нет"
    dt = datetime.datetime.utcfromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M UTC")

# ===================== PERSISTENCE USER SETTINGS =====================
def default_user_settings() -> Dict[str, Any]:
    return {
        'confirm_threshold': CONFIRM_THRESHOLD_DEFAULT,
        'fast_threshold': FAST_THRESHOLD_DEFAULT,
        'interval': CHECK_INTERVAL_DEFAULT,
        'exchange': 'binance',
        'market_type': 'spot',
        'menu_expanded': False,
        'menu_view': 'main',
        'manual_threshold_error': None,
        'tf': TIMEFRAME_DEFAULT,
        'baseline_reset': True,
        'lang': 'ru',
        'repeat_enabled': True,
        'repeat_step': REPEAT_STEP_DEFAULT,
        'repeat_cooldown': REPEAT_COOLDOWN_DEFAULT,
        # настройки OB‑сканера (по умолчанию выключен)
        'obscan': {
            'enabled': False,
            'tfs': ['1m', '5m', '10m', '15m'],
            'mode': 'any',
            'trend_lookback': 5,
            'pre_window': 10,
            'min_body_ratio': 0.2,
            'cooldown_min': 180,
            'min_vol_usdt': 1_000_000.0,
            'max_symbols': 200,
            'sleep_ms': 40,
            'interval_sec': 20,
            'left': 3,
            'right': 3,
        },
        # НАСТРОЙКИ ТРЕЙДИНГА BYBIT (Futures)
    'bybit_trade': {
        'usdt_amount': 100,      # Сумма позиции в USDT
        'leverage': 5,           # Кредитное плечо
        'order_type': 'Market',  # Тип ордера
        'tp': None,              # Take Profit
        'sl': None               # Stop Loss
        }
        
    }   

def load_user_settings():
    p = Path(USER_SETTINGS_FILE)
    if not p.exists():
        return
    try:
        data = json.load(p.open("r", encoding="utf-8"))
        if not isinstance(data, dict):
            return
        for k, v in data.items():
            try:
                chat_id = int(k)
            except:
                continue
            base = default_user_settings()
            if isinstance(v, dict):
                base.update({kk: vv for kk, vv in v.items() if kk in base})
            user_settings[chat_id] = base
            base_volume_24h.setdefault(chat_id, {})
            quote_volume_24h.setdefault(chat_id, {})
            trade_count_24h.setdefault(chat_id, {})
            repeat_last_notified.setdefault(chat_id, {})
    except Exception as e:
        logger.error(f"[USER_SETTINGS] load error: {e}")

def save_user_settings():
    p = Path(USER_SETTINGS_FILE)
    serializable: Dict[str, Dict[str, Any]] = {}
    for chat_id, cfg in user_settings.items():
        c = dict(cfg)
        c.pop('manual_threshold_error', None)
        serializable[str(chat_id)] = c
    try:
        json.dump(serializable, p.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"[USER_SETTINGS] save error: {e}")

def settings_changed(chat_id: int):
    save_user_settings()
    
    # ===================== УТИЛИТЫ =====================

def esc(text: str) -> str:
    return escape_markdown(str(text), version=2)

def ensure_user(chat_id: int):
    if chat_id not in user_settings:
        user_settings[chat_id] = default_user_settings()
    base_volume_24h.setdefault(chat_id, {})
    quote_volume_24h.setdefault(chat_id, {})
    trade_count_24h.setdefault(chat_id, {})
    repeat_last_notified.setdefault(chat_id, {})

def get_signals_stats():
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result', 'sl', 'tp'
        ]
        total = len(df)
        profit = len(df[df['result'] > 0])
        loss = len(df[df['result'] < 0])
        return f"Всего сигналов: {total} | Плюсовых: {profit} | Минусовых: {loss}"
    except Exception as e:
        return "Нет статистики"

def get_last_signals(n=3):
    try:
        df = pd.read_csv('signals_log.csv', header=None)
        df.columns = [
            'datetime', 'coin', 'price', 'direction',
            'pattern', 'rsi', 'ema', 'volume', 'result', 'sl', 'tp'
        ]
        last = df.tail(n)
        lines = []
        for idx, row in last.iterrows():
            dir_text = "🟢" if row['direction'] == "up" else "🔴"
            lines.append(
                f"{row['datetime'][-8:]} {row['coin']} {dir_text} {row['pattern']} "
                f"SL:{row['sl']} TP:{row['tp']} V:{row['volume']} | Рез: {row['result']}"
            )
        return "\n".join(lines)
    except Exception as e:
        return "Нет данных по сигналам"

def format_status_text(chat_id: int) -> str:
    s = user_settings[chat_id]
    lang = s.get('lang', 'ru')

    confirm_thr = s['confirm_threshold']
    fast_thr = s['fast_threshold']
    ex = s['exchange']
    mt = s['market_type']
    tf = s['tf']

    baseline_mode = ("сброс" if s.get('baseline_reset') else "фикс.") if lang=='ru' else ("reset" if s.get('baseline_reset') else "fixed")
    monitoring_on = chat_id in user_ws_tasks
    monitoring_line = f"Мониторинг: {'🟢 Включен' if monitoring_on else '❌ Выключен'}" if lang=='ru' else f"Monitoring: {'🟢 On' if monitoring_on else '❌ Off'}"

    def icon(selected, current): return "🟢" if selected == current else "⚪️"
    exchange_lines = [f"{icon(k, ex)} {v}" for k, v in EXCHANGE_CHOICES.items()]
    market_lines_ru = {'spot': 'Спот', 'futures': 'Фьючерс'}
    market_lines_en = {'spot': 'Spot', 'futures': 'Futures'}
    market_map = market_lines_ru if lang == 'ru' else market_lines_en
    market_lines = [f"{icon(k, mt)} {market_map[k]}" for k in MARKET_TYPE_CHOICES.keys()]

    if SUBSCRIPTIONS_ENABLED:
        sub_line = f"Подписка до: {formatted_expiry(chat_id)}" if lang=='ru' else f"Subscription until: {formatted_expiry(chat_id)}"
    else:
        sub_line = "Подписка: не требуется" if lang=='ru' else "Subscription: not required"

    repeat_line = (f"Повторы: {'on' if s['repeat_enabled'] else 'off'} | шаг {s['repeat_step']}% | cd {s['repeat_cooldown']}s"
                   if lang=='ru' else
                   f"Repeats: {'on' if s['repeat_enabled'] else 'off'} | step {s['repeat_step']}% | cd {s['repeat_cooldown']}s")

    header = "Статус мониторинга" if lang == 'ru' else "Monitoring status"

    return (
        f"{header}\n"
        f"{'Биржа' if lang=='ru' else 'Exchange'}:\n" + "\n".join(exchange_lines) + "\n"
        f"{'Тип рынка' if lang=='ru' else 'Market type'}:\n" + "\n".join(market_lines) + "\n"
        f"{'Пороги' if lang=='ru' else 'Thresholds'}: Fast {fast_thr}% | Confirm {confirm_thr}% | TF: {tf}\n"
        f"{'Базовая цена' if lang=='ru' else 'Baseline'}: {baseline_mode}\n"
        f"{repeat_line}\n"
        f"{monitoring_line}\n"
        f"{sub_line}\n"
        f"{'Язык' if lang=='ru' else 'Language'}: {s.get('lang','ru')}\n"
        f"\n{get_signals_stats()}\n"
        f"Последние сигналы:\n{get_last_signals(3)}"
    )

# ===================== КЛАВИАТУРЫ =====================

def _nearest_presets(current: float, presets: List[float], max_show: int) -> List[float]:
    diffs = sorted(((abs(p - current), p) for p in presets if p != current), key=lambda x: x[0])
    return [p for _, p in diffs[:max_show]]

REPEAT_STEP_PRESETS = [0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]
REPEAT_COOLDOWN_PRESETS = [3, 5, 10, 15, 30, 60, 120, 300]

def build_repeat_settings_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    s = user_settings[chat_id]
    lang = s.get('lang', 'ru')
    step = float(s['repeat_step'])
    cd = int(s['repeat_cooldown'])
    step_disp = f"{step:.2f}".rstrip('0').rstrip('.')

    step_presets = _nearest_presets(step, REPEAT_STEP_PRESETS, 3)
    cd_presets = _nearest_presets(cd, REPEAT_COOLDOWN_PRESETS, 4)

    step_row1 = [
        InlineKeyboardButton("-0.5", callback_data="repeat_step_adj:-0.5"),
        InlineKeyboardButton("-0.1", callback_data="repeat_step_adj:-0.1"),
        InlineKeyboardButton("+0.1", callback_data="repeat_step_adj:+0.1"),
        InlineKeyboardButton("+0.5", callback_data="repeat_step_adj:+0.5"),
    ]
    preset_row = [InlineKeyboardButton(f"{p:.2f}".rstrip('0').rstrip('.'), callback_data=f"repeat_step_set:{p}") for p in step_presets]

    cd_row1 = [
        InlineKeyboardButton("-5", callback_data="repeat_cd_adj:-5"),
        InlineKeyboardButton("-1", callback_data="repeat_cd_adj:-1"),
        InlineKeyboardButton("+1", callback_data="repeat_cd_adj:+1"),
        InlineKeyboardButton("+5", callback_data="repeat_cd_adj:+5"),
    ]
    cd_preset_row = [InlineKeyboardButton(f"{p}s", callback_data=f"repeat_cd_set:{p}") for p in cd_presets]

    toggle_label = ("🔂 Повторы: ON" if s['repeat_enabled'] else "🔂 Повторы: OFF") if lang=='ru' else ("🔂 Repeats: ON" if s['repeat_enabled'] else "🔂 Repeats: OFF")
    back_label = "⬅️ Назад" if lang=='ru' else "⬅️ Back"

    title_step = f"Шаг: {step_disp}%" if lang=='ru' else f"Step: {step_disp}%"
    title_cd = f"Кулдаун: {cd}s" if lang=='ru' else f"Cooldown: {cd}s"

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(title_step, callback_data="noop")])
    rows.append(step_row1)
    if preset_row:
        rows.append(preset_row)
    rows.append([InlineKeyboardButton(title_cd, callback_data="noop")])
    rows.append(cd_row1)
    if cd_preset_row:
        rows.append(cd_preset_row)
    rows.append([InlineKeyboardButton(toggle_label, callback_data="toggle_repeat")])
    rows.append([InlineKeyboardButton(back_label, callback_data="back_main")])
    return InlineKeyboardMarkup(rows)

def strategy_circle(enabled):
    return '🟢' if enabled else '🔴'

def build_bybit_trade_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    s = user_settings[chat_id].get('bybit_trade', {})
    usdt_amount = s.get('usdt_amount', 100)
    leverage = s.get('leverage', 5)
    order_type = s.get('order_type', 'Market')
    tp = s.get('tp', None)
    sl = s.get('sl', None)
    rows = [
        [InlineKeyboardButton(f"Сумма (USDT): {usdt_amount}", callback_data="bybit_usdt_amount")],
        [InlineKeyboardButton(f"Кредитное плечо: {leverage}x", callback_data="bybit_leverage")],
        [InlineKeyboardButton(f"Тип ордера: {order_type}", callback_data="bybit_order_type")],
        [InlineKeyboardButton(f"TP: {tp if tp is not None else '—'}", callback_data="bybit_tp"),
         InlineKeyboardButton(f"SL: {sl if sl is not None else '—'}", callback_data="bybit_sl")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(rows)

def build_keyboard(chat_id: int):
    s = user_settings[chat_id]
    lang = s.get('lang', 'ru')
    expanded = s['menu_expanded']
    view = s['menu_view']

    # --- Добавляем стратегию состояния ---
    strategies = s.get('strategies', {})
    choch_enabled = strategies.get('choch', True)
    bos_enabled = strategies.get('bos', True)
    rsi_enabled = strategies.get('rsi', True)
    pump_enabled = strategies.get('pump', True)

    if not expanded:
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔽 Открыть меню" if lang=='ru' else "🔽 Open menu", callback_data='toggle_menu')]])
    if view == 'main':
        repeat_toggle_label = ("🔂 Повторы: ON" if s['repeat_enabled'] else "🔂 Повторы: OFF") if lang=='ru' else ("🔂 Repeats: ON" if s['repeat_enabled'] else "🔂 Repeats: OFF")
        repeat_settings_label = "⚙️ Повторы" if lang=='ru' else "⚙️ Repeats"
        strategy_row = [
            InlineKeyboardButton(f"CHoCH+Volume {strategy_circle(choch_enabled)}", callback_data='toggle_choch'),
            InlineKeyboardButton(f"BOS+Zone {strategy_circle(bos_enabled)}", callback_data='toggle_bos'),
            InlineKeyboardButton(f"RSI Divergence {strategy_circle(rsi_enabled)}", callback_data='toggle_rsi'),
            InlineKeyboardButton(f"Pump/Dump {strategy_circle(pump_enabled)}", callback_data='toggle_pump')
        ]
        return InlineKeyboardMarkup([
            strategy_row,
            [InlineKeyboardButton("▶️", callback_data='start_monitorинг'),
             InlineKeyboardButton("⏹", callback_data='stop_monitorинг'),
             InlineKeyboardButton("Help", callback_data='open_help')],
            [InlineKeyboardButton("Fast/Confirm", callback_data='view_threshold'),
             InlineKeyboardButton("✏️", callback_data='view_manual_threshold'),
             InlineKeyboardButton("TF", callback_data='view_tf')],
            [InlineKeyboardButton("Биржа" if lang=='ru' else "Exch", callback_data='view_exchange'),
             InlineKeyboardButton("Рынок" if lang=='ru' else "Mkt", callback_data='view_market'),
             InlineKeyboardButton("Baseline", callback_data='toggle_baseline')],
            [InlineKeyboardButton(repeat_toggle_label, callback_data='toggle_repeat'),
             InlineKeyboardButton(repeat_settings_label, callback_data='view_repeat_settings')],
            [InlineKeyboardButton("Настройки Bybit трейда", callback_data='bybit_trade_settings')],
            [InlineKeyboardButton("Язык" if lang=='ru' else "Lang", callback_data='view_lang'),
             InlineKeyboardButton("🔼", callback_data='toggle_menu')],
            [InlineKeyboardButton("🔄 Обновить статистику", callback_data='update_stats')],
        ])
    if view == 'bybit_trade_settings':
        return build_bybit_trade_keyboard(chat_id)
    if view == 'threshold_select':
        back = "⬅️"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("3 / 5", callback_data='set_fast_confirm:3:5'),
             InlineKeyboardButton("4 / 6", callback_data='set_fast_confirm:4:6'),
             InlineKeyboardButton("5 / 8", callback_data='set_fast_confirm:5:8')],
            [InlineKeyboardButton("6 / 10", callback_data='set_fast_confirm:6:10'),
             InlineKeyboardButton("8 / 12", callback_data='set_fast_confirm:8:12'),
             InlineKeyboardButton(back, callback_data='back_main')]
        ])
    if view == 'manual_threshold':
        back = "⬅️"
        return InlineKeyboardMarkup([[InlineKeyboardButton(back, callback_data='back_main')]])
    if view == 'exchange_select':
        back = "⬅️"
        kb = [[InlineKeyboardButton(name, callback_data=f"exchange_{k}")] for k, name in EXCHANGE_CHOICES.items()]
        kb.append([InlineKeyboardButton(back, callback_data='back_main')])
        return InlineKeyboardMarkup(kb)
    if view == 'market_select':
        back = "⬅️"
        m_map_ru = {'spot': 'Спот', 'futures': 'Фьючерс'}
        m_map_en = {'spot': 'Spot', 'futures': 'Futures'}
        mm = m_map_ru if lang=='ru' else m_map_en
        kb = [[InlineKeyboardButton(mm[k], callback_data=f"market_{k}")] for k in MARKET_TYPE_CHOICES.keys()]
        kb.append([InlineKeyboardButton(back, callback_data='back_main')])
        return InlineKeyboardMarkup(kb)
    if view == 'tf_select':
        back = "⬅️"
        rows, row = [], []
        for i, tf in enumerate(ALLOWED_TF, start=1):
            row.append(InlineKeyboardButton(tf, callback_data=f"tf_{tf}"))
            if i % 6 == 0:
                rows.append(row); row = []
        if row: rows.append(row)
        rows.append([InlineKeyboardButton(back, callback_data='back_main')])
        return InlineKeyboardMarkup(rows)
    if view == 'lang_select':
        back = "⬅️"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("Русский", callback_data='lang_ru'),
             InlineKeyboardButton("English", callback_data='lang_en')],
            [InlineKeyboardButton(back, callback_data='back_main')]
        ])
    if view == 'repeat_settings':
        return build_repeat_settings_keyboard(chat_id)

    # ====== ИСПРАВЛЕННЫЙ ХВОСТ ФУНКЦИИ ======
    # Если ни один view не подходит — возвращаем минимальное меню без рекурсии!
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔽 Открыть меню" if lang=='ru' else "🔽 Open menu", callback_data='toggle_menu')]])

def build_menu_text(chat_id: int) -> str:
    s = user_settings[chat_id]
    lang = s.get('lang', 'ru')
    view = s['menu_view']
    base = format_status_text(chat_id)
    if view == 'main':
        return base
    mapping_ru = {
        'threshold_select': "Предустановки Fast / Confirm:",
        'manual_threshold': "Введите два числа 'fast confirm' (пример: 3 5)",
        'exchange_select': "Выберите биржу:",
        'market_select': "Выберите тип рынка:",
        'tf_select': "Выберите таймфрейм:",
        'lang_select': "Выбор языка:",
        'repeat_settings': "Настройки повторов:"
    }
    mapping_en = {
        'threshold_select': "Fast / Confirm presets:",
        'manual_threshold': "Send two numbers 'fast confirm' (e.g. 3 5)",
        'exchange_select': "Select exchange:",
        'market_select': "Select market:",
        'tf_select': "Select timeframe:",
        'lang_select': "Language selection:",
        'repeat_settings': "Repeat settings:"
    }
    mapping = mapping_ru if lang=='ru' else mapping_en
    if view == 'manual_threshold' and s.get('manual_threshold_error'):
        return mapping[view] + ("\n⚠️ " + s['manual_threshold_error'])
    return mapping.get(view, base)

async def send_or_edit_menu(context: ContextTypes.DEFAULT_TYPE, chat_id: int, force_new: bool = False):
    ensure_user(chat_id)
    lock = get_menu_lock(chat_id)
    async with lock:
        text = md2_safe(build_menu_text(chat_id))
        kb = build_keyboard(chat_id)
        prev_id = active_menu_message_id.get(chat_id)
        if force_new:
            if prev_id:
                await safe_delete_menu_message(context, chat_id)
            try:
                m = await context.bot.send_message(chat_id, text, reply_markup=kb, parse_mode='MarkdownV2')
                active_menu_message_id[chat_id] = m.message_id
            except Exception as e:
                logger.error(f"[MENU force_new] {e}")
            return
        if not prev_id:
            try:
                m = await context.bot.send_message(chat_id, text, reply_markup=kb, parse_mode='MarkdownV2')
                active_menu_message_id[chat_id] = m.message_id
            except Exception as e:
                logger.error(f"[MENU send] {e}")
            return
        if ALWAYS_SINGLE_MENU:
            try:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=prev_id,
                                                    text=text, reply_markup=kb, parse_mode='MarkdownV2')
                return
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    return
                logger.warning(f"[MENU edit->recreate] {e}")
            except Exception as e:
                logger.warning(f"[MENU edit exc -> recreate] {e}")
        await safe_delete_menu_message(context, chat_id)
        try:
            m = await context.bot.send_message(chat_id, text, reply_markup=kb, parse_mode='MarkdownV2')
            active_menu_message_id[chat_id] = m.message_id
        except Exception as e:
            logger.error(f"[MENU recreate send] {e}")

async def reset_to_main_menu(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    user_settings[chat_id]['menu_view'] = 'main'
    user_settings[chat_id]['manual_threshold_error'] = None
    await send_or_edit_menu(context, chat_id)
    
    # ===================== BINANCE SYMBOLS =====================
async def load_binance_symbol_sets(force=False):
    global BINANCE_SYMBOLS_READY, BINANCE_LAST_REFRESH
    if BINANCE_SYMBOLS_READY and not force:
        return
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            spot_resp = await client.get(BINANCE_SPOT_INFO_URL)
            spot_resp.raise_for_status()
            spot_data = spot_resp.json()
            BINANCE_SPOT_USDT.clear()
            for s in spot_data.get("symbols", []):
                if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
                    base = s.get("baseAsset")
                    if base: BINANCE_SPOT_USDT.add(base.upper())
            fut_resp = await client.get(BINANCE_FUT_INFO_URL)
            fut_resp.raise_for_status()
            fut_data = fut_resp.json()
            BINANCE_FUT_USDTP.clear()
            for s in fut_data.get("symbols", []):
                if s.get("quoteAsset") == "USDT":
                    base = s.get("baseAsset")
                    if base: BINANCE_FUT_USDTP.add(base.upper())
        BINANCE_SYMBOLS_READY = True
        BINANCE_LAST_REFRESH = datetime.datetime.utcnow()
        logger.info(f"[BINANCE] Loaded spot={len(BINANCE_SPOT_USDT)} futures={len(BINANCE_FUT_USDTP)}")
    except Exception as e:
        logger.error(f"Failed to load Binance symbols: {e}")

def build_tradingview_url(exchange: str, market_type: str, coin: str) -> Tuple[str, str]:
    coin_u = coin.upper()
    note = ""
    # Для Binance Futures TradingView требует суффикс .P для perpetual контрактов!
    if exchange == 'binance':
        if market_type == 'futures':
            tv_symbol = f"BINANCE:{coin_u}USDT.P"
        else:
            tv_symbol = f"BINANCE:{coin_u}USDT"
    elif exchange == 'bybit':
        tv_symbol = f"BYBIT:{coin_u}USDT" if market_type == 'spot' else f"BYBIT:{coin_u}USDT.P"
    else:
        tv_symbol = f"{coin_u}USDT"
    if TV_LAYOUT_ID:
        url = f"https://www.tradingview.com/chart/{TV_LAYOUT_ID}/?symbol={urllib.parse.quote(tv_symbol)}"
    else:
        url = f"https://www.tradingview.com/chart/?symbol={urllib.parse.quote(tv_symbol)}"
    return tv_symbol, url

# ===================== BYBIT HELPERS =====================
async def get_bybit_symbols(category: str):
    url = BYBIT_SPOT_TICKERS_URL if category == "spot" else BYBIT_FUTURES_TICKERS_URL
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    symbols = [t.get("symbol") for t in data.get("result", {}).get("list", []) if (t.get("symbol","").endswith("USDT"))]
    symbols.sort()
    return symbols

async def get_bybit_price_snapshot(category: str):
    url = BYBIT_SPOT_TICKERS_URL if category == "spot" else BYBIT_FUTURES_TICKERS_URL
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    now = datetime.datetime.now(datetime.timezone.utc)
    base = {}
    for t in data.get("result", {}).get("list", []):
        sym = t.get("symbol")
        if not sym or not sym.endswith('USDT'):
            continue
        price_str = t.get('lastPrice') or t.get('bid1Price') or t.get('ask1Price') or t.get('close')
        if not price_str: continue
        try:
            price = float(price_str)
        except:
            continue
        base[sym[:-4]] = (now, price)
    return base
    
# ===================== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ТЕКСТОВОГО ВВОДА =====================
async def universal_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]
    menu = s.get('menu_view', 'main')

    # --- Bybit trade params ---
    if menu in ['bybit_usdt_amount', 'bybit_leverage', 'bybit_tp', 'bybit_sl']:
        await bybit_trade_param_input(update, context)
        return

    # --- Ручной ввод порогов ---
    if menu == 'manual_threshold':
        await manual_threshold_input(update, context)
        return

    # --- Добавь сюда обработчики других состояний, если появятся ---
    # Например, если появятся другие меню для ручного ввода

    # Если ни одно из условий не выполнено — можно проигнорировать или отправить сообщение:
    # await update.message.reply_text("Неожиданный ввод. Откройте нужное меню.")    

# ===================== ФОРМАТИРОВАНИЕ =====================
def compact_number(x):
    try: x = float(x)
    except: return "-"
    if x == 0: return "0"
    for v, sfx in [(1e12,"T"), (1e9,"B"), (1e6,"M"), (1e3,"K")]:
        if abs(x) >= v:
            return f"{x/v:.2f}{sfx}"
    return f"{x:.2f}"

def format_price(p):
    try: p = float(p)
    except: return str(p)
    if p == 0: return "0"
    if p >= 100: fmt = f"{p:.2f}"
    elif p >= 10: fmt = f"{p:.3f}"
    elif p >= 1: fmt = f"{p:.4f}"
    elif p >= 0.1: fmt = f"{p:.5f}"
    elif p >= 0.01: fmt = f"{p:.6f}"
    elif p >= 0.001: fmt = f"{p:.7f}"
    elif p >= 0.0001: fmt = f"{p:.8f}"
    elif p >= 0.00001: fmt = f"{p:.9f}"
    elif p >= 0.000001: fmt = f"{p:.10f}"
    elif p >= 0.00000001: fmt = f"{p:.12f}"
    else: fmt = f"{p:.14f}"
    fmt = fmt.rstrip('0').rstrip('.')
    return fmt or "0"

def human_trades(trades):
    if trades is None: return "—"
    try: t = int(trades)
    except: return str(trades)
    if t >= 1_000_000: return f"{t/1_000_000:.2f}M"
    if t >= 1_000: return f"{t/1_000:.2f}K"
    return str(t)

# ===================== УВЕДОМЛЕНИЯ =====================
import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

async def send_fast_notification(context, chat_id, coin, change, price, exchange, market_type, tv_url):
    s = user_settings.get(chat_id, {})
    coin_u = coin.upper()
    pair = f"{coin_u}/USDT"
    time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = "🟢" if change > 0 else "🔴"
    arrow = "↑" if change > 0 else "↓"
    exchange_disp = exchange.upper()
    market_disp = "Фьючерсы" if market_type == 'futures' else "Спот"
    
    # Получаем необходимые параметры для AI
    rsi = 50           # Здесь нужно получить актуальное значение RSI
    ema = 200          # Здесь нужно получить актуальное значение EMA
    volume = 0         # Здесь нужно получить актуальное значение объёма
    target_price = price
    direction = 'up' if change > 0 else 'down'
    strategy = 'FAST'
    symbol = coin_u
    timestamp = time_now

    try:
        ai_result = predict_signal(
            price, volume, rsi, ema, direction, strategy, symbol, timestamp, tp=target_price
        )
    except Exception as e:
        ai_result = "AI ошибка"
    
    text_msg = (
        f"{color} <b>FAST сигнал</b> {pair} {arrow} <b>{change:+.2f}%</b>\n"
        f"<b>AI прогноз:</b> {ai_result}\n"
        f"Биржа: <b>{exchange_disp}</b> | Тип: <b>{market_disp}</b> | TF: <b>{s.get('tf', '1h')}</b>\n"
        f"Время: <b>{time_now}</b>\n"
        f"Цена: <b>{price}</b>\n"
        f"<a href='{tv_url}'>TradingView</a>"
    )
    try:
        await context.bot.send_message(chat_id, text_msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[FAST SEND FAIL] {e}")

async def send_repeat_notification(context, chat_id, coin, change, price, exchange, market_type, tv_url):
    s = user_settings.get(chat_id, {})
    coin_u = coin.upper()
    pair = f"{coin_u}/USDT"
    color = "🔴" if change < 0 else "🟢"
    arrow = "↓" if change < 0 else "↑"
    exchange_disp = exchange.upper()
    market_disp = "Фьючерсы" if market_type == 'futures' else "Спот"
    vol24_disp = compact_number(0)
    trades_disp = human_trades(0)
    price_disp = format_price(price)
    tf = s.get('tf', '1h')
    lang = s.get('lang', 'ru')
    if lang == 'ru':
        text_raw = f"{color} ПОВТОР {pair} {arrow} {change:+.2f}%\nЦена: {price_disp}\nVol24: {vol24_disp} | Trades24: {trades_disp}"
    else:
        text_raw = f"{color} REPEAT {pair} {arrow} {change:+.2f}%\nPrice: {price_disp}\nVol24: {vol24_disp} | Trades24: {trades_disp}"
    try:
        await context.bot.send_message(
            chat_id,
            text_raw,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("TradingView", url=tv_url)]])
        )
    except Exception as e:
        logger.error(f"[REPEAT SEND FAIL] {e}")

async def send_confirm_notification(
    context,
    chat_id,
    coin,
    change,
    price,
    exchange,
    market_type,
    tv_url,
    pair_slash="",
    event_tag="",
    market_label="",
    tf="1h",
    new_extreme_text="",
    vol24_disp="",
    trades_disp="",
    price_disp="",
    circle="🟢"
):
    # Здесь тоже получи или передай настоящие rsi, ema, volume!
    rsi = 50
    ema = 200
    volume = 0
    target_price = price
    direction = 'up' if change > 0 else 'down'
    strategy = 'CONFIRM'
    symbol = coin
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        ai_result = predict_signal(
            price, volume, rsi, ema, direction, strategy, symbol, timestamp, tp=target_price
        )
    except Exception as e:
        ai_result = "AI ошибка"

    text_msg = (
        f"{circle} <b>CONFIRM сигнал</b> {pair_slash} {event_tag} <b>{change:+.2f}%</b>\n"
        f"AI прогноз: <b>{ai_result}</b>\n"
        f"Биржа: <b>{exchange.upper()}</b> | Тип: <b>{market_label}</b> | TF: <b>{tf}</b>\n"
        f"Время: <b>{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
        f"Цена: <b>{price_disp}</b>\n"
        f"Vol24: <b>{vol24_disp}</b> | Сделок: <b>{trades_disp}</b>\n"
        f"Экстремум: <b>{new_extreme_text}</b>\n"
        f"<a href='{tv_url}'>TradingView</a>"
    )
    try:
        await context.bot.send_message(chat_id, text_msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[CONFIRM SEND FAIL] {e}")
  # CONFIRM СИГНАЛ


# ===================== WEBSOCKET =====================
def make_handler(exchange: str, market: str):
    is_binance = exchange == 'binance'
    ws_url = (
        ("wss://stream.binance.com:9443/ws/!ticker@arr" if market == 'spot'
         else "wss://fstream.binance.com/ws/!ticker@arr")
        if is_binance else BYBIT_WS_URLS[market]
    )
    category = 'spot' if market == 'spot' else 'futures'
    async def handler(chat_id: int, _a, _b, context: ContextTypes.DEFAULT_TYPE):
        price_base_local = {}
        last_fast_time: Dict[str, datetime.datetime] = {}
        last_confirm_time: Dict[str, datetime.datetime] = {}
        last_fast_direction: Dict[str, int] = {}
        bvol = base_volume_24h[chat_id]
        qvol = quote_volume_24h[chat_id]
        tcnt = trade_count_24h[chat_id]
        if is_binance:
            await load_binance_symbol_sets()
        else:
            try:
                snap = await get_bybit_price_snapshot(category)
                price_base_local.update(snap)
            except Exception as e:
                logger.error(f"[{exchange}] snapshot error: {e}")
        while True:
            try:
                symbols = None
                if not is_binance:
                    symbols = await get_bybit_symbols(category)
                async with websockets.connect(
                    ws_url,
                    ping_interval=BYBIT_PING_INTERVAL if not is_binance else BINANCE_PING_INTERVAL,
                    ping_timeout=BYBIT_PING_INTERVAL if not is_binance else BINANCE_PING_INTERVAL
                ) as ws:
                    if not is_binance:
                        args_all = [f"tickers.{s}" for s in symbols]
                        for i in range(0, len(args_all), 10):
                            await ws.send(json.dumps({"op": "subscribe", "args": args_all[i:i+10]}))
                            await asyncio.sleep(0.05)
                    while True:
                        raw = await ws.recv()
                        try:
                            data = json.loads(raw)
                        except:
                            continue
                        now_i = datetime.datetime.now(datetime.timezone.utc)
                        if not has_active_access(chat_id):
                            continue
                        user_cfg = user_settings.get(chat_id)
                        if not user_cfg:
                            continue
                        fast_thr = float(user_cfg['fast_threshold'])
                        confirm_thr = float(user_cfg['confirm_threshold'])
                        baseline_reset = user_cfg.get('baseline_reset', True)
                        tf = user_cfg.get('tf', TIMEFRAME_DEFAULT)
                        market_type = user_cfg['market_type']
                        if is_binance:
                            if isinstance(data, list):
                                for t in data:
                                    sym = t.get('s')
                                    if not sym or not sym.endswith('USDT'):
                                        continue
                                    coin = sym[:-4]
                                    try:
                                        price = float(t.get('c', 0))
                                    except:
                                        continue
                                    try: bvol[coin] = float(t.get('v', 0))
                                    except: pass
                                    try: qvol[coin] = float(t.get('q', 0))
                                    except: pass
                                    try: tcnt[coin] = int(t.get('n', 0))
                                    except: pass
                                    price_base_local.setdefault(coin, (now_i, price))
                                    _, base_price = price_base_local[coin]
                                    if base_price == 0: continue
                                    change = (price - base_price) / base_price * 100
                                    await process_tick_signal(
                                        context, chat_id, coin, price, change, now_i,
                                        exchange, market_type, fast_thr, confirm_thr,
                                        last_fast_time, last_confirm_time, last_fast_direction,
                                        baseline_reset, tf, bvol.get(coin), qvol.get(coin), tcnt.get(coin),
                                        price_base_local
                                    )
                        else:
                            if 'topic' in data and data['topic'].startswith('tickers') and 'data' in data:
                                items = data['data']
                                if not isinstance(items, list):
                                    items = [items]
                                for t in items:
                                    sym = t.get('symbol')
                                    if not sym or not sym.endswith('USDT'):
                                        continue
                                    price_str = (t.get('lastPrice') or t.get('bid1Price')
                                                 or t.get('ask1Price') or t.get('close'))
                                    if not price_str: continue
                                    try:
                                        price = float(price_str)
                                    except:
                                        continue
                                    coin = sym[:-4]
                                    try: bvol[coin] = float(t.get('volume24h', 0))
                                    except: pass
                                    try: qvol[coin] = float(t.get('turnover24h', 0))
                                    except: pass
                                    price_base_local.setdefault(coin, (now_i, price))
                                    _, base_price = price_base_local[coin]
                                    if base_price == 0: continue
                                    change = (price - base_price) / base_price * 100
                                    await process_tick_signal(
                                        context, chat_id, coin, price, change, now_i,
                                        exchange, market_type, fast_thr, confirm_thr,
                                        last_fast_time, last_confirm_time, last_fast_direction,
                                        baseline_reset, tf, bvol.get(coin), qvol.get(coin), tcnt.get(coin),
                                        price_base_local
                                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[WS ERR] {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)
    return handler

async def process_tick_signal(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    coin: str,
    price: float,
    change: float,
    now_i: datetime.datetime,
    exchange: str,
    market_type: str,
    fast_thr: float,
    confirm_thr: float,
    last_fast_time: Dict[str, datetime.datetime],
    last_confirm_time: Dict[str, datetime.datetime],
    last_fast_direction: Dict[str, int],
    baseline_reset: bool,
    tf: str,
    base_vol,
    quote_vol,
    trades,
    price_base_local: Dict[str, Tuple[datetime.datetime, float]]
):
    abs_change = abs(change)
    direction = 1 if change > 0 else -1
    tv_symbol_with_note, tv_url = build_tradingview_url(exchange, market_type, coin)

    rep_map = repeat_last_notified[chat_id]
    rep_info = rep_map.get(coin)
    if rep_info and rep_info.get('dir') != direction:
        rep_map.pop(coin, None)
        rep_info = None

    # Получаем свечи для анализа
    candles_df = await candle_cache_get(exchange, market_type, coin.upper(), tf, 20)
    # Безопасно получаем rsi, ema, volume (если нет данных — ставим заглушки)
    try:
        rsi = float(candles_df['rsi'].iloc[-1]) if candles_df is not None and 'rsi' in candles_df.columns else 50
    except Exception:
        rsi = 50
    try:
        ema = float(candles_df['ema'].iloc[-1]) if candles_df is not None and 'ema' in candles_df.columns else 200
    except Exception:
        ema = 200
    try:
        volume = float(candles_df['volume'].iloc[-1]) if candles_df is not None and 'volume' in candles_df.columns else 0
    except Exception:
        volume = 0

    # Confirm
    need_confirm = abs_change >= confirm_thr
    can_confirm = False
    if need_confirm:
        last_ct = last_confirm_time.get(coin)
        if not last_ct or (now_i - last_ct).total_seconds() >= CONFIRM_COOLDOWN:
            last_ft = last_fast_time.get(coin)
            if last_ft is None or (now_i - last_ft).total_seconds() >= MIN_SECONDS_BETWEEN_FAST_AND_CONFIRM:
                can_confirm = True
    if can_confirm:
        last_confirm_time[coin] = now_i
        rep_map[coin] = {'dir': direction, 'abs_change': abs_change, 'last_time': now_i}
        if baseline_reset:
            rep_map.pop(coin, None)
            price_base_local[coin] = (now_i, price)
        # ---- Вставь сюда вызов разворота ----
        from signal_patterns import check_reversal_signal
        await check_reversal_signal(
            context,
            chat_id,
            coin,
            price,
            direction,
            tf,
            exchange,
            market_type,
            tv_url,
            now_i,
            base_vol
        )
        # AI прогноз — передаём реальные параметры!
        from ai_trainer import predict_signal
        ai_result = ""
        try:
            ai_result = predict_signal(
                price,
                volume,
                price,  # target_price — можно заменить на TP, если оно есть
                rsi,
                ema,
                "up" if direction == 1 else "down",
                "CONFIRM",
                coin,
                now_i.strftime("%Y-%m-%d %H:%M:%S")
            )
        except Exception as e:
            ai_result = f"AI ошибка: {e}"

        # Отправка уведомления с AI прогнозом
        text_msg = (
            f"🟢 <b>CONFIRM сигнал</b> {tv_symbol_with_note} <b>{change:+.2f}%</b>\n"
            f"AI прогноз: <b>{ai_result}</b>\n"
            f"Биржа: <b>{exchange.upper()}</b> | Тип: <b>{market_type}</b> | TF: <b>{tf}</b>\n"
            f"Время: <b>{now_i.strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
            f"Цена: <b>{format_price(price)}</b>\n"
            f"Vol24: <b>{compact_number(base_vol)}</b> | Сделок: <b>{human_trades(trades)}</b>\n"
            f"<a href='{tv_url}'>TradingView</a>"
        )
        try:
            await context.bot.send_message(chat_id, text_msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"[CONFIRM SEND FAIL] {e}")

    # ----- Проверка паттерн-сигналов CHoCH, BOS, RSI -----
    try:
        from signal_patterns import check_signals
        await check_signals(
            context,
            chat_id,
            coin,
            tf,
            exchange,
            market_type,
            tv_url,
            support_zones=[],
            resistance_zones=[]
        )
    except Exception as e:
        logger.error(f"[PATTERN SIGNAL FAIL] {coin}: {e}")
        return

    # Fast
    need_fast = (abs_change >= fast_thr) and (abs_change < confirm_thr)
    if need_fast:
        last_ft = last_fast_time.get(coin)
        send_fast = False
        if last_ft:
            if (now_i - last_ft).total_seconds() >= FAST_COOLDOWN:
                send_fast = True
        else:
            send_fast = True
        if send_fast:
            context.application.create_task(
                candle_cache_prefetch(exchange, market_type, coin.upper(), tf, CHART_CANDLES_LIMIT)
            )
            # AI прогноз для FAST тоже можно добавить аналогично!
            try:
                ai_result = predict_signal(
                    price,
                    volume,
                    price,
                    rsi,
                    ema,
                    "up" if direction == 1 else "down",
                    "FAST",
                    coin,
                    now_i.strftime("%Y-%m-%d %H:%M:%S")
                )
            except Exception as e:
                ai_result = f"AI ошибка: {e}"

            text_msg = (
                f"{'🟢' if direction == 1 else '🔴'} <b>FAST сигнал</b> {coin}/USDT {'↑' if direction == 1 else '↓'} <b>{change:+.2f}%</b>\n"
                f"AI прогноз: <b>{ai_result}</b>\n"
                f"Биржа: <b>{exchange.upper()}</b> | Тип: <b>{market_type}</b> | TF: <b>{tf}</b>\n"
                f"Время: <b>{now_i.strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
                f"Цена: <b>{format_price(price)}</b>\n"
                f"<a href='{tv_url}'>TradingView</a>"
            )
            try:
                await context.bot.send_message(chat_id, text_msg, parse_mode="HTML")
            except Exception as e:
                logger.error(f"[FAST SEND FAIL] {e}")

            last_fast_time[coin] = now_i
            last_fast_direction[coin] = direction
            rep_map[coin] = {'dir': direction, 'abs_change': abs_change, 'last_time': now_i}
            return

    # Repeat — без изменений
    settings = user_settings.get(chat_id, {})
    if not settings.get('repeat_enabled', True):
        return
    step = float(settings.get('repeat_step', REPEAT_STEP_DEFAULT))
    rep_cooldown = int(settings.get('repeat_cooldown', REPEAT_COOLDOWN_DEFAULT))
    rep_info = rep_map.get(coin)
    if rep_info and rep_info.get('dir') == direction:
        last_abs = float(rep_info.get('abs_change', 0.0))
        last_time = rep_info.get('last_time')
        abs_step_reached = abs_change - last_abs >= step

        candles_df = await candle_cache_get(exchange, market_type, coin.upper(), tf, 20)
        if candles_df is None or len(candles_df) < 6:
            return

        closes = candles_df['close'].iloc[-6:]
        last_close = closes.iloc[-1]
        first_close = closes.iloc[0]

        rollback = False
        if direction == 1:
            rollback = closes.min() < first_close + (last_close - first_close) * 0.5
        else:
            rollback = closes.max() > first_close - (first_close - last_close) * 0.5
        if rollback:
            rep_info['abs_change'] = abs_change
            rep_info['last_time'] = now_i
            return

        last_vol = candles_df['volume'].iloc[-1]
        median_vol = candles_df['volume'].iloc[-6:].median()
        if last_vol < median_vol:
            return

        high = candles_df['high'].iloc[:-1].max()
        low = candles_df['low'].iloc[:-1].min()
        broke_high = last_close > high
        broke_low = last_close < low
        if direction == 1 and not broke_high:
            return
        if direction == -1 and not broke_low:
            return

        if abs_step_reached:
            if (last_time is None) or ((now_i - last_time).total_seconds() >= rep_cooldown):
                await send_repeat_notification(context, chat_id, coin, change, price, exchange, market_type, tv_url)
                rep_info['abs_change'] = abs_change
                rep_info['last_time'] = now_i

       # ===================== HANDLERS FAB =====================
binance_spot_ws_handler    = make_handler('binance', 'spot')
binance_futures_ws_handler = make_handler('binance', 'futures')
bybit_spot_ws_handler      = make_handler('bybit', 'spot')
bybit_futures_ws_handler   = make_handler('bybit', 'futures')

async def restart_user_ws_task(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    old = user_ws_tasks.get(chat_id)
    if old:
        old.cancel()
        with suppress(asyncio.CancelledError):
            await old
    s = user_settings[chat_id]
    if s['exchange'] == 'binance':
        handler = binance_spot_ws_handler if s['market_type'] == 'spot' else binance_futures_ws_handler
    else:
        handler = bybit_spot_ws_handler if s['market_type'] == 'spot' else bybit_futures_ws_handler
    task = context.application.create_task(handler(chat_id, 0, 0, context))
    user_ws_tasks[chat_id] = task

async def stop_user_ws_task(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    t = user_ws_tasks.get(chat_id)
    if t:
        t.cancel()
        with suppress(asyncio.CancelledError):
            await t
        user_ws_tasks.pop(chat_id, None)

# ===================== HELP PAGINATION =====================
def build_help_keyboard(lang: str, page: int, total: int, mode: str):
    prev_label = "◀️ Пред" if lang=='ru' else "◀️ Prev"
    next_label = "След ▶️" if lang=='ru' else "Next ▶️"
    close_label = "Закрыть" if lang=='ru' else "Close"
    lang_btn = "EN 🇬🇧" if lang=='ru' else "RU 🇷🇺"
    mode_btn = "Полная" if (lang=='ru' and mode=='short') else ("Кратко" if (lang=='ru' and mode=='full') else ("Full" if mode=='short' else "Short"))
    buttons = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(prev_label, callback_data=f"help_nav:{page-1}"))
    if page < total - 1:
        nav_row.append(InlineKeyboardButton(next_label, callback_data=f"help_nav:{page+1}"))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton(lang_btn, callback_data=f"help_lang:{'en' if lang=='ru' else 'ru'}"),
                    InlineKeyboardButton(mode_btn, callback_data=f"help_mode:{'full' if mode=='short' else 'short'}")])
    buttons.append([InlineKeyboardButton(close_label, callback_data="help_close")])
    return InlineKeyboardMarkup(buttons)

def compose_help_text(chat_id: int, page_text: str, lang: str, mode: str, page: int, total: int) -> str:
    status = format_status_text(chat_id)
    header = "ПОМОЩЬ" if lang=='ru' else "HELP"
    mode_line = ("Режим: Полный" if (lang=='ru' and mode=='full') else ("Режим: Краткий" if lang=='ru' else ("Mode: Full" if mode=='full' else "Mode: Short")))
    return f"{header} | {mode_line} | {page+1}/{total}\n\n{status}\n\n{page_text}"

async def show_help_page(_update_or_context, context: ContextTypes.DEFAULT_TYPE, chat_id: int, page: int = 0):
    ensure_user(chat_id)
    st = help_state.setdefault(chat_id, {'page': 0, 'mode': 'short', 'msg_id': None})
    lang = user_settings[chat_id].get('lang', 'ru')
    mode = st.get('mode', 'short')
    pages = get_help_pages(lang, mode)
    total = len(pages)
    if total == 0:
        return
    page = max(0, min(page, total-1))
    text = compose_help_text(chat_id, pages[page], lang, mode, page, total)
    kb = build_help_keyboard(lang, page, total, mode)
    msg_id = st.get('msg_id')
    if msg_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=kb)
        except BadRequest:
            m = await context.bot.send_message(chat_id, text, reply_markup=kb)
            st['msg_id'] = m.message_id
    else:
        m = await context.bot.send_message(chat_id, text, reply_markup=kb)
        st['msg_id'] = m.message_id
    st['page'] = page

    # ===================== КОМАНДЫ =====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    first_name = user.first_name or ""
    username = user.username or ""
    language_code = user.language_code or ""
    register_user(chat_id, first_name, username, language_code)  # ← вот здесь учёт пользователя
    ensure_user(chat_id)
    await send_or_edit_menu(context, chat_id, force_new=True)

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(f"937124861: {user_id}\nADMIN_ID: {ADMIN_ID}")
    if user_id != ADMIN_ID:
        await update.message.reply_text("Нет доступа.")
        return
    from users_manager import get_all_users
    users = get_all_users()
    if not users:
        await update.message.reply_text("Нет пользователей.")
        return
    text_lines = []
    for u in users:
        line = (
            f"ID: {u['chat_id']}\n"
            f"Имя: {u['first_name']}\n"
            f"Username: @{u['username']}\n"
            f"Язык: {u['language_code']}\n"
            f"Первый вход: {u['date_first']}\n"
            f"Последний вход: {u['date_last']}\n"
            f"Входов: {u['visits']}\n"
            "-------------------"
        )
        text_lines.append(line)
    await update.message.reply_text("\n".join(text_lines))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    help_state.setdefault(chat_id, {'page': 0, 'mode': 'short', 'msg_id': None})
    await show_help_page(update, context, chat_id, help_state[chat_id]['page'])

async def fast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]; lang = s.get('lang','ru')
    if not context.args:
        await update.message.reply_text("Использование: /fast 3.0" if lang=='ru' else "Usage: /fast 3.0")
        return
    raw = context.args[0].replace(',', '.')
    try:
        val = float(raw)
        if not (0 < val < 100):
            raise ValueError
    except:
        await update.message.reply_text("Неверное значение fast (0 < value < 100)." if lang=='ru' else "Invalid fast (0 < value < 100).")
        return
    s['fast_threshold'] = val
    settings_changed(chat_id)
    await update.message.reply_text(f"Fast порог установлен: {val}%" if lang=='ru' else f"Fast threshold set: {val}%")

async def confirm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]; lang = s.get('lang','ru')
    if not context.args:
        await update.message.reply_text("Использование: /confirm 5.0" if lang=='ru' else "Usage: /confirm 5.0")
        return
    raw = context.args[0].replace(',', '.')
    try:
        val = float(raw)
        if not (0 < val < 100):
            raise ValueError
    except:
        await update.message.reply_text("Неверное значение confirm (0 < value < 100)." if lang=='ru' else "Invalid confirm (0 < value < 100).")
        return
    s['confirm_threshold'] = val
    settings_changed(chat_id)
    await update.message.reply_text(f"Confirm порог установлен: {val}%" if lang=='ru' else f"Confirm threshold set: {val}%")

async def threshold_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await confirm_command(update, context)

async def baselinereset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    lang = user_settings[chat_id].get('lang','ru')
    if not context.args:
        st = "on" if user_settings[chat_id]['baseline_reset'] else "off"
        await update.message.reply_text(
            f"Текущее состояние: {st}. Использование: /baselinereset on|off" if lang=='ru'
            else f"Current state: {st}. Usage: /baselinereset on|off"
        )
        return
    val = context.args[0].lower()
    if val not in ("on", "off"):
        await update.message.reply_text("on или off" if lang=='ru' else "Use on or off")
        return
    user_settings[chat_id]['baseline_reset'] = (val == "on")
    settings_changed(chat_id)
    await update.message.reply_text(f"baseline_reset = {val}")

async def tf_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    lang = user_settings[chat_id].get('lang','ru')
    if not context.args:
        await update.message.reply_text(
            ("Использование: /tf 1h (" + ", ".join(ALLOWED_TF) + ")") if lang=='ru'
            else ("Usage: /tf 1h (" + ", ".join(ALLOWED_TF) + ")")
        ); return
    tf = context.args[0].lower()
    if tf not in ALLOWED_TF:
        await update.message.reply_text("Недопустимый таймфрейм." if lang=='ru' else "Invalid timeframe.")
        return
    user_settings[chat_id]['tf'] = tf
    settings_changed(chat_id)
    await update.message.reply_text(f"Таймфрейм установлен: {tf}" if lang=='ru' else f"Timeframe set: {tf}")

async def reload_symbols_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = user_settings.get(chat_id, {}).get('lang','ru')
    await update.message.reply_text("Обновляю символы Binance..." if lang=='ru' else "Refreshing Binance symbols...")
    try:
        await load_binance_symbol_sets(force=True)
        await update.message.reply_text(
            f"Загружено: spot={len(BINANCE_SPOT_USDT)} futures={len(BINANCE_FUT_USDTP)}" if lang=='ru'
            else f"Loaded: spot={len(BINANCE_SPOT_USDT)} futures={len(BINANCE_FUT_USDTP)}"
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}" if lang=='ru' else f"Error: {e}")

async def debug_symbol_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = user_settings.get(update.effective_chat.id, {}).get('lang','ru')
    if not context.args:
        await update.message.reply_text("Использование: /debug_symbol BTC" if lang=='ru' else "Usage: /debug_symbol BTC")
        return
    coin = context.args[0].upper()
    in_spot = coin in BINANCE_SPOT_USDT
    in_fut  = coin in BINANCE_FUT_USDTP
    await update.message.reply_text(
        f"{coin}: spot={in_spot} futures={in_fut} (посл. обновление={BINANCE_LAST_REFRESH})" if lang=='ru'
        else f"{coin}: spot={in_spot} futures={in_fut} (last refresh={BINANCE_LAST_REFRESH})"
    )
    
 # ===================== КОМАНДЫ (ПОДПИСКИ, ЯЗЫК, ПОВТОРЫ, АДМИН) =====================

async def tune_command(update, context):
    chat_id = update.effective_chat.id
    lang = user_settings.get(chat_id, {}).get('lang', 'ru')
    if chat_id != ADMIN_ID:
        await update.message.reply_text("Нет доступа.")
        return
    await update.message.reply_text("Запускаю автообучение..." if lang == 'ru' else "Starting auto tuning...")
    try:
        subprocess.run(["python3", "auto_tune.py"])
        await update.message.reply_text("Обучение завершено." if lang == 'ru' else "Auto tuning finished.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}" if lang == 'ru' else f"Error: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = user_settings.get(chat_id, {}).get('lang','ru')
    rec = get_user_sub(chat_id)
    active = has_active_access(chat_id)
    if lang=='ru':
        msg = (f"Подписка:\nАктивна: {'Да' if active else 'Нет'}\nДо: {formatted_expiry(chat_id)}\n"
               f"Trial использован: {'Да' if rec.get('trial_used') else 'Нет'}\n"
               f"Система подписок: {'Включена' if SUBSCRIPTIONS_ENABLED else 'Отключена'}")
    else:
        msg = (f"Subscription:\nActive: {'Yes' if active else 'No'}\nUntil: {formatted_expiry(chat_id)}\n"
               f"Trial used: {'Yes' if rec.get('trial_used') else 'No'}\n"
               f"System: {'Enabled' if SUBSCRIPTIONS_ENABLED else 'Disabled'}")
    await update.message.reply_text(msg)

async def trial_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lang = user_settings.get(chat_id, {}).get('lang','ru')
    if not SUBSCRIPTIONS_ENABLED:
        await update.message.reply_text("Trial отключён." if lang=='ru' else "Trial disabled.")
        return
    rec = get_user_sub(chat_id)
    if rec.get("trial_used"):
        await update.message.reply_text("Пробный период уже использован." if lang=='ru' else "Trial already used.")
        return
    now_ts = datetime.datetime.utcnow().timestamp()
    new_expiry = max(now_ts, rec.get("expiry_ts", 0)) + TRIAL_DURATION_SECONDS
    SUBSCRIPTIONS[str(chat_id)] = {"trial_used": True, "expiry_ts": new_expiry}
    save_subscriptions()
    await update.message.reply_text(
        f"Trial активирован до: {formatted_expiry(chat_id)}" if lang=='ru'
        else f"Trial activated until: {formatted_expiry(chat_id)}"
    )
    await send_or_edit_menu(context, chat_id, force_new=False)

async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = user_settings.get(update.effective_chat.id, {}).get('lang','ru')
    if not SUBSCRIPTIONS_ENABLED:
        await update.message.reply_text("Подписки отключены. Доступ свободный." if lang=='ru'
                                        else "Subscriptions disabled. Free access.")
        return
    if lang=='ru':
        text = ("Оплата подписки:\n1. Оплатите на кошелёк.\n2. Сообщите админу хеш.\n"
                f"ID администратора: {ADMIN_ID}")
    else:
        text = ("Payment:\n1. Send to wallet.\n2. Provide tx hash to admin.\n"
                f"Admin ID: {ADMIN_ID}")
    await update.message.reply_text(text)

async def grant_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = user_settings.get(update.effective_chat.id, {}).get('lang','ru')
    if not SUBSCRIPTIONS_ENABLED:
        await update.message.reply_text("Система подписок отключена." if lang=='ru' else "Subscription system disabled.")
        return
    if update.effective_user.id != ADMIN_ID or ADMIN_ID == 0:
        await update.message.reply_text("Недостаточно прав." if lang=='ru' else "Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /grant <days> [user_id]" if lang=='ru'
                                        else "Usage: /grant <days> [user_id]")
        return
    try:
        days = int(context.args[0])
    except:
        await update.message.reply_text("Некорректное число дней." if lang=='ru' else "Invalid days.")
        return
    if days <= 0:
        await update.message.reply_text("Дней должно быть > 0." if lang=='ru' else "Days must be > 0.")
        return
    if len(context.args) > 1:
        try: target_id = int(context.args[1])
        except:
            await update.message.reply_text("user_id некорректен." if lang=='ru' else "Invalid user_id.")
            return
    else:
        target_id = update.effective_chat.id
    rec = get_user_sub(target_id)
    now_ts = datetime.datetime.utcnow().timestamp()
    new_expiry = max(now_ts, rec.get("expiry_ts", 0)) + days * 86400
    SUBSCRIPTIONS[str(target_id)] = {"trial_used": rec.get('trial_used', False), "expiry_ts": new_expiry}
    save_subscriptions()
    await update.message.reply_text(
        f"Выдано {days} дн. до {formatted_expiry(target_id)} (user {target_id})" if lang=='ru'
        else f"Granted {days} d until {formatted_expiry(target_id)} (user {target_id})"
    )
    if target_id == update.effective_chat.id:
        await send_or_edit_menu(context, target_id, force_new=False)

async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    if not context.args:
        await update.message.reply_text("Использование: /lang ru|en\nUsage: /lang ru|en")
        return
    choice = context.args[0].lower()
    if choice not in ('ru','en'):
        await update.message.reply_text("Допустимо: ru или en\nAllowed: ru or en")
        return
    user_settings[chat_id]['lang'] = choice
    settings_changed(chat_id)
    msg = "Язык переключён." if choice=='ru' else "Language switched."
    await update.message.reply_text(msg)
    await send_or_edit_menu(context, chat_id, force_new=False)

async def repeat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]; lang = s.get('lang','ru')

    def status_text():
        if lang=='ru':
            return (f"Повторы: {'ON' if s['repeat_enabled'] else 'OFF'}\n"
                    f"Шаг: {s['repeat_step']}%\nКулдаун: {s['repeat_cooldown']} сек\n"
                    "Команды:\n/repeat on|off\n/repeat step <число>\n/repeat cooldown <сек>\n/repeat status")
        else:
            return (f"Repeats: {'ON' if s['repeat_enabled'] else 'OFF'}\n"
                    f"Step: {s['repeat_step']}%\nCooldown: {s['repeat_cooldown']} s\n"
                    "Commands:\n/repeat on|off\n/repeat step <number>\n/repeat cooldown <seconds>\n/repeat status")

    if not context.args:
        await update.message.reply_text(status_text())
        return

    cmd = context.args[0].lower()
    if cmd in ('status',):
        await update.message.reply_text(status_text()); return
    if cmd in ('on','off'):
        s['repeat_enabled'] = (cmd == 'on')
        settings_changed(chat_id)
        await update.message.reply_text("Повторы включены" if (cmd=='on' and lang=='ru') else
                                        ("Повторы выключены" if lang=='ru' else
                                         ("Repeats enabled" if cmd=='on' else "Repeats disabled")))
        return
    if cmd == 'step':
        if len(context.args) < 2:
            await update.message.reply_text("Нужно число: /repeat step 0.8" if lang=='ru' else "Need number: /repeat step 0.8")
            return
        raw = context.args[1].replace(',', '.')
        try:
            val = float(raw)
            if not (REPEAT_STEP_MIN <= val <= REPEAT_STEP_MAX):
                raise ValueError
        except:
            await update.message.reply_text("Некорректный шаг (пределы см. меню)" if lang=='ru' else "Invalid step (see limits in menu)")
            return
        s['repeat_step'] = round(val, 2)
        settings_changed(chat_id)
        await update.message.reply_text(f"Шаг повтора = {val}%" if lang=='ru' else f"Repeat step = {val}%")
        return
    if cmd == 'cooldown':
        if len(context.args) < 2:
            await update.message.reply_text("Нужно: /repeat cooldown 8" if lang=='ru' else "Need: /repeat cooldown 8")
            return
        try:
            val = int(context.args[1])
            if not (REPEAT_COOLDOWN_MIN <= val <= REPEAT_COOLDOWN_MAX):
                raise ValueError
        except:
            await update.message.reply_text("Кулдаун вне диапазона" if lang=='ru' else "Cooldown out of range")
            return
        s['repeat_cooldown'] = val
        settings_changed(chat_id)
        await update.message.reply_text(f"Кулдаун повтора = {val}s" if lang=='ru' else f"Repeat cooldown = {val}s")
        return

    await update.message.reply_text(status_text())

    # ===================== CALLBACK ROUTER =====================

def _adjust_step(current: float, delta: float) -> float:
    new = current + delta
    new = max(REPEAT_STEP_MIN, min(REPEAT_STEP_MAX, new))
    return round(new + 1e-9, 2)

def _adjust_cd(current: int, delta: int) -> int:
    new = current + delta
    return max(REPEAT_COOLDOWN_MIN, min(REPEAT_COOLDOWN_MAX, new))

async def handle_toggle_strategy(s, data, chat_id, context, q):
    for code in ['choch', 'bos', 'rsi', 'pump']:
        if data == f'toggle_{code}':
            strategies = s.setdefault('strategies', {})
            strategies[code] = not strategies.get(code, True)
            save_user_settings()
            await send_or_edit_menu(context, chat_id, force_new=True)
            await q.answer(f"{code.upper()} {'включена' if strategies[code] else 'выключена'}")
            return True
    return False

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    chat_id = q.message.chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]
    lang = s.get('lang', 'ru')
    data = q.data

    try:
        # 1. Переключение стратегий
        if await handle_toggle_strategy(s, data, chat_id, context, q):
            return

        # 2. Остальные обработчики (разделяй по типу действия)
                # --- BYBIT TRADE SETTINGS ---

        if data == "bybit_trade_settings":
            s['menu_view'] = 'bybit_trade_settings'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == "bybit_leverage":
            await context.bot.send_message(chat_id, "Введите кредитное плечо (например, 10):")
            s['menu_view'] = 'bybit_leverage'
            await q.answer()
            return

        if data == "bybit_volume":
            await context.bot.send_message(chat_id, "Введите размер позиции (например, 0.01):")
            s['menu_view'] = 'bybit_volume'
            await q.answer()
            return

        if data == "bybit_order_type":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Market", callback_data="bybit_order_type_set:Market"),
                 InlineKeyboardButton("Limit", callback_data="bybit_order_type_set:Limit")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="bybit_trade_settings")]
            ])
            await context.bot.send_message(chat_id, "Выберите тип ордера:", reply_markup=kb)
            await q.answer()
            return

        if data.startswith("bybit_order_type_set:"):
            s['bybit_trade']['order_type'] = data.split(":")[1]
            s['menu_view'] = 'bybit_trade_settings'
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await q.answer("Тип ордера установлен!")
            return

        if data == "bybit_tp":
            await context.bot.send_message(chat_id, "Введите Take Profit (например, 28000):")
            s['menu_view'] = 'bybit_tp'
            await q.answer()
            return

        if data == "bybit_sl":
            await context.bot.send_message(chat_id, "Введите Stop Loss (например, 27000):")
            s['menu_view'] = 'bybit_sl'
            await q.answer()
            return
        
        
        if data == 'strategy_choch':
            await run_strategy(context, chat_id, 'choch')
            await q.answer("CHoCH+Volume")
            return

        if data == 'strategy_bos':
            await run_strategy(context, chat_id, 'bos')
            await q.answer("BOS+Zone")
            return

        if data == 'strategy_rsi':
            await run_strategy(context, chat_id, 'rsi')
            await q.answer("RSI Divergence")
            return

        if data == 'strategy_pump':
            await run_strategy(context, chat_id, 'pump')
            await q.answer("Pump/Dump")
            return

        if data == 'update_stats':
            await update_stats_command(context, chat_id)
            await send_or_edit_menu(context, chat_id, force_new=True)
            await q.answer("Статистика обновлена!")
            return

        if data == 'toggle_menu':
            s['menu_expanded'] = not s['menu_expanded']
            s['menu_view'] = 'main'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'back_main':
            s['menu_view'] = 'main'
            s['manual_threshold_error'] = None
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'start_monitorинг':
            if not has_active_access(chat_id):
                await q.answer("Нет активной подписки" if lang=='ru' else "No active subscription", show_alert=True)
                await reset_to_main_menu(context, chat_id)
                return
            await restart_user_ws_task(chat_id, context)
            await reset_to_main_menu(context, chat_id)
            await q.answer("Мониторинг запущен" if lang=='ru' else "Monitoring started")
            return

        if data == 'stop_monitorинг':
            await stop_user_ws_task(chat_id, context)
            await reset_to_main_menu(context, chat_id)
            await q.answer("Мониторинг остановлен" if lang=='ru' else "Monitoring stopped")
            return

        if data == 'view_threshold':
            s['menu_view'] = 'threshold_select'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'view_manual_threshold':
            s['menu_view'] = 'manual_threshold'
            s['manual_threshold_error'] = None
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'view_exchange':
            s['menu_view'] = 'exchange_select'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'view_market':
            s['menu_view'] = 'market_select'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'view_tf':
            s['menu_view'] = 'tf_select'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data == 'toggle_baseline':
            s['baseline_reset'] = not s.get('baseline_reset', True)
            settings_changed(chat_id)
            await reset_to_main_menu(context, chat_id)
            await q.answer("OK")
            return

        if data == 'view_lang':
            s['menu_view'] = 'lang_select'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data.startswith("lang_"):
            choice = data.split("_",1)[1]
            if choice in ('ru','en'):
                s['lang'] = choice
                settings_changed(chat_id)
            await reset_to_main_menu(context, chat_id)
            await q.answer("OK")
            return

        if data == 'open_help':
            help_state[chat_id] = {'page': 0, 'mode': 'short', 'msg_id': None}
            await show_help_page(update, context, chat_id, 0)
            await q.answer()
            return

        if data.startswith("help_nav:"):
            page = int(data.split(":",1)[1])
            await show_help_page(update, context, chat_id, page)
            await q.answer()
            return

        if data.startswith("help_lang:"):
            new_lang = data.split(":",1)[1]
            if new_lang in ('ru','en'):
                s['lang'] = new_lang
                settings_changed(chat_id)
            cur = help_state.get(chat_id, {}).get('page', 0)
            await show_help_page(update, context, chat_id, cur)
            await q.answer()
            return

        if data.startswith("help_mode:"):
            new_mode = data.split(":",1)[1]
            hs = help_state.setdefault(chat_id, {'page': 0, 'mode': 'short', 'msg_id': None})
            if new_mode in ('short','full'):
                hs['mode'] = new_mode
                hs['page'] = 0
            await show_help_page(update, context, chat_id, hs['page'])
            await q.answer()
            return

        if data == 'help_close':
            st = help_state.get(chat_id)
            if st and st.get('msg_id'):
                with suppress(Exception):
                    await context.bot.delete_message(chat_id, st['msg_id'])
            help_state.pop(chat_id, None)
            await q.answer("Закрыто" if lang=='ru' else "Closed")
            return

        if data.startswith("set_fast_confirm:"):
            with suppress(Exception):
                _, rest = data.split(":", 1)
                fast_val, conf_val = rest.split(":")
                fast_val = float(fast_val)
                conf_val = float(conf_val)
                if fast_val < conf_val:
                    s['fast_threshold'] = fast_val
                    s['confirm_threshold'] = conf_val
                    settings_changed(chat_id)
            await reset_to_main_menu(context, chat_id)
            await q.answer("Установлено" if lang=='ru' else "Applied")
            return

        if data.startswith("exchange_"):
            exch = data.split("_", 1)[1]
            if exch in EXCHANGE_CHOICES:
                s['exchange'] = exch
                settings_changed(chat_id)
                if chat_id in user_ws_tasks:
                    await restart_user_ws_task(chat_id, context)
            await reset_to_main_menu(context, chat_id)
            await q.answer("Биржа изменена" if lang=='ru' else "Exchange changed")
            return

        if data.startswith("market_"):
            mt = data.split("_", 1)[1]
            if mt in MARKET_TYPE_CHOICES:
                s['market_type'] = mt
                settings_changed(chat_id)
                if chat_id in user_ws_tasks:
                    await restart_user_ws_task(chat_id, context)
            await reset_to_main_menu(context, chat_id)
            await q.answer("Тип изменён" if lang=='ru' else "Market type changed")
            return

        if data.startswith("tf_"):
            tf = data.split("_",1)[1]
            if tf in ALLOWED_TF:
                s['tf'] = tf
                settings_changed(chat_id)
            await reset_to_main_menu(context, chat_id)
            await q.answer(tf)
            return

        if data.startswith("copy_pair:"):
            pair = data.split(":", 1)[1]
            await q.answer(pair, show_alert=True)
            return

        if data == 'toggle_repeat':
            s['repeat_enabled'] = not s.get('repeat_enabled', True)
            settings_changed(chat_id)
            if s['menu_view'] == 'repeat_settings':
                await send_or_edit_menu(context, chat_id)
            else:
                await reset_to_main_menu(context, chat_id)
            await q.answer("OK")
            return

        if data == 'view_repeat_settings':
            s['menu_view'] = 'repeat_settings'
            await send_or_edit_menu(context, chat_id)
            await q.answer()
            return

        if data.startswith("repeat_step_set:"):
            with suppress(Exception):
                val = float(data.split(":",1)[1])
                if REPEAT_STEP_MIN <= val <= REPEAT_STEP_MAX:
                    s['repeat_step'] = round(val, 2)
                    settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await q.answer("OK")
            return

        if data.startswith("repeat_cd_set:"):
            with suppress(Exception):
                val = int(data.split(":",1)[1])
                if REPEAT_COOLDOWN_MIN <= val <= REPEAT_COOLDOWN_MAX:
                    s['repeat_cooldown'] = val
                    settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await q.answer("OK")
            return

        if data.startswith("repeat_step_adj:"):
            delta = float(data.split(":",1)[1])
            s['repeat_step'] = _adjust_step(float(s['repeat_step']), delta)
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await q.answer("OK")
            return

        if data.startswith("repeat_cd_adj:"):
            delta = int(data.split(":",1)[1])
            s['repeat_cooldown'] = _adjust_cd(int(s['repeat_cooldown']), delta)
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await q.answer("OK")
            return

        if data == 'noop':
            await q.answer()
            return

        if data.startswith("summary:"):
            await q.answer("Резюме пока не реализовано" if lang=='ru' else "Summary not implemented yet", show_alert=True)
            return

        # Если действие не распознано
        await q.answer("Неизвестное действие" if lang=='ru' else "Unknown action")

    except Exception as e:
        logger.error(f"[CB ERROR] {e}")
        await q.answer("Ошибка" if lang=='ru' else "Error")
        await send_or_edit_menu(context, chat_id)
# ========== ВСТРОЕННАЯ ФУНКЦИЯ ДЛЯ ЗАПУСКА СТРАТЕГИЙ ==========
from telegram.helpers import escape_markdown

async def run_strategy(context, chat_id, strategy):
    from signal_patterns import (
        detect_choch_with_volume,
        detect_bos_and_zone,
        detect_rsi_divergence_with_volume
    )
    try:
        from signal_patterns import detect_pump_dump
    except ImportError:
        detect_pump_dump = None

    tf = user_settings[chat_id]['tf']
    exchange = user_settings[chat_id]['exchange']
    market_type = user_settings[chat_id]['market_type']
    coin = 'BTC'

    tv_symbol, tv_url = build_tradingview_url(exchange, market_type, coin)
    candles_df = await candle_cache_get(exchange, market_type, coin.upper(), tf, CHART_CANDLES_LIMIT)
    signal = None
    emoji = ""
    if strategy == 'choch':
        signal = detect_choch_with_volume(candles_df)
        emoji = "🔄"
    elif strategy == 'bos':
        signal = detect_bos_and_zone(candles_df, [], [])
        emoji = "🚦"
    elif strategy == 'rsi':
        signal = detect_rsi_divergence_with_volume(candles_df)
        emoji = "🧭"
    elif strategy == 'pump' and detect_pump_dump:
        signal = detect_pump_dump(candles_df)
        emoji = "📈"

    if not signal:
        text = f"{emoji} Нет сигнала по стратегии"
    else:
        msg = signal.get('msg', str(signal))
        text = f"{emoji} {msg}"

    text = md2_safe(text)

    await context.bot.send_message(chat_id, text, parse_mode='MarkdownV2')
    
   # ===================== РУЧНОЙ ВВОД ПОРОГОВ =====================
async def manual_threshold_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]
    lang = s.get('lang','ru')
    if s['menu_view'] != 'manual_threshold':
        return
    with suppress(Exception):
        await update.message.delete()
    raw = update.message.text.strip().replace(',', '.')
    parts = raw.split()
    if len(parts) != 2:
        s['manual_threshold_error'] = "Нужно два числа: fast confirm" if lang=='ru' else "Need two numbers: fast confirm"
        await send_or_edit_menu(context, chat_id)
        return
    try:
        fv = float(parts[0]); cv = float(parts[1])
        if not (0 < fv < cv <= 100):
            raise ValueError
    except:
        s['manual_threshold_error'] = "Формат: 3 5 (fast < confirm ≤ 100)" if lang=='ru' else "Format: 3 5 (fast < confirm ≤ 100)"
        await send_or_edit_menu(context, chat_id)
        return
    s['fast_threshold'] = fv
    s['confirm_threshold'] = cv
    s['manual_threshold_error'] = None
    s['menu_view'] = 'main'
    settings_changed(chat_id)
    await send_or_edit_menu(context, chat_id)

# ===================== SHUTDOWN =====================
async def _safe_delete_message(bot, chat_id: int, message_id: int):
    with suppress(Exception):
        await bot.delete_message(chat_id, message_id)

async def on_shutdown(application):
    logger.info("Shutdown: saving settings and announcing maintenance")
    save_user_settings()
    maintenance_text = "Ваш помощник на обслуживании"
    all_chat_ids = set()
    all_chat_ids.update(user_settings.keys())
    all_chat_ids.update(active_menu_message_id.keys())
    all_chat_ids.update(help_state.keys())
    for chat_id in list(all_chat_ids):
        msg_id = active_menu_message_id.get(chat_id)
        if msg_id:
            await _safe_delete_message(application.bot, chat_id, msg_id)
        hs = help_state.get(chat_id)
        if hs and hs.get('msg_id'):
            await _safe_delete_message(application.bot, chat_id, hs['msg_id'])
        with suppress(Exception):
            await application.bot.send_message(chat_id, maintenance_text)
    for t in list(user_ws_tasks.values()):
        t.cancel()
    await asyncio.gather(*user_ws_tasks.values(), return_exceptions=True)
    user_ws_tasks.clear()
    logger.info("Shutdown complete.")

# ... другие команды ...

async def update_stats_command(context, chat_id):
    import subprocess
    try:
        # Перезапускает пересчёт результатов и авто-тюн
        subprocess.run(["python3", "signal_result_updater.py"])
        await context.bot.send_message(chat_id, "Статистика по сигналам обновлена!")
    except Exception as e:
        await context.bot.send_message(chat_id, f"Ошибка обновления: {e}")

async def bybit_trade_param_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ensure_user(chat_id)
    s = user_settings[chat_id]
    trade = s['bybit_trade']
    menu = s['menu_view']

    val = update.message.text.strip()
    if menu == 'bybit_usdt_amount':
        try:
            amt = float(val)
            if amt <= 0:
                raise ValueError
            trade['usdt_amount'] = amt
            s['menu_view'] = 'bybit_trade_settings'
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await update.message.reply_text(f"Сумма позиции установлена: {amt} USDT")
        except Exception:
            await update.message.reply_text("Введите положительное число (например, 100).")
        return

    if menu == 'bybit_leverage':
        try:
            lev = int(val)
            if not (1 <= lev <= 100):
                raise ValueError
            trade['leverage'] = lev
            s['menu_view'] = 'bybit_trade_settings'
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await update.message.reply_text(f"Кредитное плечо установлено: {lev}x")
        except Exception:
            await update.message.reply_text("Введите число от 1 до 100.")
        return

    if menu == 'bybit_tp':
        try:
            tp = float(val)
            trade['tp'] = tp
            s['menu_view'] = 'bybit_trade_settings'
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await update.message.reply_text(f"TP установлен: {tp}")
        except Exception:
            await update.message.reply_text("Введите число.")
        return

    if menu == 'bybit_sl':
        try:
            sl = float(val)
            trade['sl'] = sl
            s['menu_view'] = 'bybit_trade_settings'
            settings_changed(chat_id)
            await send_or_edit_menu(context, chat_id)
            await update.message.reply_text(f"SL установлен: {sl}")
        except Exception:
            await update.message.reply_text("Введите число.")
        return

    # --- остальные меню (например, manual_threshold_input) ---

    # --- Здесь можешь оставить старую обработку других меню ---
    # manual_threshold_input и прочие

# ... остальные команды ...

 # ===================== MAIN =====================
# ЗАМЕНИТЕ вашу функцию main() и нижний блок запуска на этот код

async def start_auto_update(app):
    app.create_task(auto_update_signal_results(app))

def main():
    load_help_texts_from_files()
    load_subscriptions()
    load_user_settings()
    load_users()
    app = ApplicationBuilder().token(TOKEN).build()

    # Команды
    app.add_handler(CommandHandler('users', users_command))
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', start))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('tune', tune_command))
    app.add_handler(CommandHandler('fast', fast_command))
    app.add_handler(CommandHandler('confirm', confirm_command))
    app.add_handler(CommandHandler('threshold', threshold_command))
    app.add_handler(CommandHandler('baselinereset', baselinereset_command))
    app.add_handler(CommandHandler('tf', tf_command))
    app.add_handler(CommandHandler('reload_symbols', reload_symbols_command))
    app.add_handler(CommandHandler('debug_symbol', debug_symbol_command))
    app.add_handler(CommandHandler('status', status_command))
    app.add_handler(CommandHandler('trial', trial_command))
    app.add_handler(CommandHandler('buy', buy_command))
    app.add_handler(CommandHandler('grant', grant_command))
    app.add_handler(CommandHandler('lang', lang_command))
    app.add_handler(CommandHandler('repeat', repeat_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, universal_text_input))
    # Callback и ручной ввод
    app.add_handler(CallbackQueryHandler(callback_router, pattern=r"^(?!obscan:)"))

    # post_shutdown в PTB v20 задаётся как свойство приложения, а не параметр run_polling()
    app.post_shutdown = on_shutdown

    logger.info("Bot started (repeat alerts + settings + persistence + full help)")

    # Регистрация OB‑сканера Binance с панелью /obscan (если требуется)
    #register_obscan(app, user_settings, settings_changed, has_active_access)

    app.post_init = start_auto_update
    app.run_polling()

if __name__ == '__main__':
    main()