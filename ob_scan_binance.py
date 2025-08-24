import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple
from contextlib import suppress
import os
import urllib.parse
import logging
from collections import deque

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler()

# ====== Логирование ======
LOG_LEVEL = os.getenv("OBSCAN_LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("obscan")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(_h)
logger.setLevel(LOG_LEVEL)

def _ring(context: ContextTypes.DEFAULT_TYPE) -> deque:
    return context.application.bot_data.setdefault("obscan_ring", deque(maxlen=600))  # type: ignore[assignment]

def _log(context: ContextTypes.DEFAULT_TYPE, level: int, msg: str, **kv):
    txt = msg + (f" | {kv}" if kv else "")
    try:
        _ring(context).append(txt)
    except Exception:
        pass
    try:
        logger.log(level, txt)
    except Exception:
        pass

def _set_metric(context: ContextTypes.DEFAULT_TYPE, chat_id: int, **kv):
    metrics = context.application.bot_data.setdefault("obscan_metrics", {})  # type: ignore[assignment]
    m = metrics.setdefault(chat_id, {})
    m.update(kv)

def _get_metric(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> Dict[str, Any]:
    return context.application.bot_data.get("obscan_metrics", {}).get(chat_id, {})  # type: ignore[return-value]

# ====== Binance Futures API ======
FUT_TICKER_24H_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
FUT_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

SUPPORTED_TFS = ["5m", "15m"]
TV_LAYOUT_ID = os.getenv("TV_LAYOUT_ID", "").strip()
TV_FUT_SUFFIX = "USDT.P"

# ====== Настройки по умолчанию (Lean, без OB) ======
def _default_obscan() -> Dict[str, Any]:
    return {
        "enabled": False,
        "tfs": ["5m", "15m"],

        # Производительность
        "interval_sec": 200,
        "sleep_ms": 100,
        "min_vol_usdt": 20_000_000.0,
        "max_symbols": 40,
        "min_price": 0.02,

        # Тренд (общий кулдаун в минутах — для уведомлений)
        "cooldown_ta_min": 120,

        # BOS (слом структуры)
        "bos": {
            "enabled": True,
            "tfs": ["5m", "15m"],
            "swing_left": 4,
            "swing_right": 4,
            "margin_pct": 0.0015,       # запас над/под свингом
            "confirm_close": True,       # требовать закрытие за уровнем
            "min_body_ratio": 0.55,      # импульс тела последней свечи
            "vol_mult": 1.3,             # объём выше медианы в X раз
            "align_with_trend": False,   # фильтровать по тренду 15м
            "cooldown_sec": 180          # секунд между одинаковыми уровнями
        },

        # Снятие ликвидности (5m)
        "liquidity_alert": {
            "enabled": True,
            "tf": "5m",
            "lookback_swings": 4,
            "sweep_margin_pct": 0.0015,
            "min_wick_ratio": 0.60,
            "must_close_back": True,
            "vol_mult": 1.5,
            "align_against_trend": False,     # сигналить против тренда (для реверсов)
            "require_bos_post_confirm": False,# требовать BOS после свипа
            "cooldown_sec": 900               # секунд между алертами по символу/типу
        },

        # Тренд‑алерт (15m main, 5m подтверждение опционально)
        "trend_alert": {
            "enabled": True,
            "tf_main": "15m",
            "tf_aux": "5m",
            "confirm_bars": 2,
            "close_buffer_atr": 0.10,
            "adx_min": 25.0,
            "bos_margin_pct": 0.0015,
            "impulse_body_ratio": 0.60,
            "vol_mult": 1.5,
            "ema200_distance_max_atr": 1.5,
            "atr_pct_min": 0.003,
            "atr_pct_max": 0.025,
            "mtf_required": True,
            "hysteresis_bars": 2,  # не давать противоположный сигнал пока не пройдут N баров 15m
        },
    }

# ====== Индикаторы / математика ======
def _detect_pivots(highs: List[float], lows: List[float], left: int, right: int) -> Tuple[List[bool], List[bool]]:
    n = len(highs)
    sh = [False] * n
    sl = [False] * n
    for i in range(left, n - right):
        h, l = highs[i], lows[i]
        if all(h > highs[i - j - 1] for j in range(left)) and all(h >= highs[i + j + 1] for j in range(right)):
            sh[i] = True
        if all(l < lows[i - j - 1] for j in range(left)) and all(l <= lows[i + j + 1] for j in range(right)):
            sl[i] = True
    return sh, sl

def _last_true(flags: List[bool]) -> Optional[int]:
    for i in range(len(flags)-1, -1, -1):
        if flags[i]: return i
    return None

def _ema_last_pair(values: List[float], period: int) -> Optional[Tuple[float, float]]:
    n = len(values)
    if n < period: return None
    sma = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    ema_prev = sma
    ema = ema_prev
    for price in values[period:]:
        ema_prev = ema
        ema = price * k + ema * (1.0 - k)
    return (ema_prev, ema)

def _adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    n = len(closes)
    if n < period + 1: return 0.0
    tr_list: List[float] = []; plus_dm_list: List[float] = []; minus_dm_list: List[float] = []
    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        plus_dm_list.append(plus_dm); minus_dm_list.append(minus_dm); tr_list.append(tr)
    def _smooth(values: List[float], p: int) -> List[float]:
        if len(values) < p: return []
        s = [sum(values[:p])]
        for v in values[p:]:
            s.append(s[-1] - (s[-1]/p) + v)
        return s
    tr_s = _smooth(tr_list, period); plus_s = _smooth(plus_dm_list, period); minus_s = _smooth(minus_dm_list, period)
    if not tr_s or not plus_s or not minus_s: return 0.0
    di_plus = [0.0]*len(tr_s); di_minus = [0.0]*len(tr_s)
    for i in range(len(tr_s)):
        if tr_s[i] > 0:
            di_plus[i] = 100.0 * (plus_s[i]/tr_s[i])
            di_minus[i] = 100.0 * (minus_s[i]/tr_s[i])
    dx: List[float] = []
    for i in range(len(di_plus)):
        denom = di_plus[i] + di_minus[i]
        dx.append(0.0 if denom == 0 else 100.0 * abs(di_plus[i]-di_minus[i]) / denom)
    if len(dx) < period: return 0.0
    adx_vals = [sum(dx[:period])/period]
    for v in dx[period:]:
        adx_vals.append(((adx_vals[-1]*(period-1)) + v)/period)
    return float(adx_vals[-1]) if adx_vals else 0.0

def _median(vals: List[float]) -> float:
    if not vals: return 0.0
    w = sorted(vals); n = len(w)
    return w[n//2] if n % 2 == 1 else 0.5*(w[n//2 - 1] + w[n//2])

def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    n = len(closes)
    if n < period + 1: return 0.0
    trs: List[float] = []
    for i in range(1, n):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    if len(trs) < period: return 0.0
    return sum(trs[-period:])/period

def _atr_pct(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    a = _atr(highs, lows, closes, period)
    c = closes[-1] if closes else 1.0
    return (a / c) if c != 0 else 0.0

def _fmt_price(a: float) -> str:
    if a == 0: return "0"
    if abs(a) >= 1: return f"{a:.4f}".rstrip("0").rstrip(".")
    if abs(a) >= 0.1: return f"{a:.5f}".rstrip("0").rstrip(".")
    return f"{a:.8f}".rstrip("0").rstrip(".")

def _tf_human(tf: str) -> str:
    return tf.replace("m", "м")

def _tv_url_for_symbol(symbol: str) -> Tuple[str, str]:
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    tv_symbol = f"BINANCE:{base}{TV_FUT_SUFFIX}"
    url = f"https://www.tradingview.com/chart/{TV_LAYOUT_ID}/?symbol={urllib.parse.quote(tv_symbol)}" if TV_LAYOUT_ID else f"https://www.tradingview.com/chart/?symbol={urllib.parse.quote(tv_symbol)}"
    pair_slash = f"{base}/USDT"
    return url, pair_slash

# ====== REST и утилиты ======
async def _fetch_ticker_24h(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    r = await client.get(FUT_TICKER_24H_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []

async def _get_universe(client: httpx.AsyncClient, min_vol_usdt: float, max_symbols: int, min_price: float) -> List[str]:
    data = await _fetch_ticker_24h(client)
    items: List[Tuple[str, float]] = []
    for t in data:
        sym = str(t.get("symbol") or "")
        if not sym.endswith("USDT"):
            continue
        try:
            qv = float(t.get("quoteVolume") or 0)
            last = float(t.get("lastPrice") or 0)
        except Exception:
            qv, last = 0.0, 0.0
        if qv >= min_vol_usdt and last >= min_price:
            items.append((sym, qv))
    items.sort(key=lambda x: x[1], reverse=True)
    if max_symbols <= 0: return [s for s,_ in items]
    return [s for s,_ in items[:max(1, max_symbols)]]

async def _fetch_klines(client: httpx.AsyncClient, symbol: str, tf: str, limit: int = 300) -> List[List[Any]]:
    params = {"symbol": symbol, "interval": tf, "limit": limit}
    r = await client.get(FUT_KLINES_URL, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def _klines_to_ohlc(klines: List[List[Any]]) -> Tuple[List[int], List[float], List[float], List[float], List[float]]:
    if not klines or not isinstance(klines, list):
        return [], [], [], [], []
    try:
        times, opens, highs, lows, closes = [], [], [], [], []
        for k in klines:
            times.append(int(k[0]))
            opens.append(float(k[1])); highs.append(float(k[2])); lows.append(float(k[3])); closes.append(float(k[4]))
        return times, opens, highs, lows, closes
    except Exception:
        return [], [], [], [], []

def _klines_to_ohlcv(klines: List[List[Any]]) -> Tuple[List[int], List[float], List[float], List[float], List[float], List[float]]:
    if not klines or not isinstance(klines, list):
        return [], [], [], [], [], []
    try:
        times, opens, highs, lows, closes, vols = [], [], [], [], [], []
        for k in klines:
            times.append(int(k[0]))
            opens.append(float(k[1])); highs.append(float(k[2])); lows.append(float(k[3])); closes.append(float(k[4])); vols.append(float(k[5] or 0.0))
        return times, opens, highs, lows, closes, vols
    except Exception:
        return [], [], [], [], [], []

# ====== BOS (расширенный) ======
def _bos_detect_ext(
    o: List[float], h: List[float], l: List[float], c: List[float], v: List[float],
    swing_left: int, swing_right: int, margin_pct: float,
    confirm_close: bool, min_body_ratio: float, vol_mult: float
) -> Tuple[str, Optional[float], Dict[str, Any]]:
    info: Dict[str, Any] = {}
    n = len(c)
    if n < max(20, swing_left + swing_right + 5):
        return "none", None, info
    sh, sl = _detect_pivots(h, l, swing_left, swing_right)
    last_sh = _last_true(sh)
    last_sl = _last_true(sl)
    i = n - 1

    # объём и импульс последней свечи
    rng = max(h[i]-l[i], 1e-12)
    body = abs(c[i]-o[i])
    impulse = body / rng
    medv = _median(v[-20:]) if len(v) >= 20 else _median(v)
    vol_ratio = (v[i]/medv) if medv > 0 else 0.0
    info.update({"impulse_ratio": impulse, "vol_ratio": vol_ratio})

    # пороги
    if impulse < min_body_ratio or vol_ratio < vol_mult:
        return "none", None, info

    # проверка BOS вверх/вниз
    if last_sh is not None:
        level_up = h[last_sh]*(1.0+max(0.0, margin_pct))
        cond = (c[i] if confirm_close else h[i]) > level_up
        if cond:
            return "bullish", float(h[last_sh]), info
    if last_sl is not None:
        level_dn = l[last_sl]*(1.0-max(0.0, margin_pct))
        cond = (c[i] if confirm_close else l[i]) < level_dn
        if cond:
            return "bearish", float(l[last_sl]), info

    return "none", None, info

# ====== Снятие ликвидности ======
def _detect_liquidity_sweep(
    t: List[int], o: List[float], h: List[float], l: List[float], c: List[float], v: List[float], la: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    if len(c) < 60: return "none", {}
    left = int(la["lookback_swings"]); right = left
    sh, sl = _detect_pivots(h, l, left, right)
    sh_i = _last_true(sh); sl_i = _last_true(sl)
    i = len(c)-1
    med = _median(v[-20:]) if len(v) >= 20 else _median(v); vol_ratio = (v[i]/med) if med>0 else 0.0
    margin = float(la["sweep_margin_pct"]); must_back = bool(la["must_close_back"]); wick_min = float(la["min_wick_ratio"])
    # sweep highs
    if sh_i is not None:
        level = h[sh_i]*(1.0+margin)
        swept = h[i] > level; close_back = c[i] < h[sh_i]
        rng = max(h[i]-l[i], 1e-12); upper_wick = max(0.0, h[i]-max(o[i], c[i])); wr = upper_wick/rng
        if swept and (not must_back or close_back) and wr >= wick_min and vol_ratio >= float(la["vol_mult"]):
            return "sweep_highs", {"swing_level": h[sh_i], "wick_ratio": wr, "vol_ratio": vol_ratio}
    # sweep lows
    if sl_i is not None:
        level = l[sl_i]*(1.0-margin)
        swept = l[i] < level; close_back = c[i] > l[sl_i]
        rng = max(h[i]-l[i], 1e-12); lower_wick = max(0.0, min(o[i], c[i]) - l[i]); wr = lower_wick/rng
        if swept and (not must_back or close_back) and wr >= wick_min and vol_ratio >= float(la["vol_mult"]):
            return "sweep_lows", {"swing_level": l[sl_i], "wick_ratio": wr, "vol_ratio": vol_ratio}
    return "none", {}

# ====== Тренд (15m) ======
def _trend_eval_15m(highs: List[float], lows: List[float], closes: List[float], ema_fast: int, ema_slow: int, adx_min: float) -> str:
    efast = _ema_last_pair(closes, ema_fast); eslow = _ema_last_pair(closes, ema_slow)
    if not efast or not eslow: return "none"
    efp, ef = efast; esp, es = eslow
    adx = _adx(highs, lows, closes, 14)
    if ef > es and ef >= efp and es >= esp and adx >= adx_min: return "bullish"
    if ef < es and ef <= efp and es <= esp and adx >= adx_min: return "bearish"
    return "none"

def _trend_main_detect(h: List[float], l: List[float], c: List[float], ta: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    info: Dict[str, Any] = {}
    if len(c) < 220: return "none", info
    e50p = _ema_last_pair(c, 50); e200p = _ema_last_pair(c, 200)
    if not e50p or not e200p: return "none", info
    e50_prev, e50 = e50p; e200_prev, e200 = e200p
    adx = _adx(h, l, c, 14); atr = _atr(h, l, c, 14)
    if atr <= 0: return "none", info
    atrp = (atr / max(c[-1], 1e-12))
    dist = abs(c[-1]-e200)/atr
    if not (float(ta["atr_pct_min"]) <= atrp <= float(ta["atr_pct_max"])): return "none", info
    if dist > float(ta["ema200_distance_max_atr"]): return "none", info
    info.update({"adx": adx, "atr_pct": atrp, "dist_atr": dist, "confirm_bars": int(ta["confirm_bars"]), "close_buffer_atr": float(ta["close_buffer_atr"])})
    def _held(up: bool) -> bool:
        cb = int(ta["confirm_bars"]); buf = float(ta["close_buffer_atr"])
        for i in range(1, cb+1):
            px = c[-i]
            if up:
                if px < e200 + buf*atr: return False
            else:
                if px > e200 - buf*atr: return False
        return True
    if adx >= float(ta["adx_min"]):
        if (e50 > e200) and (e50 >= e50_prev) and (e200 >= e200_prev) and _held(True):
            return "bullish", info
        if (e50 < e200) and (e50 <= e50_prev) and (e200 <= e200_prev) and _held(False):
            return "bearish", info
    return "none", info

def _aux_bos_confirm(o: List[float], h: List[float], l: List[float], c: List[float], v: List[float], direction: str, ta: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    out: Dict[str, Any] = {}; margin = float(ta["bos_margin_pct"])
    if len(c) < 50: return False, out
    sh, sl = _detect_pivots(h, l, 4, 4); sh_i = _last_true(sh); sl_i = _last_true(sl)
    i = len(c)-1
    if direction == "bullish" and sh_i is not None:
        level = h[sh_i]*(1.0+margin)
        if c[i] <= level: return False, out
    if direction == "bearish" and sl_i is not None:
        level = l[sl_i]*(1.0-margin)
        if c[i] >= level: return False, out
    body = abs(c[i]-o[i]); rng = max(h[i]-l[i], 1e-12); impulse = body/rng
    med = _median(v[-20:]) if len(v) >= 20 else _median(v); vol_ratio = (v[i]/med) if med>0 else 0.0
    if impulse < float(ta["impulse_body_ratio"]) or vol_ratio < float(ta["vol_mult"]): return False, out
    out.update({"impulse_ratio": impulse, "vol_ratio": vol_ratio})
    return True, out

# ====== Отправка сообщений ======
async def _send_bos_alert(context: ContextTypes.DEFAULT_TYPE, chat_id: int, symbol: str, tf: str, direction: str, level: float, info: Dict[str, Any]):
    tv_url, pair_slash = _tv_url_for_symbol(symbol)
    dir_ru = "⬆️ Вверх (BULLISH)" if direction == "bullish" else "⬇️ Вниз (BEARISH)"
    lvl = _fmt_price(level)
    extras = f"\nИмпульс: {info.get('impulse_ratio',0):.2f} | Объём: {info.get('vol_ratio',0):.2f}×"
    text = f"⚡️ <b>Слом структуры (BOS)</b>\nМонета: <b>{symbol}</b>\nТФ: <b>{_tf_human(tf)}</b>\nНаправление: <b>{dir_ru}</b>\nУровень BOS: <b>{lvl}</b>{extras}"
    try:
        try:
            copy_btn = InlineKeyboardButton(pair_slash, copy_text=pair_slash)
        except TypeError:
            copy_btn = InlineKeyboardButton(pair_slash, callback_data=f"ob:copy_pair:{pair_slash}")
        kb = InlineKeyboardMarkup([[copy_btn, InlineKeyboardButton("График", url=tv_url)]])
        await context.bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")
        _log(context, logging.INFO, "BOS sent", chat_id=chat_id, symbol=symbol, tf=tf, dir=direction)
    except Exception as e:
        _log(context, logging.ERROR, "BOS send failed", chat_id=chat_id, err=str(e))

async def _send_liquidity_alert(context: ContextTypes.DEFAULT_TYPE, chat_id: int, symbol: str, tf: str, kind: str, info: Dict[str, Any]):
    tv_url, pair_slash = _tv_url_for_symbol(symbol)
    kind_ru = "Снятие ликвидности ВЫШЕ" if kind == "sweep_highs" else "Снятие ликвидности НИЖЕ"
    lvl = _fmt_price(float(info.get("swing_level", 0.0)))
    text = f"💧 {kind_ru}\n{symbol} {_tf_human(tf)} | Свинг: {lvl}\nWick: {info.get('wick_ratio',0):.2f} | Объём: {info.get('vol_ratio',0):.2f}×"
    try:
        try:
            copy_btn = InlineKeyboardButton(pair_slash, copy_text=pair_slash)
        except TypeError:
            copy_btn = InlineKeyboardButton(pair_slash, callback_data=f"ob:copy_pair:{pair_slash}")
        kb = InlineKeyboardMarkup([[copy_btn, InlineKeyboardButton("График", url=tv_url)]])
        await context.bot.send_message(chat_id, text, reply_markup=kb)
        _log(context, logging.INFO, "LIQ sent", chat_id=chat_id, symbol=symbol, kind=kind)
    except Exception as e:
        _log(context, logging.ERROR, "LIQ send failed", chat_id=chat_id, err=str(e))

async def notify_trend(context: ContextTypes.DEFAULT_TYPE, chat_id: int, symbol: str, direction: str, tf="15m"):
    txt = f"📊 <b>Смена тренда!</b>\nМонета: <b>{symbol}</b>\nТаймфрейм: <b>{tf}</b>\nНаправление: <b>{'⬆️ Вверх' if direction == 'bullish' else '⬇️ Вниз'}</b>"
    await context.bot.send_message(chat_id, txt, parse_mode="HTML")

async def _send_trend_alert(context: ContextTypes.DEFAULT_TYPE, chat_id: int, symbol: str, tf_main: str, tf_aux: str, direction: str, info: Dict[str, Any]):
    tv_url, pair_slash = _tv_url_for_symbol(symbol)
    dir_ru = "БЫЧИЙ" if direction == "bullish" else "МЕДВЕЖИЙ"
    lines = [
        f"🔔 Смена тренда: {symbol} → {dir_ru} ({_tf_human(tf_main)})",
        f"EMA50{'>' if direction=='bullish' else '<'}EMA200, ADX {info.get('adx',0):.1f}",
        f"Удержание над/под EMA200: {info.get('confirm_bars',0)} бар(а) с буфером {info.get('close_buffer_atr',0):.2f}×ATR",
        f"ATR%: {info.get('atr_pct',0.0)*100:.2f}% | Дист. до EMA200: {info.get('dist_atr',0.0):.2f}×ATR",
    ]
    if info.get("mtf_ok"):
        lines.append(f"BOS на {_tf_human(tf_aux)}: импульс {info.get('impulse_ratio',0):.2f}, объём {info.get('vol_ratio',0):.2f}×")
    text = "\n".join(lines)
    await notify_trend(context, chat_id, symbol, direction)
    try:
        try:
            copy_btn = InlineKeyboardButton(pair_slash, copy_text=pair_slash)
        except TypeError:
            copy_btn = InlineKeyboardButton(pair_slash, callback_data=f"ob:copy_pair:{pair_slash}")
        kb = InlineKeyboardMarkup([[copy_btn, InlineKeyboardButton("График", url=tv_url)]])
        await context.bot.send_message(chat_id, text, reply_markup=kb)
        _log(context, logging.INFO, "TA sent", chat_id=chat_id, symbol=symbol, dir=direction)
    except Exception as e:
        _log(context, logging.ERROR, "TA send failed", chat_id=chat_id, err=str(e))

# ====== Основной скан ======
async def _scan_once(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_settings: Dict[int, Dict[str, Any]]):
    s = user_settings.get(chat_id, {})
    obset = s.get("obscan") or {}
    if not obset.get("enabled"): return

    # Общие
    active_tfs = set([tf for tf in obset.get("tfs", ["5m","15m"]) if tf in SUPPORTED_TFS]) or {"5m","15m"}
    min_vol = float(obset.get("min_vol_usdt", 20_000_000))
    max_symbols = int(obset.get("max_symbols", 40))
    min_price = float(obset.get("min_price", 0.02))
    sleep_ms = int(obset.get("sleep_ms", 100))

    # Модули
    bos_cfg = obset.get("bos") or {}
    bos_enabled = bool(bos_cfg.get("enabled", True))
    bos_tfs = set([tf for tf in bos_cfg.get("tfs", ["5m","15m"]) if tf in SUPPORTED_TFS])
    bos_tfs = bos_tfs & active_tfs

    liq_cfg = obset.get("liquidity_alert") or {}
    liq_enabled = bool(liq_cfg.get("enabled", True))
    liq_tf = str(liq_cfg.get("tf", "5m"))
    if liq_tf not in SUPPORTED_TFS: liq_tf = "5m"

    ta_cfg = obset.get("trend_alert") or {}
    ta_enabled = bool(ta_cfg.get("enabled", True))
    ta_cd = int(obset.get("cooldown_ta_min", 120)) * 60

    # Состояние
    state   = context.application.bot_data.setdefault("lean_state", {})  # type: ignore[assignment]
    st_chat = state.setdefault(chat_id, {})
    ta_state = st_chat.setdefault("ta_state", {})     # symbol -> {"dir":..., "ts":...}
    liq_last = st_chat.setdefault("liq_last", {})     # (symbol, kind) -> ts
    bos_last = st_chat.setdefault("bos_last", {})     # (symbol, tf, dir, level) -> ts

    bos_cd = int(bos_cfg.get("cooldown_sec", 180))
    liq_cd = int(liq_cfg.get("cooldown_sec", 900))

    async with httpx.AsyncClient() as client:
        # Универс
        try:
            symbols = await _get_universe(client, min_vol, max_symbols, min_price)
        except Exception as e:
            _log(context, logging.ERROR, "universe failed", chat_id=chat_id, err=str(e)); return
        if not symbols:
            _log(context, logging.INFO, "no symbols by filters", chat_id=chat_id); return

        for symbol in symbols:
            try:
                # 5m данные при необходимости
                need5 = ("5m" in active_tfs) or ("5m" in bos_tfs) or (liq_enabled and liq_tf=="5m") or (ta_enabled and ta_cfg.get("mtf_required", True))
                kl5  = await _fetch_klines(client, symbol, "5m", limit=300) if need5 else []
                t5,o5,h5,l5,c5,v5 = _klines_to_ohlcv(kl5) if kl5 else ([],[],[],[],[],[])

                # 15m данные при необходимости
                need15 = ("15m" in active_tfs) or ("15m" in bos_tfs) or ta_enabled or (liq_enabled and liq_tf=="15m")
                kl15 = await _fetch_klines(client, symbol, "15m", limit=500) if need15 else []
                t15,o15,h15,l15,c15 = _klines_to_ohlc(kl15) if kl15 else ([],[],[],[],[])

                # 1) Тренд‑алерт (15m) + гистерезис
                trend_dir_main = "none"
                if ta_enabled and c15:
                    direction, info = _trend_main_detect(h15, l15, c15, ta_cfg)
                    trend_dir_main = direction if direction in ("bullish","bearish") else "none"
                    if direction in ("bullish","bearish"):
                        # MTF подтверждение
                        mtf_ok = True
                        if bool(ta_cfg.get("mtf_required", True)):
                            if not c5:
                                mtf_ok = False
                            else:
                                ok, add = _aux_bos_confirm(o5,h5,l5,c5,v5,direction,ta_cfg)
                                mtf_ok = ok; info.update(add or {})
                        info["mtf_ok"] = mtf_ok

                        # Кулдаун + гистерезис по барам 15m
                        st = ta_state.get(symbol, {})
                        st_dir = st.get("dir"); st_ts  = float(st.get("ts", 0))
                        now_ts = time.time()
                        hysteresis_bars = int(ta_cfg.get("hysteresis_bars", 2))
                        hysteresis_sec = hysteresis_bars * 15 * 60
                        can_send = mtf_ok and (direction != st_dir or (now_ts - st_ts) >= max(ta_cd, hysteresis_sec))
                        if can_send:
                            await _send_trend_alert(context, chat_id, symbol, "15m", "5m", direction, info)
                            ta_state[symbol] = {"dir": direction, "ts": now_ts}

                # 2) Снятие ликвидности
                if liq_enabled:
                    # Выбор ТФ
                    if liq_tf == "5m" and c5:
                        kind, li = _detect_liquidity_sweep(t5,o5,h5,l5,c5,v5, liq_cfg)
                        tf_label = "5m"
                        # тренд‑фильтр (против тренда, если включен)
                        if kind != "none" and trend_dir_main != "none" and bool(liq_cfg.get("align_against_trend", False)):
                            if (kind == "sweep_highs" and trend_dir_main != "bearish") or (kind == "sweep_lows" and trend_dir_main != "bullish"):
                                kind = "none"
                        # доп. подтверждение BOS после свипа (опционально)
                        if kind != "none" and bool(liq_cfg.get("require_bos_post_confirm", False)):
                            dir_bos, lvl_bos, _ = _bos_detect_ext(o5,h5,l5,c5,v5,4,4, float(ta_cfg.get("bos_margin_pct", 0.0015)), True, 0.40, 1.0)
                            need = "bearish" if kind=="sweep_highs" else "bullish"
                            if dir_bos != need:
                                kind = "none"
                        if kind != "none":
                            key = (symbol, kind)
                            last = float(liq_last.get(key, 0))
                            if (time.time()-last) >= liq_cd:
                                await _send_liquidity_alert(context, chat_id, symbol, tf_label, kind, li)
                                liq_last[key] = time.time()

                    elif liq_tf == "15m" and c15:
                        # Для 15m свипов можно реализовать аналогично (объёмы недоступны в той же выборке) — пропускаем
                        pass

                # 3) BOS‑алерты на выбранных ТФ
                if bos_enabled:
                    def do_bos(tf: str, o: List[float], h: List[float], l: List[float], c: List[float], v: List[float]):
                        if not c: return
                        dir_, lvl, info = _bos_detect_ext(
                            o,h,l,c,v,
                            int(bos_cfg.get("swing_left",4)), int(bos_cfg.get("swing_right",4)),
                            float(bos_cfg.get("margin_pct",0.0015)),
                            bool(bos_cfg.get("confirm_close", True)),
                            float(bos_cfg.get("min_body_ratio",0.55)),
                            float(bos_cfg.get("vol_mult",1.3))
                        )
                        if dir_ == "none" or lvl is None: return
                        # тренд‑фильтр по желанию
                        if bool(bos_cfg.get("align_with_trend", False)) and trend_dir_main != "none":
                            if (dir_ == "bullish" and trend_dir_main != "bullish") or (dir_ == "bearish" and trend_dir_main != "bearish"):
                                return
                        key = (symbol, tf, dir_, round(float(lvl), 6))
                        last = float(bos_last.get(key, 0))
                        if (time.time()-last) >= bos_cd:
                            asyncio.create_task(_send_bos_alert(context, chat_id, symbol, tf, dir_, float(lvl), info))
                            bos_last[key] = time.time()

                    if "5m" in bos_tfs and o5:   do_bos("5m",  o5,h5,l5,c5,v5)
                    if "15m" in bos_tfs and o15: do_bos("15m", o15,h15,l15,c15, [1.0]*len(c15))  # нет v15 — прокидываем заглушку

            except Exception as e:
                _log(context, logging.DEBUG, "scan symbol failed", symbol=symbol, err=str(e))
            await asyncio.sleep(sleep_ms/1000.0)

# ====== Планировщик ======
async def _job(context: ContextTypes.DEFAULT_TYPE, user_settings: Dict[int, Dict[str, Any]], has_access):
    last_runs = context.application.bot_data.setdefault("lean_last_run", {})  # type: ignore[assignment]
    now = time.time()
    for chat_id, _ in list(user_settings.items()):
        try:
            if not has_access(chat_id): continue
            ob = _ensure_obscan(user_settings, chat_id)
            if not ob.get("enabled"): continue
            interval = int(ob.get("interval_sec", 60))
            lr = float(last_runs.get(chat_id, 0))
            if (now - lr) < interval: continue
            t0 = time.time()
            await _scan_once(context, chat_id, user_settings)
            last_runs[chat_id] = time.time()
            _set_metric(context, chat_id, last_duration=round(time.time()-t0,2))
        except Exception as e:
            _log(context, logging.ERROR, "job error", chat_id=chat_id, err=str(e))

# ====== Панель ======
def _ensure_obscan(user_settings: Dict[int, Dict[str, Any]], chat_id: int) -> Dict[str, Any]:
    s = user_settings.setdefault(chat_id, {})
    ob = s.setdefault("obscan", _default_obscan())
    # Санитизация
    ob["tfs"] = [tf for tf in ob.get("tfs", []) if tf in SUPPORTED_TFS] or ["5m","15m"]
    ob["interval_sec"] = max(15, int(ob.get("interval_sec", 60) or 60))
    ob["sleep_ms"] = max(20, int(ob.get("sleep_ms", 100) or 100))
    ob["min_vol_usdt"] = float(ob.get("min_vol_usdt", 20_000_000) or 20_000_000)
    ob["max_symbols"] = max(1, int(ob.get("max_symbols", 40) or 40))
    ob["min_price"] = float(ob.get("min_price", 0.02) or 0.02)
    ob["cooldown_ta_min"] = max(30, int(ob.get("cooldown_ta_min", 120) or 120))

    bos = ob.get("bos") or {}
    bos.setdefault("enabled", True)
    bos["tfs"] = [tf for tf in bos.get("tfs", ["5m","15m"]) if tf in SUPPORTED_TFS] or ["5m","15m"]
    bos.setdefault("swing_left", 4)
    bos.setdefault("swing_right", 4)
    bos.setdefault("margin_pct", 0.0015)
    bos.setdefault("confirm_close", True)
    bos.setdefault("min_body_ratio", 0.55)
    bos.setdefault("vol_mult", 1.3)
    bos.setdefault("align_with_trend", False)
    bos.setdefault("cooldown_sec", 180)
    ob["bos"] = bos

    la = ob.get("liquidity_alert") or {}
    la.setdefault("enabled", True)
    la["tf"] = la.get("tf") if la.get("tf") in SUPPORTED_TFS else "5m"
    la.setdefault("lookback_swings", 4)
    la.setdefault("sweep_margin_pct", 0.0015)
    la.setdefault("min_wick_ratio", 0.60)
    la.setdefault("must_close_back", True)
    la.setdefault("vol_mult", 1.5)
    la.setdefault("align_against_trend", False)
    la.setdefault("require_bos_post_confirm", False)
    la.setdefault("cooldown_sec", 900)
    ob["liquidity_alert"] = la

    ta = ob.get("trend_alert") or {}
    ta.setdefault("enabled", True)
    ta.setdefault("tf_main", "15m")
    ta.setdefault("tf_aux", "5m")
    ta.setdefault("confirm_bars", 2)
    ta.setdefault("close_buffer_atr", 0.10)
    ta.setdefault("adx_min", 25.0)
    ta.setdefault("bos_margin_pct", 0.0015)
    ta.setdefault("impulse_body_ratio", 0.60)
    ta.setdefault("vol_mult", 1.5)
    ta.setdefault("ema200_distance_max_atr", 1.5)
    ta.setdefault("atr_pct_min", 0.003)
    ta.setdefault("atr_pct_max", 0.025)
    ta.setdefault("mtf_required", True)
    ta.setdefault("hysteresis_bars", 2)
    ob["trend_alert"] = ta

    return ob

def _status_text(ob: Dict[str, Any]) -> str:
    tfs = ", ".join(_tf_human(tf) for tf in ob.get("tfs", []))
    bos = ob["bos"]; la = ob["liquidity_alert"]; ta = ob["trend_alert"]
    modules = f"⚡️ BOS: {'ВКЛ' if bos['enabled'] else 'ВЫКЛ'} | 💧 Sweep: {'ВКЛ' if la['enabled'] else 'ВЫКЛ'} | 📊 Trend: {'ВКЛ' if ta['enabled'] else 'ВЫКЛ'}"
    cds = f"CD: BOS={bos.get('cooldown_sec')}с | LIQ={int(la.get('cooldown_sec',900)/60)}м | TA={ob.get('cooldown_ta_min')}м"
    bos_line = f"BOS: TF={','.join(bos.get('tfs',[]))} | марж={bos['margin_pct']*100:.2f}% | имп={bos['min_body_ratio']:.2f} | vol×{bos['vol_mult']:.2f} | тренд={'ДА' if bos['align_with_trend'] else 'НЕТ'}"
    liq_line = f"Sweep: TF={_tf_human(la['tf'])} | свинг={la['lookback_swings']} | марж={la['sweep_margin_pct']*100:.2f}% | wick≥{la['min_wick_ratio']:.2f} | back={'ДА' if la['must_close_back'] else 'НЕТ'} | vol×{la['vol_mult']:.2f}"
    trend_line = f"Trend: 15м | ADX≥{ta['adx_min']:.0f} | ATR% {ta['atr_pct_min']*100:.2f}…{ta['atr_pct_max']*100:.2f} | MTF={'ДА' if ta['mtf_required'] else 'НЕТ'} | hyst={ta['hysteresis_bars']} бар"
    return (
        f"🔥 Lean‑сканер: {'ВКЛ' if ob.get('enabled') else 'ВЫКЛ'} (Binance Futures)\n"
        f"ТФ: {tfs}\n"
        f"Универс: Мин. объём 24ч {int(ob.get('min_vol_usdt'))} | Макс. монет {ob.get('max_symbols')} | Мин. цена {ob.get('min_price')}\n"
        f"Интервал: {ob.get('interval_sec')}с | Sleep: {ob.get('sleep_ms')}мс\n"
        f"{modules}\n{cds}\n{bos_line}\n{liq_line}\n{trend_line}"
    )

def _build_panel(ob: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    # Сканер
    rows.append([InlineKeyboardButton(f"🔥 Сканер: {'ВКЛ' if ob.get('enabled') else 'ВЫКЛ'}", callback_data="ob:toggle")])
    # Модули
    rows.append([
        InlineKeyboardButton(f"⚡️ BOS: {'ВКЛ' if ob['bos']['enabled'] else 'ВЫКЛ'}", callback_data="ob:mod:bos"),
        InlineKeyboardButton(f"💧 Sweep: {'ВКЛ' if ob['liquidity_alert']['enabled'] else 'ВЫКЛ'}", callback_data="ob:mod:liq"),
        InlineKeyboardButton(f"📊 Trend: {'ВКЛ' if ob['trend_alert']['enabled'] else 'ВЫКЛ'}", callback_data="ob:mod:ta"),
    ])
    # BOS пресеты/тогглы
    rows.append([
        InlineKeyboardButton("BOS Строже", callback_data="ob:bos:preset:strict"),
        InlineKeyboardButton("BOS Мягче", callback_data="ob:bos:preset:soft"),
        InlineKeyboardButton(f"BOS Тренд: {'ДА' if ob['bos']['align_with_trend'] else 'НЕТ'}", callback_data="ob:bos:align"),
    ])
    # LIQ пресеты/тогглы
    rows.append([
        InlineKeyboardButton("LIQ Строже", callback_data="ob:liq:preset:strict"),
        InlineKeyboardButton("LIQ Мягче", callback_data="ob:liq:preset:soft"),
        InlineKeyboardButton(f"Против тренда: {'ДА' if ob['liquidity_alert']['align_against_trend'] else 'НЕТ'}", callback_data="ob:liq:align"),
    ])
    rows.append([
        InlineKeyboardButton(f"Требовать BOS: {'ДА' if ob['liquidity_alert']['require_bos_post_confirm'] else 'НЕТ'}", callback_data="ob:liq:reqbos"),
        InlineKeyboardButton(f"TF Sweep: {_tf_human(ob['liquidity_alert']['tf'])}", callback_data="ob:liq:tf"),
    ])
    # Trend быстрые тумблеры
    rows.append([
        InlineKeyboardButton(f"Trend MTF: {'ДА' if ob['trend_alert']['mtf_required'] else 'НЕТ'}", callback_data="ob:ta:mtf"),
        InlineKeyboardButton(f"Hyst: {ob['trend_alert']['hysteresis_bars']} б", callback_data="ob:ta:hyst"),
    ])
    # Таймфреймы
    tfs = set(ob.get("tfs", []))
    def tf_btn(tf: str) -> InlineKeyboardButton:
        mark = "✅" if tf in tfs else "⚪️"
        return InlineKeyboardButton(f"{mark} {_tf_human(tf)}", callback_data=f"ob:tf:{tf}")
    rows.append([tf_btn("5m"), tf_btn("15m")])
    # Универс
    rows.append([
        InlineKeyboardButton("Мин. объём −5M", callback_data="ob:minvol:-5000000"),
        InlineKeyboardButton("+5M", callback_data="ob:minvol:+5000000"),
        InlineKeyboardButton("20M", callback_data="ob:minvol:set:20000000"),
    ])
    rows.append([
        InlineKeyboardButton("Макс. монет −10", callback_data="ob:max:-10"),
        InlineKeyboardButton("+10", callback_data="ob:max:+10"),
    ])
    rows.append([
        InlineKeyboardButton("Мин. цена −0.01", callback_data="ob:minprice:-0.01"),
        InlineKeyboardButton("+0.01", callback_data="ob:minprice:+0.01"),
    ])
    # Интервал/слип
    rows.append([
        InlineKeyboardButton("Интервал 60с", callback_data="ob:int:set:60"),
        InlineKeyboardButton("90с", callback_data="ob:int:set:90"),
    ])
    rows.append([
        InlineKeyboardButton("Sleep 80мс", callback_data="ob:sleep:set:80"),
        InlineKeyboardButton("100мс", callback_data="ob:sleep:set:100"),
        InlineKeyboardButton("120мс", callback_data="ob:sleep:set:120"),
    ])
    # Кулдауны
    rows.append([
        InlineKeyboardButton(f"CD BOS {ob['bos']['cooldown_sec']}с", callback_data="ob:cd:bos"),
        InlineKeyboardButton(f"LIQ {int(ob['liquidity_alert']['cooldown_sec']/60)}м", callback_data="ob:cd:liq"),
        InlineKeyboardButton(f"TA {ob.get('cooldown_ta_min')}м", callback_data="ob:cd:ta"),
    ])
    # Скрыть
    rows.append([InlineKeyboardButton("Скрыть", callback_data="ob:hide")])
    return InlineKeyboardMarkup(rows)

async def _send_stub(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    stubs = context.application.bot_data.setdefault("lean_stubs", {})  # type: ignore[assignment]
    prev = stubs.get(chat_id)
    if prev:
        with suppress(Exception): await context.bot.delete_message(chat_id=chat_id, message_id=prev)
    try:
        btn = InlineKeyboardButton("Открыть панель", callback_data="ob:open")
        m = await context.bot.send_message(chat_id, "Панель скрыта. Нажмите «Открыть панель».", reply_markup=InlineKeyboardMarkup([[btn]]))
        stubs[chat_id] = m.message_id  # type: ignore[index]
    except Exception: pass

async def _open_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, user_settings: Dict[int, Dict[str, Any]]):
    chat_id = update.effective_chat.id
    panels = context.application.bot_data.setdefault("lean_panels", {})  # type: ignore[assignment]
    stubs  = context.application.bot_data.setdefault("lean_stubs", {})   # type: ignore[assignment]
    # Удаляем старое меню
    prev_id = panels.get(chat_id)
    if prev_id: 
        with suppress(Exception): await context.bot.delete_message(chat_id=chat_id, message_id=prev_id)
        panels.pop(chat_id, None)
    prev_stub = stubs.get(chat_id)
    if prev_stub:
        with suppress(Exception): await context.bot.delete_message(chat_id=chat_id, message_id=prev_stub)
        stubs.pop(chat_id, None)
    with suppress(Exception):
        if update.effective_message and getattr(update.effective_message, "text", "").startswith("/"): await update.effective_message.delete()

    ob = _ensure_obscan(user_settings, chat_id)
    text = _status_text(ob)
    kb = _build_panel(ob)
    m = await context.bot.send_message(chat_id, text, reply_markup=kb)
    panels[chat_id] = m.message_id  # type: ignore[index]

async def _edit_panel(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_settings: Dict[int, Dict[str, Any]]):
    ob = _ensure_obscan(user_settings, chat_id)
    text = _status_text(ob); kb = _build_panel(ob)
    panels = context.application.bot_data.get("lean_panels", {})
    msg_id = panels.get(chat_id)
    if msg_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=kb)
        except Exception:
            m = await context.bot.send_message(chat_id, text, reply_markup=kb)
            panels[chat_id] = m.message_id  # type: ignore[index]
    else:
        m = await context.bot.send_message(chat_id, text, reply_markup=kb)
        panels[chat_id] = m.message_id  # type: ignore[index]

# ====== Колбэки панели ======
async def _on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE, user_settings: Dict[int, Dict[str, Any]], settings_changed_cb):
    q = update.callback_query; chat_id = q.message.chat.id
    ob = _ensure_obscan(user_settings, chat_id)
    data = q.data or ""
    panels = context.application.bot_data.setdefault("lean_panels", {})  # type: ignore[assignment]
    stubs  = context.application.bot_data.setdefault("lean_stubs", {})   # type: ignore[assignment]
    try:
        if data == "ob:open":
            with suppress(Exception): await q.message.delete()
            stubs.pop(chat_id, None)
            await _open_panel(update, context, user_settings); await q.answer("Открыто"); return
        if data == "ob:hide":
            pid = panels.get(chat_id)
            if pid:
                with suppress(Exception): await context.bot.delete_message(chat_id=chat_id, message_id=pid)
                panels.pop(chat_id, None)
            await _send_stub(context, chat_id); await q.answer("Скрыто"); return

        if data == "ob:toggle":
            ob["enabled"] = not ob.get("enabled", False); settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return

        if data.startswith("ob:mod:"):
            mod = data.split(":")[2]
            if mod == "bos": ob["bos"]["enabled"] = not ob["bos"]["enabled"]
            if mod == "liq": ob["liquidity_alert"]["enabled"] = not ob["liquidity_alert"]["enabled"]
            if mod == "ta":  ob["trend_alert"]["enabled"] = not ob["trend_alert"]["enabled"]
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return

        if data.startswith("ob:tf:"):
            tf = data.split(":")[2]
            if tf in SUPPORTED_TFS:
                tfs = set(ob.get("tfs", []))
                if tf in tfs: tfs.remove(tf)
                else: tfs.add(tf)
                ob["tfs"] = [t for t in SUPPORTED_TFS if t in tfs] or [tf]
                settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return

        # BOS настройки
        if data == "ob:bos:align":
            ob["bos"]["align_with_trend"] = not ob["bos"].get("align_with_trend", False)
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("BOS тренд"); return
        if data == "ob:bos:preset:strict":
            ob["bos"]["min_body_ratio"] = 0.60; ob["bos"]["vol_mult"] = 1.7
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("BOS строгий"); return
        if data == "ob:bos:preset:soft":
            ob["bos"]["min_body_ratio"] = 0.45; ob["bos"]["vol_mult"] = 1.2
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("BOS мягкий"); return

        # LIQ настройки
        if data == "ob:liq:align":
            ob["liquidity_alert"]["align_against_trend"] = not ob["liquidity_alert"].get("align_against_trend", False)
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("LIQ тренд"); return
        if data == "ob:liq:reqbos":
            ob["liquidity_alert"]["require_bos_post_confirm"] = not ob["liquidity_alert"].get("require_bos_post_confirm", False)
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("LIQ BOS"); return
        if data == "ob:liq:preset:strict":
            la = ob["liquidity_alert"]; la["min_wick_ratio"] = 0.70; la["vol_mult"] = 1.8; la["must_close_back"] = True
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("LIQ строгий"); return
        if data == "ob:liq:preset:soft":
            la = ob["liquidity_alert"]; la["min_wick_ratio"] = 0.50; la["vol_mult"] = 1.2; la["must_close_back"] = False
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("LIQ мягкий"); return
        if data == "ob:liq:tf":
            la = ob["liquidity_alert"]; la["tf"] = "15m" if la.get("tf","5m")=="5m" else "5m"
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer(f"TF {la['tf']}"); return

        # Trend настройки
        if data == "ob:ta:mtf":
            ob["trend_alert"]["mtf_required"] = not ob["trend_alert"].get("mtf_required", True)
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("TA MTF"); return
        if data == "ob:ta:hyst":
            cur = int(ob["trend_alert"].get("hysteresis_bars", 2))
            nxt = {1:2, 2:3, 3:1}.get(cur, 2)
            ob["trend_alert"]["hysteresis_bars"] = nxt
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer(f"Hyst {nxt}"); return

        # Кулдауны
        if data == "ob:cd:bos":
            cur = int(ob["bos"].get("cooldown_sec", 180)); nxt = {120:180, 180:240, 240:300, 300:120}.get(cur, 180)
            ob["bos"]["cooldown_sec"] = nxt; settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer(f"BOS {nxt}с"); return
        if data == "ob:cd:liq":
            cur = int(ob["liquidity_alert"].get("cooldown_sec", 900)); nxt = {300:600, 600:900, 900:1200, 1200:300}.get(cur, 900)
            ob["liquidity_alert"]["cooldown_sec"] = nxt; settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer(f"LIQ {int(nxt/60)}м"); return
        if data == "ob:cd:ta":
            key = "cooldown_ta_min"; cur = int(ob.get(key, 120)); nxt = {60:90, 90:120, 120:180, 180:240, 240:60}.get(cur, 120)
            ob[key] = nxt; settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer(f"TA {nxt}м"); return

        # Универс
        if data.startswith("ob:minvol:"):
            parts = data.split(":")
            if parts[2] == "set":
                ob["min_vol_usdt"] = float(parts[3])
            else:
                ob["min_vol_usdt"] = max(0.0, float(ob.get("min_vol_usdt",0)) + float(parts[2]))
            settings_changed_cb(chat_id); await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return
        if data.startswith("ob:max:"):
            delta = int(data.split(":")[2]); cur = int(ob.get("max_symbols",40))
            ob["max_symbols"] = max(1, cur + delta); settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return
        if data.startswith("ob:minprice:"):
            delta = float(data.split(":")[2]); cur = float(ob.get("min_price",0.02))
            ob["min_price"] = max(0.0, round(cur + delta, 4)); settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return

        # Интервал/слип
        if data.startswith("ob:int:set:"):
            ob["interval_sec"] = int(data.split(":")[3]); settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return
        if data.startswith("ob:sleep:set:"):
            ob["sleep_ms"] = int(data.split(":")[3]); settings_changed_cb(chat_id)
            await _edit_panel(context, chat_id, user_settings); await q.answer("OK"); return

        if data.startswith("ob:copy_pair:"):
            pair = data.split(":", 2)[2]
            await q.answer(f"{pair}", show_alert=False); return

        # Закрыть (фолбэк)
        if data == "ob:close":
            with suppress(Exception): await q.message.delete()
            context.application.bot_data.get("lean_panels", {}).pop(chat_id, None)
            await q.answer("Закрыто"); return

    except Exception as e:
        _log(context, logging.ERROR, "callback error", chat_id=chat_id, err=str(e))
        with suppress(Exception): await q.answer("Ошибка")

# ====== Диагностика ======
async def _debug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, user_settings: Dict[int, Dict[str, Any]]):
    chat_id = update.effective_chat.id
    ob = _ensure_obscan(user_settings, chat_id)
    metrics = _get_metric(context, chat_id)
    last_runs = context.application.bot_data.get("lean_last_run", {})
    last_run = float(last_runs.get(chat_id, 0))
    ago = int(time.time() - last_run) if last_run else None
    bos = ob["bos"]; la = ob["liquidity_alert"]; ta = ob["trend_alert"]
    lines = [
        f"enabled: {ob.get('enabled')}",
        f"tfs: {','.join(ob.get('tfs',[]))}",
        f"universe: min_vol={int(ob.get('min_vol_usdt'))} max_symbols={ob.get('max_symbols')} min_price={ob.get('min_price')}",
        f"interval: {ob.get('interval_sec')} sleep_ms: {ob.get('sleep_ms')}",
        f"modules: bos={bos['enabled']} liq={la['enabled']} ta={ta['enabled']}",
        f"cooldowns: bos={bos.get('cooldown_sec')}s liq={la.get('cooldown_sec')}s ta={ob.get('cooldown_ta_min')}m",
        f"last_run_sec_ago: {ago}",
        f"metrics: {metrics or '{}'}",
    ]
    await update.effective_message.reply_text("LEAN SCANNER DEBUG\n" + "\n".join(lines))

async def _oblog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ring = list(_ring(context))
    n = 120
    tail = ring[-n:] if len(ring) > n else ring
    if not tail:
        await update.effective_message.reply_text("Лог пуст."); return
    text = "Последние записи лога:\n" + "\n".join(tail)
    if len(text) > 3500: text = "…\n" + text[-3500:]
    await update.effective_message.reply_text(text)

# ====== Регистрация ======
def register_obscan(application: Application, user_settings: Dict[int, Dict[str, Any]], settings_changed_cb, has_access_callable):
    async def open_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await _open_panel(update, context, user_settings)

    async def obdebug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await _debug_cmd(update, context, user_settings)

    async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await _on_cb(update, context, user_settings, settings_changed_cb)

    # Команды
    application.add_handler(CommandHandler("obscan", open_cmd), group=-100)
    application.add_handler(CommandHandler("obdebug", obdebug_cmd), group=-100)
    application.add_handler(CommandHandler("oblog", _oblog_cmd), group=-100)

    # CallbackQueryHandler первым, чтобы не перехватывали другие
    application.add_handler(CallbackQueryHandler(on_cb, pattern=r"^ob:", block=True), group=-100)

    # ---- Новый запуск сканера для каждого пользователя ----

    async def scan_user_job(context, chat_id, user_settings, has_access_callable):
        try:
            if not has_access_callable(chat_id):
                return
            await _scan_once(context, chat_id, user_settings)
        except Exception as e:
            _log(context, logging.ERROR, "scan_user_job error", chat_id=chat_id, err=str(e))

    def start_user_scan(context, chat_id, interval_sec, user_settings, has_access_callable):
        scheduler.add_job(
            scan_user_job,
            'interval',
            seconds=interval_sec,
            args=[context, chat_id, user_settings, has_access_callable],
            max_instances=1,
            id=f"user_scan_{chat_id}",
            coalesce=True,
            misfire_grace_time=30
        )

    prev_post_init = application.post_init
    prev_post_shutdown = application.post_shutdown

    async def post_init_wrapper(app: Application):
        if prev_post_init:
            await prev_post_init(app)
        # Запускаем APScheduler
        scheduler.start()
        # Создаем dummy context для задач
        class Dummy:
            def __init__(self, app):
                self.application = app
                self.bot = app.bot
        context = Dummy(app)
        # Запускаем сканеры для всех пользователей, которые есть в user_settings
        for chat_id, cfg in user_settings.items():
            obscan_cfg = cfg.get("obscan", {})
            interval_sec = obscan_cfg.get("interval_sec", 60)
            start_user_scan(context, chat_id, interval_sec, user_settings, has_access_callable)

    async def post_shutdown_wrapper(app: Application):
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        if prev_post_shutdown:
            await prev_post_shutdown(app)

    application.post_init = post_init_wrapper
    application.post_shutdown = post_shutdown_wrapper