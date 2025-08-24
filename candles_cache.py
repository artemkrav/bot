from __future__ import annotations
import datetime
from typing import Dict, Tuple, Optional, List
import httpx
import pandas as pd
import aiohttp
import requests

# === ДОБАВЛЯЕМ ИМПОРТ pandas_ta ДЛЯ ИНДИКАТОРОВ ===
try:
    import pandas_ta as ta
except ImportError:
    ta = None  # Если нет pandas_ta, индикаторы не будут считаться

# Простой in-memory кэш: key -> (dt_cached, DataFrame)
_cache: Dict[Tuple[str, str, str, str, int], Tuple[datetime.datetime, pd.DataFrame]] = {}

CACHE_TTL_SECONDS = 60

BINANCE_SPOT_KLINES = "https://api.binance.com/api/v3/klines"
BINANCE_FUT_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BYBIT_KLINES = "https://api.bybit.com/v5/market/kline"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INSTRUMENTS_INFO = "https://api.bybit.com/v5/market/instruments-info"

def _binance_symbol(coin: str, market_type: str) -> str:
    return f"{coin.upper()}USDT"

def _bybit_symbol(coin: str, market_type: str) -> str:
    return f"{coin.upper()}USDT"

def _bybit_category(market_type: str) -> str:
    return "spot" if market_type == "spot" else "linear"

def bybit_symbol_allowed(symbol: str, category: str = "linear", min_vol_usdt: float = 10000) -> bool:
    """
    Проверяет, существует ли тикер на Bybit для нужной категории и объём торгов выше заданного порога.
    min_vol_usdt — минимальный объем торгов за 24ч (или 0 для игнорирования).
    """
    url = f"{BYBIT_TICKERS}?category={category}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        for item in data.get("result", {}).get("list", []):
            if item.get("symbol") == symbol:
                if min_vol_usdt > 0:
                    vol = float(item.get("turnover24h", 0))
                    if vol < min_vol_usdt:
                        print(f"[SKIP] {symbol} имеет низкий объём: {vol}")
                        return False
                return True
        return False
    except Exception as e:
        print(f"[DEBUG] Ошибка фильтра по объёму Bybit: {e}")
        return False

def filter_bybit_symbols(symbols: List[str], category: str = "linear", min_vol_usdt: float = 10000) -> List[str]:
    """
    Возвращает только те тикеры, которые есть на Bybit и имеют достаточный объём торгов.
    """
    url = f"{BYBIT_TICKERS}?category={category}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        allowed = set()
        for item in data.get("result", {}).get("list", []):
            sym = item.get("symbol")
            vol = float(item.get("turnover24h", 0))
            if sym in symbols and vol >= min_vol_usdt:
                allowed.add(sym)
        return list(allowed)
    except Exception as e:
        print(f"[DEBUG] Ошибка фильтрации списка Bybit: {e}")
        return []

async def _fetch_binance(exchange: str, market_type: str, coin: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    base_url = BINANCE_SPOT_KLINES if market_type == 'spot' else BINANCE_FUT_KLINES
    sym = _binance_symbol(coin, market_type)
    params = {"symbol": sym, "interval": tf, "limit": limit}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(base_url, params=params)
        if r.status_code != 200:
            print(f"[ERROR] Binance API status: {r.status_code}")
            return None
        data = r.json()
    rows = []
    for k in data:
        rows.append({
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    df = pd.DataFrame(rows)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df = add_indicators(df)
    return df

async def _fetch_bybit(exchange: str, market_type: str, coin: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    """
    Получение свечей с Bybit (spot или linear).
    Возвращает pd.DataFrame или None, если нет свечей.
    """
    category = _bybit_category(market_type)
    symbol = _bybit_symbol(coin, market_type)
    url = f"{BYBIT_KLINES}?category={category}&symbol={symbol}&interval={tf}&limit={limit}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            response = await resp.json()
            print("BYBIT RAW RESPONSE:", response)  # Для отладки

            candles = response.get("result", {}).get("list", [])
            if not candles:
                print(f"[ERROR] Нет свечей для {symbol} {tf} ({category})")
                return None

            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "turnover"
            ])
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.sort_values("timestamp")
            print(f"[DEBUG] BYBIT: {market_type} {coin} {tf} - свечей получено: {len(df)}")
            df = add_indicators(df)
            return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # EMA и RSI считаются по close
    if len(df) == 0:
        df['ema'] = []
        df['rsi'] = []
        return df
    try:
        df['ema'] = df['close'].ewm(span=14, adjust=False).mean()
        if ta is not None:
            df['rsi'] = ta.rsi(df['close'], length=14)
        else:
            df['rsi'] = compute_rsi(df['close'], period=14)
    except Exception as e:
        print(f"[ERROR] Индикаторы EMA/RSI: {e}")
        df['ema'] = None
        df['rsi'] = None
    return df

def compute_rsi(close, period=14):
    # Простейший расчет RSI (если нет pandas_ta)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

async def candle_cache_get(exchange: str, market_type: str, coin: str, tf: str, limit: int, min_vol_usdt: float = 10000) -> Optional[pd.DataFrame]:
    key = (exchange, market_type, coin.upper(), tf, int(limit))
    now = datetime.datetime.utcnow()
    if key in _cache:
        ts, df = _cache[key]
        if (now - ts).total_seconds() <= CACHE_TTL_SECONDS:
            return df
    df = None
    if exchange == 'binance':
        df = await _fetch_binance(exchange, market_type, coin, tf, limit)
    elif exchange == 'bybit':
        symbol = _bybit_symbol(coin, market_type)
        category = _bybit_category(market_type)
        # Улучшенная фильтрация по объёму
        if not bybit_symbol_allowed(symbol, category, min_vol_usdt=min_vol_usdt):
            print(f"[SKIP] Тикер {symbol} не прошёл фильтр по объёму или отсутствует на Bybit ({category})")
            return None
        df = await _fetch_bybit(exchange, market_type, coin, tf, limit)
        if df is not None:
            print(f"[DEBUG] BYBIT: {market_type} {coin} {tf} - свечей получено: {df.shape[0]}")
    else:
        print(f"[ERROR] Неизвестная биржа: {exchange}")
        df = None
    if df is not None:
        _cache[key] = (now, df)
    return df

async def candle_cache_prefetch(exchange: str, market_type: str, coin: str, tf: str, limit: int, min_vol_usdt: float = 10000):
    try:
        await candle_cache_get(exchange, market_type, coin, tf, limit, min_vol_usdt)
    except Exception as e:
        print(f"[ERROR] Prefetch error: {e}")