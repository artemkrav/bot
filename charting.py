from io import BytesIO
from typing import Dict
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import pandas as pd

def analyze_extremes(candles_df: pd.DataFrame, lookback: int) -> Dict[str, bool]:
    """
    candles_df: DataFrame с колонками ['open_time','open','high','low','close','volume'].
    Возвращает dict: {'new_high': bool, 'new_low': bool}
    """
    if candles_df is None or len(candles_df) < max(lookback + 1, 2):
        return {"new_high": False, "new_low": False}
    df = candles_df.reset_index(drop=True).copy()
    # Берём последнюю свечу как "текущую"
    last = df.iloc[-1]
    prev = df.iloc[-(lookback+1):-1]
    if prev.empty:
        return {"new_high": False, "new_low": False}
    new_high = last['high'] >= prev['high'].max()
    new_low = last['low'] <= prev['low'].min()
    return {"new_high": bool(new_high), "new_low": bool(new_low)}

def generate_chart_image(candles_df: pd.DataFrame, symbol: str, tf: str, exchange: str, event_line: str = "") -> bytes:
    """
    Строит простой график close, возвращает PNG bytes.
    """
    if candles_df is None or candles_df.empty:
        raise ValueError("Empty candles dataframe")
    df = candles_df.copy()
    # Ожидается колонка 'open_time' в pandas.Timestamp
    if not isinstance(df['open_time'].iloc[0], (pd.Timestamp, )):
        # попытка привести epoch ms/seconds
        try:
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        except Exception:
            df['open_time'] = pd.to_datetime(df['open_time'], utc=True, errors='coerce')
    fig, ax = plt.subplots(figsize=(10, 4), dpi=150)
    ax.plot(df['open_time'], df['close'], color="#2E86C1", linewidth=1.5)
    ax.set_title(f"{symbol} {tf} @ {exchange}", fontsize=12)
    ax.set_xlabel("")
    ax.set_ylabel("Price")
    ax.grid(True, linestyle="--", alpha=0.25)
    # Аннотация последней точки
    last_time = df['open_time'].iloc[-1]
    last_close = df['close'].iloc[-1]
    ax.scatter([last_time], [last_close], color="#D35400", s=18, zorder=5)
    if event_line:
        ax.text(0.99, 0.02, event_line, transform=ax.transAxes, ha="right", va="bottom", color="#D35400", fontsize=10)
    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()