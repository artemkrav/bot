import httpx

async def get_binance_symbols(market_type="spot"):
    url = (
        "https://api.binance.com/api/v3/exchangeInfo"
        if market_type == "spot"
        else "https://fapi.binance.com/fapi/v1/exchangeInfo"
    )
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    # Для Binance: символы пары, статус TRADING, quoteAsset USDT
    return {s["symbol"] for s in data.get("symbols", []) if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"}

async def get_bybit_symbols(category="linear"):
    url = f"https://api.bybit.com/v5/market/symbols?category={category}"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    # Для Bybit: только USDT-пары
    return {s["symbol"] for s in data.get("result", {}).get("list", []) if s.get("quoteCoin") == "USDT"}