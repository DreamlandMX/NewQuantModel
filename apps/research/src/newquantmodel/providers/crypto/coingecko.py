from __future__ import annotations

import pandas as pd

from newquantmodel.providers.market.http import get_json


COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
STABLE_IDS = {
    "tether",
    "usd-coin",
    "usds",
    "dai",
    "first-digital-usd",
    "true-usd",
    "ethena-usde",
    "usdd",
    "pax-dollar",
    "binance-usd",
    "paypal-usd",
    "gemini-dollar",
    "frax",
    "liquity-usd",
    "rlusd",
    "usd1-wlfi",
    "bfusd",
    "fdusd",
}
STABLE_SYMBOLS = {
    "USDT",
    "USDC",
    "USDS",
    "DAI",
    "FDUSD",
    "TUSD",
    "USDE",
    "USDD",
    "USDP",
    "BUSD",
    "PYUSD",
    "GUSD",
    "FRAX",
    "LUSD",
    "RLUSD",
    "USD1",
    "BFUSD",
}


def fetch_top_market_cap(limit: int = 50) -> pd.DataFrame:
    per_page = min(max(limit * 2, 50), 250)
    payload = get_json(
        COINGECKO_MARKETS_URL,
        params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": str(per_page),
            "page": "1",
            "sparkline": "false",
            "price_change_percentage": "24h",
        },
        timeout=20,
    )
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["coingecko_id", "symbol", "name", "market_cap_rank", "market_cap", "current_price"])
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame = frame[~frame["id"].isin(STABLE_IDS)]
    frame = frame[~frame["symbol"].isin(STABLE_SYMBOLS)]
    frame = frame.sort_values("market_cap_rank").head(limit)
    return frame.rename(columns={"id": "coingecko_id"})[
        ["coingecko_id", "symbol", "name", "market_cap_rank", "market_cap", "current_price"]
    ].reset_index(drop=True)
