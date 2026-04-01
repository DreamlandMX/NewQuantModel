from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ProviderConfig:
    name: str
    market: str
    notes: str


DEFAULT_PROVIDERS = [
    ProviderConfig("coingecko", "crypto", "Universe construction and spot metadata"),
    ProviderConfig("binance_perp", "crypto", "Tradable perpetual market proxies"),
    ProviderConfig("akshare", "cn_equity", "China constituent and price adapters"),
    ProviderConfig("eastmoney", "cn_equity", "Fallback China index and market metadata"),
    ProviderConfig("yahoo", "us_equity", "US prices and benchmark data"),
    ProviderConfig("stooq", "us_equity", "US fallback data and index history"),
]
