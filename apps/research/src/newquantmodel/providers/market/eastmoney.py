from __future__ import annotations

from datetime import datetime

import pandas as pd

from .http import get_json


EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


def symbol_to_secid(symbol: str) -> str:
    clean = symbol.upper().replace(".SS", ".SH")
    code, suffix = clean.split(".")
    market = "1" if suffix == "SH" else "0"
    return f"{market}.{code}"


def fetch_daily_history(symbol: str, start_date: str) -> pd.DataFrame:
    payload = get_json(
        EASTMONEY_KLINE_URL,
        params={
            "secid": symbol_to_secid(symbol),
            "klt": "101",
            "fqt": "1",
            "beg": start_date.replace("-", ""),
            "end": "20500101",
            "lmt": "10000",
            "iscca": "1",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        },
        timeout=20,
    )
    lines = ((payload or {}).get("data") or {}).get("klines") or []
    rows: list[dict] = []
    for line in lines:
        parts = str(line).split(",")
        if len(parts) < 6:
            continue
        rows.append(
            {
                "timestamp": pd.Timestamp(parts[0]).tz_localize("Asia/Shanghai").tz_convert("UTC"),
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    out = pd.DataFrame(rows)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    return out.sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_intraday_history(symbol: str, start_date: str, interval_minutes: int = 30) -> pd.DataFrame:
    payload = get_json(
        EASTMONEY_KLINE_URL,
        params={
            "secid": symbol_to_secid(symbol),
            "klt": str(interval_minutes),
            "fqt": "1",
            "beg": start_date.replace("-", ""),
            "end": "20500101",
            "lmt": "10000",
            "iscca": "1",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        },
        timeout=20,
    )
    lines = ((payload or {}).get("data") or {}).get("klines") or []
    rows: list[dict] = []
    for line in lines:
        parts = str(line).split(",")
        if len(parts) < 6:
            continue
        rows.append(
            {
                "timestamp": pd.Timestamp(parts[0]).tz_localize("Asia/Shanghai").tz_convert("UTC"),
                "open": float(parts[1]),
                "close": float(parts[2]),
                "high": float(parts[3]),
                "low": float(parts[4]),
                "volume": float(parts[5]),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    out = pd.DataFrame(rows)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    return out.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
