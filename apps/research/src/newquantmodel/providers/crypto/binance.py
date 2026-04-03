from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from zipfile import ZipFile

import pandas as pd

from newquantmodel.providers.market.http import get_json, get_bytes, head_ok


BINANCE_DATA_API = "https://data-api.binance.vision/api/v3"
BINANCE_ARCHIVE = "https://data.binance.vision/data"
BINANCE_FUTURES_API = "https://fapi.binance.com"


def to_spot_pair(symbol: str) -> str:
    return f"{symbol.upper()}USDT"


def monthly_spot_url(pair: str, year: int, month: int) -> str:
    return f"{BINANCE_ARCHIVE}/spot/monthly/klines/{pair}/1h/{pair}-1h-{year:04d}-{month:02d}.zip"


def daily_spot_url(pair: str, day: date) -> str:
    return f"{BINANCE_ARCHIVE}/spot/daily/klines/{pair}/1h/{pair}-1h-{day:%Y-%m-%d}.zip"


def monthly_futures_url(pair: str, year: int, month: int) -> str:
    return f"{BINANCE_ARCHIVE}/futures/um/monthly/klines/{pair}/1h/{pair}-1h-{year:04d}-{month:02d}.zip"


def recent_spot_symbols() -> set[str]:
    payload = get_json(f"{BINANCE_DATA_API}/exchangeInfo", timeout=20)
    symbols = payload.get("symbols") or []
    return {
        row["symbol"]
        for row in symbols
        if row.get("status") == "TRADING"
        and row.get("quoteAsset") == "USDT"
        and row.get("isSpotTradingAllowed", False)
    }


def has_perpetual_proxy(pair: str, reference_date: date | None = None) -> bool:
    ref = reference_date or (date.today().replace(day=1) - timedelta(days=1))
    return head_ok(monthly_futures_url(pair, ref.year, ref.month))


def _month_starts(start: date, end: date) -> list[date]:
    cursor = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    months: list[date] = []
    while cursor <= last:
        months.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def _parse_zip_rows(payload: bytes) -> pd.DataFrame:
    with ZipFile(BytesIO(payload)) as archive:
        name = archive.namelist()[0]
        with archive.open(name) as handle:
            frame = pd.read_csv(
                handle,
                header=None,
                usecols=[0, 1, 2, 3, 4, 5],
                names=["open_time", "open", "high", "low", "close", "volume"],
            )
    open_time_numeric = pd.to_numeric(frame["open_time"], errors="coerce")
    if open_time_numeric.notna().any():
        max_value = float(open_time_numeric.dropna().abs().max())
        if max_value >= 1e18:
            unit = "ns"
        elif max_value >= 1e15:
            unit = "us"
        elif max_value >= 1e12:
            unit = "ms"
        else:
            unit = "s"
        timestamps = pd.to_datetime(open_time_numeric, unit=unit, utc=True, errors="coerce")
    else:
        timestamps = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")

    frame["timestamp"] = timestamps
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.drop(columns=["open_time"]).dropna().sort_values("timestamp")


def fetch_hourly_history(pair: str, start_date: date, end_date: date) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    month_end = date.today().replace(day=1) - timedelta(days=1)
    for month_start in _month_starts(start_date, month_end):
        url = monthly_spot_url(pair, month_start.year, month_start.month)
        if not head_ok(url):
            continue
        frames.append(_parse_zip_rows(get_bytes(url, timeout=45)))

    current_day = month_end + timedelta(days=1)
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    while current_day <= yesterday:
        url = daily_spot_url(pair, current_day)
        if head_ok(url):
            frames.append(_parse_zip_rows(get_bytes(url, timeout=30)))
        current_day += timedelta(days=1)

    try:
        payload = get_json(
            f"{BINANCE_DATA_API}/klines",
            params={"symbol": pair, "interval": "1h", "limit": "1000"},
            timeout=20,
        )
        recent = pd.DataFrame(payload, columns=[
            "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume",
            "trades", "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        if not recent.empty:
            recent["timestamp"] = pd.to_datetime(recent["open_time"], unit="ms", utc=True)
            for column in ["open", "high", "low", "close", "volume"]:
                recent[column] = pd.to_numeric(recent[column], errors="coerce")
            frames.append(recent[["timestamp", "open", "high", "low", "close", "volume"]].dropna())
    except Exception:
        pass

    if not frames:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    out = out[(out["timestamp"] >= pd.Timestamp(start_date, tz="UTC")) & (out["timestamp"] <= pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1))]
    return out.reset_index(drop=True)


def fetch_futures_hourly_history(pair: str, start_date: date, end_date: date) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    month_end = date.today().replace(day=1) - timedelta(days=1)
    for month_start in _month_starts(start_date, month_end):
        url = monthly_futures_url(pair, month_start.year, month_start.month)
        if not head_ok(url):
            continue
        frames.append(_parse_zip_rows(get_bytes(url, timeout=45)))

    try:
        payload = get_json(
            f"{BINANCE_FUTURES_API}/fapi/v1/klines",
            params={"symbol": pair, "interval": "1h", "limit": "1000"},
            timeout=20,
        )
        recent = pd.DataFrame(
            payload,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
        if not recent.empty:
            recent["timestamp"] = pd.to_datetime(recent["open_time"], unit="ms", utc=True)
            for column in ["open", "high", "low", "close", "volume"]:
                recent[column] = pd.to_numeric(recent[column], errors="coerce")
            frames.append(recent[["timestamp", "open", "high", "low", "close", "volume"]].dropna())
    except Exception:
        pass

    if not frames:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    out = out[(out["timestamp"] >= pd.Timestamp(start_date, tz="UTC")) & (out["timestamp"] <= pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1))]
    return out.reset_index(drop=True)


def fetch_intraday_history(pair: str, interval: str = "30m", limit: int = 1000) -> pd.DataFrame:
    payload = get_json(
        f"{BINANCE_DATA_API}/klines",
        params={"symbol": pair, "interval": interval, "limit": str(limit)},
        timeout=20,
    )
    recent = pd.DataFrame(
        payload,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    if recent.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    recent["timestamp"] = pd.to_datetime(recent["open_time"], unit="ms", utc=True)
    for column in ["open", "high", "low", "close", "volume"]:
        recent[column] = pd.to_numeric(recent[column], errors="coerce")
    return recent[["timestamp", "open", "high", "low", "close", "volume"]].dropna().sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_futures_intraday_history(pair: str, interval: str = "1h", limit: int = 1000) -> pd.DataFrame:
    payload = get_json(
        f"{BINANCE_FUTURES_API}/fapi/v1/klines",
        params={"symbol": pair, "interval": interval, "limit": str(limit)},
        timeout=20,
    )
    recent = pd.DataFrame(
        payload,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    if recent.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"])
    recent["timestamp"] = pd.to_datetime(recent["open_time"], unit="ms", utc=True)
    for column in ["open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"]:
        recent[column] = pd.to_numeric(recent[column], errors="coerce")
    return recent[["timestamp", "open", "high", "low", "close", "volume", "taker_buy_base", "taker_buy_quote"]].dropna().sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_funding_rate_history(pair: str, limit: int = 500) -> pd.DataFrame:
    payload = get_json(
        f"{BINANCE_FUTURES_API}/fapi/v1/fundingRate",
        params={"symbol": pair, "limit": str(limit)},
        timeout=20,
    )
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "funding_rate"])
    frame["timestamp"] = pd.to_datetime(pd.to_numeric(frame["fundingTime"], errors="coerce"), unit="ms", utc=True, errors="coerce")
    frame["funding_rate"] = pd.to_numeric(frame["fundingRate"], errors="coerce")
    return frame[["timestamp", "funding_rate"]].dropna().sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_open_interest_history(pair: str, period: str = "1h", limit: int = 500) -> pd.DataFrame:
    payload = get_json(
        f"{BINANCE_FUTURES_API}/futures/data/openInterestHist",
        params={"symbol": pair, "period": period, "limit": str(limit)},
        timeout=20,
    )
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "sum_open_interest", "sum_open_interest_value"])
    frame["timestamp"] = pd.to_datetime(pd.to_numeric(frame["timestamp"], errors="coerce"), unit="ms", utc=True, errors="coerce")
    frame["sum_open_interest"] = pd.to_numeric(frame["sumOpenInterest"], errors="coerce")
    frame["sum_open_interest_value"] = pd.to_numeric(frame["sumOpenInterestValue"], errors="coerce")
    return frame[["timestamp", "sum_open_interest", "sum_open_interest_value"]].dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_basis_history(pair: str, period: str = "1h", limit: int = 500) -> pd.DataFrame:
    payload = get_json(
        f"{BINANCE_FUTURES_API}/futures/data/basis",
        params={"symbol": pair, "contractType": "PERPETUAL", "period": period, "limit": str(limit)},
        timeout=20,
    )
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "basis_rate"])
    timestamp_column = "timestamp" if "timestamp" in frame.columns else "time"
    value_column = "basisRate" if "basisRate" in frame.columns else ("annualizedBasisRate" if "annualizedBasisRate" in frame.columns else None)
    if value_column is None:
        return pd.DataFrame(columns=["timestamp", "basis_rate"])
    frame["timestamp"] = pd.to_datetime(pd.to_numeric(frame[timestamp_column], errors="coerce"), unit="ms", utc=True, errors="coerce")
    frame["basis_rate"] = pd.to_numeric(frame[value_column], errors="coerce")
    return frame[["timestamp", "basis_rate"]].dropna().sort_values("timestamp").drop_duplicates(subset=["timestamp"])
