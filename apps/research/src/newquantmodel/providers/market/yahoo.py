from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from .http import get_json


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _chunk_range(start_ts: datetime, end_ts: datetime, interval: str) -> list[tuple[datetime, datetime]]:
    max_days = 730 if interval == "1d" else 180
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start_ts
    while cursor < end_ts:
        nxt = min(cursor + timedelta(days=max_days), end_ts)
        chunks.append((cursor, nxt))
        cursor = nxt + timedelta(seconds=1)
    return chunks


def _parse_payload(payload: object) -> pd.DataFrame:
    result = ((payload or {}).get("chart") or {}).get("result") if isinstance(payload, dict) else None
    if not result or not isinstance(result, list):
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    first = result[0] or {}
    timestamps = first.get("timestamp") or []
    quote = ((first.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    rows: list[dict] = []
    size = min(len(timestamps), len(closes))
    for idx in range(size):
        ts_raw = timestamps[idx]
        close_value = closes[idx]
        if ts_raw is None or close_value is None:
            continue
        open_value = opens[idx] if idx < len(opens) and opens[idx] is not None else close_value
        high_value = highs[idx] if idx < len(highs) and highs[idx] is not None else close_value
        low_value = lows[idx] if idx < len(lows) and lows[idx] is not None else close_value
        volume_value = volumes[idx] if idx < len(volumes) and volumes[idx] is not None else 0.0
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(int(ts_raw), tz=timezone.utc),
                "open": float(open_value),
                "high": float(high_value),
                "low": float(low_value),
                "close": float(close_value),
                "volume": float(volume_value),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def fetch_daily_history(symbol: str, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk_start, chunk_end in _chunk_range(start_ts, end_ts, "1d"):
        payload = get_json(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={
                "interval": "1d",
                "period1": int(chunk_start.timestamp()),
                "period2": int(chunk_end.timestamp()),
                "includePrePost": "true",
                "events": "div,split",
            },
            timeout=20,
        )
        parsed = _parse_payload(payload)
        if not parsed.empty:
            frames.append(parsed)

    if not frames:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return pd.concat(frames, ignore_index=True).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
