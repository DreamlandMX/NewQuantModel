from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .http import get_json


BASE_URL = "https://yfiua.github.io/index-constituents"
HISTORY_STARTS = {
    "csi300": date(2023, 7, 1),
    "sse": date(2023, 7, 1),
    "nasdaq100": date(2023, 7, 1),
    "sp500": date(2023, 7, 1),
    "dowjones": date(2023, 7, 1),
}


@dataclass(slots=True)
class ConstituentSource:
    code: str
    universe: str
    market: str
    data_source: str


CURRENT_SOURCES = [
    ConstituentSource("csi300", "csi300", "cn_equity", "yfiua"),
    ConstituentSource("sp500", "sp500", "us_equity", "yfiua"),
    ConstituentSource("nasdaq100", "nasdaq100", "us_equity", "yfiua"),
    ConstituentSource("dowjones", "dow30", "us_equity", "yfiua"),
]


def _normalize_rows(payload: list[dict], source: ConstituentSource) -> pd.DataFrame:
    frame = pd.DataFrame(payload)
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "name", "universe", "market"])
    symbol_col = "Symbol" if "Symbol" in frame.columns else "symbol"
    name_col = "Name" if "Name" in frame.columns else "name"
    out = pd.DataFrame(
        {
            "symbol": frame[symbol_col].astype(str).str.upper(),
            "name": frame[name_col].astype(str),
            "universe": source.universe,
            "market": source.market,
        }
    )
    return out.drop_duplicates()


def fetch_current_constituents(source: ConstituentSource) -> pd.DataFrame:
    payload = get_json(f"{BASE_URL}/constituents-{source.code}.json")
    return _normalize_rows(payload, source)


def fetch_monthly_constituents(source: ConstituentSource, year: int, month: int) -> pd.DataFrame:
    payload = get_json(f"{BASE_URL}/{year:04d}/{month:02d}/constituents-{source.code}.json")
    return _normalize_rows(payload, source)


def iter_month_starts(start_date: date, end_date: date) -> list[date]:
    cursor = date(start_date.year, start_date.month, 1)
    last = date(end_date.year, end_date.month, 1)
    months: list[date] = []
    while cursor <= last:
        months.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def build_membership_history(source: ConstituentSource, start_date: date, end_date: date, current: pd.DataFrame | None = None) -> pd.DataFrame:
    coverage_start = max(HISTORY_STARTS[source.code], start_date)
    current = current if current is not None else fetch_current_constituents(source)
    history_rows: list[pd.DataFrame] = []
    last_snapshot = current.copy()

    if start_date < HISTORY_STARTS[source.code]:
        bootstrap = current.copy()
        bootstrap["effective_from"] = pd.Series(
            [pd.Timestamp(start_date)] * len(bootstrap),
            index=bootstrap.index,
            dtype="datetime64[ns]",
        )
        bootstrap["effective_to"] = pd.Series(
            [pd.Timestamp(HISTORY_STARTS[source.code]) - pd.Timedelta(days=1)] * len(bootstrap),
            index=bootstrap.index,
            dtype="datetime64[ns]",
        )
        bootstrap["coverage_mode"] = "approx_bootstrap"
        bootstrap["data_source"] = source.data_source
        history_rows.append(bootstrap)

    for month_start in iter_month_starts(coverage_start, end_date):
        try:
            snapshot = fetch_monthly_constituents(source, month_start.year, month_start.month)
            last_snapshot = snapshot.copy()
        except Exception:
            snapshot = last_snapshot.copy()
        month_end = (pd.Timestamp(month_start) + pd.offsets.MonthEnd(0)).date()
        snapshot["effective_from"] = pd.Series(
            [pd.Timestamp(month_start)] * len(snapshot),
            index=snapshot.index,
            dtype="datetime64[ns]",
        )
        snapshot["effective_to"] = pd.Series(
            [pd.Timestamp(min(month_end, end_date))] * len(snapshot),
            index=snapshot.index,
            dtype="datetime64[ns]",
        )
        snapshot["coverage_mode"] = "point_in_time"
        snapshot["data_source"] = source.data_source
        history_rows.append(snapshot)

    if not history_rows:
        return pd.DataFrame(columns=["symbol", "name", "universe", "market", "effective_from", "effective_to", "coverage_mode", "data_source"])
    return pd.concat(history_rows, ignore_index=True).drop_duplicates()
