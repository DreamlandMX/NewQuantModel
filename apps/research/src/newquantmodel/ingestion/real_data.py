from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import numpy as np
import pandas as pd

from newquantmodel.config.settings import AppPaths
from newquantmodel.providers.crypto.binance import (
    fetch_basis_history,
    fetch_funding_rate_history,
    fetch_futures_hourly_history,
    fetch_futures_intraday_history,
    fetch_hourly_history,
    fetch_intraday_history as fetch_binance_intraday,
    fetch_open_interest_history,
    has_perpetual_proxy,
    recent_spot_symbols,
    to_spot_pair,
)
from newquantmodel.providers.crypto.coingecko import fetch_top_market_cap
from newquantmodel.providers.market.eastmoney import (
    fetch_daily_history as fetch_eastmoney_daily,
    fetch_intraday_history as fetch_eastmoney_intraday,
)
from newquantmodel.providers.market.yahoo import (
    fetch_daily_history as fetch_yahoo_daily,
    fetch_intraday_history as fetch_yahoo_intraday,
)
from newquantmodel.providers.market.yfiua import CURRENT_SOURCES, ConstituentSource, build_membership_history, fetch_current_constituents
from newquantmodel.analytics.signals import WEEKLY_RULES
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel.storage.parquet_store import read_frame, replace_market_rows, replace_rows_by_keys, sync_duckdb, write_frame


INDEX_SYMBOLS = {
    "cn_equity": ["000001.SS", "000300.SS"],
    "us_equity": ["^DJI", "^NDX", "^GSPC"],
}

MACRO_SYMBOLS = {
    "vix": "^VIX",
    "dxy": "DX-Y.NYB",
    "tnx": "^TNX",
    "gold": "GC=F",
    "oil": "CL=F",
    "sp500": "^GSPC",
    "nasdaq100": "^NDX",
    "csi300": "000300.SS",
}


@dataclass(slots=True)
class IngestResult:
    market: str
    asset_master: pd.DataFrame
    universe_membership: pd.DataFrame
    bars_30m: pd.DataFrame
    bars_1h: pd.DataFrame
    bars_4h: pd.DataFrame
    bars_1d: pd.DataFrame
    universe_records: pd.DataFrame
    data_health: pd.DataFrame


def _today_utc() -> datetime:
    return datetime.now(timezone.utc)


def _start_date(years: int) -> date:
    now = _today_utc().date()
    return date(now.year - years, now.month, max(1, min(now.day, 28)))


def _intraday_start_date(days: int = 60) -> date:
    return _today_utc().date() - timedelta(days=days)


def _aggregate_bars(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    indexed = frame.set_index("timestamp").sort_index()
    return indexed.resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["close"]).reset_index()


def _aggregate_daily_from_hourly(frame: pd.DataFrame) -> pd.DataFrame:
    return _aggregate_bars(frame, "1D")


def _aggregate_hourly_from_30m(frame: pd.DataFrame) -> pd.DataFrame:
    return _aggregate_bars(frame, "1H")


def _aggregate_4h_from_hourly(frame: pd.DataFrame) -> pd.DataFrame:
    return _aggregate_bars(frame, "4H")


def _current_refresh(ts: datetime) -> str:
    return ts.isoformat()


def _coverage_pct(total: int, covered: int) -> float:
    if total <= 0:
        return 0.0
    return round(covered / total * 100.0, 2)


def _missing_bar_pct(frame: pd.DataFrame, frequency: str) -> float:
    if frame.empty:
        return 100.0
    if frequency == "1d":
        return 0.0
    expected_alias = "1h" if frequency == "1h" else "1D"
    missing = 0.0
    total = 0
    for _, group in frame.sort_values("timestamp").groupby("symbol"):
        if len(group) < 2:
            continue
        expected = pd.date_range(group["timestamp"].min(), group["timestamp"].max(), freq=expected_alias, tz="UTC")
        observed = pd.DatetimeIndex(group["timestamp"])
        total += len(expected)
        missing += max(len(expected.difference(observed)), 0)
    if total <= 0:
        return 0.0
    return round(missing / total * 100.0, 2)


def _current_membership_rows(symbol: str, name: str, universe: str, market: str, as_of: datetime, coverage_mode: str, data_source: str) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "universe": universe,
        "market": market,
        "effective_from": pd.Timestamp(as_of.date()),
        "effective_to": pd.NaT,
        "coverage_mode": coverage_mode,
        "data_source": data_source,
    }


def _load_cached_current(cache_path, source: ConstituentSource) -> pd.DataFrame:
    cached = read_json(cache_path, [])
    if not cached:
        raise FileNotFoundError(cache_path)
    frame = pd.DataFrame(cached)
    if frame.empty:
        raise ValueError(f"Empty cached constituent file for {source.universe}")
    return frame


def _price_cache_path(raw_dir, symbol: str):
    safe_symbol = symbol.replace("/", "_").replace("^", "IDX_")
    return raw_dir / "prices" / f"{safe_symbol}.json"


def _write_price_cache(cache_path, bars: pd.DataFrame) -> None:
    payload = bars.copy()
    payload["timestamp"] = pd.to_datetime(payload["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    write_json(cache_path, payload.to_dict(orient="records"))


def _read_price_cache(cache_path) -> pd.DataFrame:
    cached = read_json(cache_path, [])
    if not cached:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame = pd.DataFrame(cached)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna().sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def _macro_cache_path(paths: AppPaths, symbol: str):
    safe_symbol = symbol.replace("/", "_").replace("^", "IDX_").replace("=", "_")
    return paths.raw_dir / "macro" / "prices" / f"{safe_symbol}.json"


def _fetch_macro_history(paths: AppPaths, symbol: str, start: date, end: datetime) -> pd.DataFrame:
    cache_path = _macro_cache_path(paths, symbol)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        bars = fetch_yahoo_daily(symbol, datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc), end)
        if not bars.empty:
            _write_price_cache(cache_path, bars)
            return bars
    except Exception:
        pass
    return _read_price_cache(cache_path)


def _build_macro_factor_frame(paths: AppPaths, start: date, end: datetime) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=min(8, max(len(MACRO_SYMBOLS), 1))) as executor:
        future_map = {
            executor.submit(_fetch_macro_history, paths, symbol, start, end): name
            for name, symbol in MACRO_SYMBOLS.items()
        }
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                frame = future.result()
            except Exception:
                continue
            if frame.empty:
                continue
            renamed = frame[["timestamp", "close"]].rename(columns={"close": name})
            frames[name] = renamed

    if not frames:
        return pd.DataFrame(columns=["timestamp"])

    merged: pd.DataFrame | None = None
    for frame in frames.values():
        merged = frame if merged is None else merged.merge(frame, on="timestamp", how="outer")
    if merged is None or merged.empty:
        return pd.DataFrame(columns=["timestamp"])

    merged = merged.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).ffill()
    merged["macro_vix_level"] = pd.to_numeric(merged.get("vix"), errors="coerce").fillna(method="ffill").fillna(0.0)
    vix_mean = merged["macro_vix_level"].rolling(126, min_periods=20).mean()
    vix_std = merged["macro_vix_level"].rolling(126, min_periods=20).std().replace(0.0, np.nan)
    merged["macro_vix_level"] = ((merged["macro_vix_level"] - vix_mean) / vix_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_vix_ret_5"] = pd.to_numeric(merged.get("vix"), errors="coerce").pct_change(5).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_dxy_ret_20"] = pd.to_numeric(merged.get("dxy"), errors="coerce").pct_change(20).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_tnx_change_20"] = pd.to_numeric(merged.get("tnx"), errors="coerce").diff(20).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_gold_ret_20"] = pd.to_numeric(merged.get("gold"), errors="coerce").pct_change(20).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_oil_ret_20"] = pd.to_numeric(merged.get("oil"), errors="coerce").pct_change(20).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_equity_rs_20"] = (
        pd.to_numeric(merged.get("nasdaq100"), errors="coerce").pct_change(20)
        - pd.to_numeric(merged.get("sp500"), errors="coerce").pct_change(20)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    merged["macro_regime_score"] = (
        -0.30 * merged["macro_vix_level"]
        - 0.20 * merged["macro_vix_ret_5"]
        - 0.15 * merged["macro_dxy_ret_20"]
        - 0.10 * merged["macro_tnx_change_20"]
        + 0.10 * merged["macro_gold_ret_20"]
        - 0.10 * merged["macro_oil_ret_20"]
        + 0.25 * merged["macro_equity_rs_20"]
    )
    return merged[["timestamp", "macro_vix_level", "macro_vix_ret_5", "macro_dxy_ret_20", "macro_tnx_change_20", "macro_gold_ret_20", "macro_oil_ret_20", "macro_equity_rs_20", "macro_regime_score"]].fillna(0.0)


def _resample_macro_frame(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    payload = frame.sort_values("timestamp").set_index("timestamp")
    return payload.resample(rule, label="right", closed="right").last().dropna(how="all").reset_index()


def _derivative_cache_path(paths: AppPaths, symbol: str):
    safe_symbol = symbol.replace("/", "_").replace("^", "IDX_")
    return paths.raw_dir / "crypto" / "derivatives" / f"{safe_symbol}_1h.json"


def _read_derivative_cache(paths: AppPaths, symbol: str) -> pd.DataFrame:
    cache_path = _derivative_cache_path(paths, symbol)
    cached = read_json(cache_path, [])
    if not cached:
        return pd.DataFrame()
    frame = pd.DataFrame(cached)
    if frame.empty:
        return pd.DataFrame()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    for column in ["funding_rate", "open_interest_change", "basis_rate", "taker_buy_imbalance"]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def _write_derivative_cache(paths: AppPaths, symbol: str, frame: pd.DataFrame) -> None:
    cache_path = _derivative_cache_path(paths, symbol)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = frame.copy()
    payload["timestamp"] = pd.to_datetime(payload["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    write_json(cache_path, payload.to_dict(orient="records"))


def _build_crypto_derivatives_frame(paths: AppPaths, symbol: str, bars_1h: pd.DataFrame) -> pd.DataFrame:
    spot = bars_1h[(bars_1h["market"] == "crypto") & (bars_1h["symbol"] == symbol)][["timestamp", "close", "volume"]].copy()
    if spot.empty:
        return pd.DataFrame(columns=["timestamp", "funding_rate", "open_interest_change", "basis_rate", "taker_buy_imbalance"])
    cached = _read_derivative_cache(paths, symbol)
    if not cached.empty and pd.Timestamp(cached["timestamp"].max()) >= pd.Timestamp(spot["timestamp"].max()) - pd.Timedelta(hours=2):
        return cached
    start = pd.Timestamp(spot["timestamp"].min()).date()
    end = pd.Timestamp(spot["timestamp"].max()).date()

    futures = pd.DataFrame()
    recent = pd.DataFrame()
    funding = pd.DataFrame()
    open_interest = pd.DataFrame()
    basis = pd.DataFrame()
    try:
        futures = fetch_futures_hourly_history(symbol, start, end)
    except Exception:
        futures = pd.DataFrame()
    try:
        recent = fetch_futures_intraday_history(symbol, interval="1h", limit=1000)
    except Exception:
        recent = pd.DataFrame()
    try:
        funding = fetch_funding_rate_history(symbol, limit=500)
    except Exception:
        funding = pd.DataFrame()
    try:
        open_interest = fetch_open_interest_history(symbol, period="1h", limit=500)
    except Exception:
        open_interest = pd.DataFrame()
    try:
        basis = fetch_basis_history(symbol, period="1h", limit=500)
    except Exception:
        basis = pd.DataFrame()

    merged = spot.sort_values("timestamp").copy()
    if not futures.empty:
        merged = merged.merge(
            futures[["timestamp", "close"]].rename(columns={"close": "futures_close"}),
            on="timestamp",
            how="left",
        )
        merged["basis_rate"] = (merged["futures_close"] / merged["close"].replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan)
    else:
        merged["basis_rate"] = np.nan
    if not basis.empty:
        merged = pd.merge_asof(
            merged.sort_values("timestamp"),
            basis.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            tolerance=pd.Timedelta(hours=12),
            suffixes=("", "_api"),
        )
        merged["basis_rate"] = merged["basis_rate_api"].combine_first(merged["basis_rate"]) if "basis_rate_api" in merged.columns else merged["basis_rate"]
    if not recent.empty:
        recent = recent.sort_values("timestamp").copy()
        recent["taker_buy_imbalance"] = (
            (2.0 * recent["taker_buy_base"] / recent["volume"].replace(0.0, np.nan)) - 1.0
        ).replace([np.inf, -np.inf], np.nan)
        merged = pd.merge_asof(
            merged.sort_values("timestamp"),
            recent[["timestamp", "taker_buy_imbalance"]].sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            tolerance=pd.Timedelta(hours=1),
        )
    else:
        merged["taker_buy_imbalance"] = np.nan
    if not funding.empty:
        merged = pd.merge_asof(
            merged.sort_values("timestamp"),
            funding.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            tolerance=pd.Timedelta(hours=12),
        )
    else:
        merged["funding_rate"] = np.nan
    if not open_interest.empty:
        open_interest = open_interest.sort_values("timestamp").copy()
        open_interest["open_interest_change"] = pd.to_numeric(open_interest["sum_open_interest_value"], errors="coerce").pct_change().replace([np.inf, -np.inf], np.nan)
        merged = pd.merge_asof(
            merged.sort_values("timestamp"),
            open_interest[["timestamp", "open_interest_change"]].sort_values("timestamp"),
            on="timestamp",
            direction="backward",
            tolerance=pd.Timedelta(hours=4),
        )
    else:
        merged["open_interest_change"] = np.nan

    for column in ["funding_rate", "open_interest_change", "basis_rate", "taker_buy_imbalance"]:
        merged[column] = pd.to_numeric(merged.get(column), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    final = merged[["timestamp", "funding_rate", "open_interest_change", "basis_rate", "taker_buy_imbalance"]].drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    _write_derivative_cache(paths, symbol, final)
    return final


def build_external_factor_panel(paths: AppPaths, bars_1d: pd.DataFrame, bars_1h: pd.DataFrame, signal_panel: pd.DataFrame) -> pd.DataFrame:
    if signal_panel.empty:
        return pd.DataFrame()
    keys = signal_panel[["market", "symbol", "timestamp", "signalFrequency"]].drop_duplicates().copy()
    if keys.empty:
        return pd.DataFrame()

    start_date = pd.Timestamp(keys["timestamp"].min()).date()
    end_ts = pd.Timestamp(keys["timestamp"].max())
    macro_daily = _build_macro_factor_frame(paths, start_date, end_ts)
    macro_by_market_frequency: dict[tuple[str, str], pd.DataFrame] = {}
    for market in sorted(keys["market"].dropna().astype(str).unique()):
        if market == "crypto":
            weekly_rule = WEEKLY_RULES["crypto"]
        else:
            weekly_rule = WEEKLY_RULES.get(market, "W-FRI")
        macro_by_market_frequency[(market, "daily")] = macro_daily.copy()
        macro_by_market_frequency[(market, "weekly")] = _resample_macro_frame(macro_daily, weekly_rule)

    rows: list[pd.DataFrame] = []
    for (market, signal_frequency), scoped_keys in keys.groupby(["market", "signalFrequency"], sort=False):
        if signal_frequency not in {"daily", "weekly"}:
            continue
        merged = scoped_keys.copy()
        macro_frame = macro_by_market_frequency.get((str(market), str(signal_frequency)))
        if macro_frame is not None and not macro_frame.empty:
            merged = merged.merge(macro_frame, on="timestamp", how="left")
        for column in ["macro_vix_level", "macro_vix_ret_5", "macro_dxy_ret_20", "macro_tnx_change_20", "macro_gold_ret_20", "macro_oil_ret_20", "macro_equity_rs_20", "macro_regime_score"]:
            if column not in merged.columns:
                merged[column] = 0.0

        if market == "crypto":
            derivative_frames: list[pd.DataFrame] = []
            symbols = sorted(scoped_keys["symbol"].dropna().astype(str).unique())
            with ThreadPoolExecutor(max_workers=min(8, max(len(symbols), 1))) as executor:
                future_map = {
                    executor.submit(_build_crypto_derivatives_frame, paths, symbol, bars_1h): symbol
                    for symbol in symbols
                }
                for future in as_completed(future_map):
                    symbol = future_map[future]
                    try:
                        derivative = future.result()
                    except Exception:
                        continue
                    if derivative.empty:
                        continue
                    if signal_frequency == "daily":
                        derivative = (
                            derivative.set_index("timestamp")
                            .resample("1D")
                            .mean()
                            .dropna(how="all")
                            .reset_index()
                        )
                    else:
                        derivative = (
                            derivative.set_index("timestamp")
                            .resample(WEEKLY_RULES["crypto"], label="right", closed="right")
                            .mean()
                            .dropna(how="all")
                            .reset_index()
                        )
                    derivative["symbol"] = symbol
                    derivative["market"] = "crypto"
                    derivative["signalFrequency"] = signal_frequency
                    derivative_frames.append(derivative)
            derivative_panel = pd.concat(derivative_frames, ignore_index=True) if derivative_frames else pd.DataFrame()
            if not derivative_panel.empty:
                merged = merged.merge(derivative_panel, on=["market", "symbol", "timestamp", "signalFrequency"], how="left")
        for column in ["funding_rate", "open_interest_change", "basis_rate", "taker_buy_imbalance"]:
            if column not in merged.columns:
                merged[column] = 0.0
            merged[column] = pd.to_numeric(merged[column], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        rows.append(merged)

    if not rows:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "signalFrequency"])
    panel = pd.concat(rows, ignore_index=True).sort_values(["market", "signalFrequency", "symbol", "timestamp"]).reset_index(drop=True)
    return panel


def _fetch_cn_daily_with_fallback(raw_dir, symbol: str, start: date, as_of: datetime) -> pd.DataFrame:
    cache_path = _price_cache_path(raw_dir, symbol)
    fetchers = [
        lambda: fetch_eastmoney_daily(symbol, start.isoformat()),
        lambda: fetch_yahoo_daily(symbol, datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc), as_of),
    ]
    for fetcher in fetchers:
        try:
            bars = fetcher()
        except Exception:
            continue
        if not bars.empty:
            _write_price_cache(cache_path, bars)
            return bars
    return _read_price_cache(cache_path)


def _fetch_us_daily_with_cache(raw_dir, symbol: str, start: date, as_of: datetime) -> pd.DataFrame:
    cache_path = _price_cache_path(raw_dir, symbol)
    try:
        bars = fetch_yahoo_daily(symbol, datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc), as_of)
        if not bars.empty:
            _write_price_cache(cache_path, bars)
            return bars
    except Exception:
        pass
    return _read_price_cache(cache_path)


def _intraday_cache_path(raw_dir, symbol: str, suffix: str):
    safe_symbol = symbol.replace("/", "_").replace("^", "IDX_")
    return raw_dir / "prices" / f"{safe_symbol}_{suffix}.json"


def _fetch_us_intraday_with_cache(raw_dir, symbol: str, start: date, as_of: datetime) -> pd.DataFrame:
    cache_path = _intraday_cache_path(raw_dir, symbol, "30m")
    try:
        bars = fetch_yahoo_intraday(symbol, datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc), as_of, interval="30m")
        if not bars.empty:
            _write_price_cache(cache_path, bars)
            return bars
    except Exception:
        pass
    return _read_price_cache(cache_path)


def _fetch_cn_intraday_with_fallback(raw_dir, symbol: str, start: date, as_of: datetime) -> pd.DataFrame:
    cache_path = _intraday_cache_path(raw_dir, symbol, "30m")
    fetchers = [
        lambda: fetch_eastmoney_intraday(symbol, start.isoformat(), interval_minutes=30),
        lambda: fetch_yahoo_intraday(symbol, datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc), as_of, interval="30m"),
    ]
    for fetcher in fetchers:
        try:
            bars = fetcher()
        except Exception:
            continue
        if not bars.empty:
            _write_price_cache(cache_path, bars)
            return bars
    return _read_price_cache(cache_path)


def ingest_crypto(paths: AppPaths, years: int, limit: int | None = None) -> IngestResult:
    as_of = _today_utc()
    start = _start_date(years)
    intraday_start = _intraday_start_date(30)
    coins = fetch_top_market_cap(limit or 50)
    spot_symbols = recent_spot_symbols()
    raw_dir = paths.raw_dir / "crypto"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_json(raw_dir / "coingecko_top.json", coins.to_dict(orient="records"))

    asset_rows: list[dict] = []
    bars_30m_rows: list[pd.DataFrame] = []
    bars_1h_rows: list[pd.DataFrame] = []
    bars_4h_rows: list[pd.DataFrame] = []
    bars_1d_rows: list[pd.DataFrame] = []

    for _, row in coins.iterrows():
        pair = to_spot_pair(row["symbol"])
        if pair not in spot_symbols:
            continue
        intraday_30m = fetch_binance_intraday(pair, interval="30m", limit=1000)
        if not intraday_30m.empty:
            intraday_30m = intraday_30m[intraday_30m["timestamp"] >= pd.Timestamp(intraday_start, tz="UTC")].copy()
            intraday_30m["symbol"] = pair
            intraday_30m["market"] = "crypto"
            bars_30m_rows.append(intraday_30m[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        hourly = fetch_hourly_history(pair, start, as_of.date())
        if hourly.empty:
            continue
        hourly["symbol"] = pair
        hourly["market"] = "crypto"
        hourly["primaryVenue"] = "Binance Spot"
        hourly_intraday = hourly[hourly["timestamp"] >= pd.Timestamp(intraday_start, tz="UTC")].copy()
        bars_4h = _aggregate_4h_from_hourly(hourly_intraday[["timestamp", "open", "high", "low", "close", "volume"]]) if not hourly_intraday.empty else pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        daily = _aggregate_daily_from_hourly(hourly[["timestamp", "open", "high", "low", "close", "volume"]])
        daily["symbol"] = pair
        daily["market"] = "crypto"
        bars_1h_rows.append(hourly[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        if not bars_4h.empty:
            bars_4h["symbol"] = pair
            bars_4h["market"] = "crypto"
            bars_4h_rows.append(bars_4h[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        bars_1d_rows.append(daily[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        tradable = has_perpetual_proxy(pair)
        asset_rows.append(
            {
                "symbol": pair,
                "name": row["name"],
                "market": "crypto",
                "timezone": "UTC",
                "isTradable": tradable,
                "hedgeProxy": f"{pair} perpetual" if tradable else None,
                "memberships": ["crypto_top50_spot"],
                "riskBucket": "crypto-beta",
                "primaryVenue": "Binance Spot",
                "tradableSymbol": pair if tradable else None,
                "quoteAsset": "USDT",
                "hasPerpetualProxy": tradable,
                "historyCoverageStart": daily["timestamp"].min().date().isoformat(),
            }
        )

    asset_master = pd.DataFrame(asset_rows)
    bars_30m = pd.concat(bars_30m_rows, ignore_index=True) if bars_30m_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_1h = pd.concat(bars_1h_rows, ignore_index=True) if bars_1h_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_4h = pd.concat(bars_4h_rows, ignore_index=True) if bars_4h_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_1d = pd.concat(bars_1d_rows, ignore_index=True) if bars_1d_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])

    membership_rows = asset_master[["symbol", "name"]].copy()
    if membership_rows.empty:
        membership = pd.DataFrame(columns=["symbol", "name", "universe", "market", "effective_from", "effective_to", "coverage_mode", "data_source"])
    else:
        membership_rows["universe"] = "crypto_top50_spot"
        membership_rows["market"] = "crypto"
        membership_rows["effective_from"] = pd.Timestamp(as_of.date())
        membership_rows["effective_to"] = pd.NaT
        membership_rows["coverage_mode"] = "approx_bootstrap"
        membership_rows["data_source"] = "coingecko+binance"
        membership = membership_rows

    tradable_count = int(asset_master["isTradable"].sum()) if not asset_master.empty else 0
    universe_records = pd.DataFrame(
        [
            {
                "market": "crypto",
                "universe": "crypto_top50_spot",
                "coverageDate": as_of.date().isoformat(),
                "memberCount": len(coins),
                "policyNotes": ["Top 50 spot market cap universe", "Stablecoins excluded", "Tradable subset requires Binance perpetual proxy"],
                "tradableProxy": "Binance perpetual archive",
                "dataSource": "CoinGecko + Binance",
                "coverageMode": "approx_bootstrap",
                "historyStartDate": start.isoformat(),
                "coveragePct": _coverage_pct(len(coins), len(asset_master)),
                "refreshSchedule": "00:00/04:00/08:00/12:00/16:00/20:00 UTC",
                "lastRefreshAt": _current_refresh(as_of),
            }
        ]
    )
    data_health = pd.DataFrame(
        [
            {
                "market": "crypto",
                "lastRefreshAt": _current_refresh(as_of),
                "coveragePct": _coverage_pct(len(coins), len(asset_master)),
                "missingBarPct": _missing_bar_pct(bars_1h, "1h"),
                "tradableCoveragePct": _coverage_pct(len(asset_master), tradable_count),
                "membershipMode": "approx_bootstrap",
                "historyStartDate": start.isoformat(),
                "stale": False,
                "notes": ["Hourly bars sourced from Binance archive/data-api", "Tradability uses Binance futures archive existence"],
            }
        ]
    )
    return IngestResult("crypto", asset_master, membership, bars_30m, bars_1h, bars_4h, bars_1d, universe_records, data_health)


def _fetch_universe_sources(market: str) -> list[ConstituentSource]:
    if market == "cn_equity":
        return [source for source in CURRENT_SOURCES if source.market == "cn_equity"]
    if market == "us_equity":
        return [source for source in CURRENT_SOURCES if source.market == "us_equity"]
    return []


def ingest_equities(paths: AppPaths, market: str, years: int, limit: int | None = None) -> IngestResult:
    as_of = _today_utc()
    start = _start_date(years)
    intraday_start = _intraday_start_date(60)
    sources = _fetch_universe_sources(market)
    raw_dir = paths.raw_dir / market
    raw_dir.mkdir(parents=True, exist_ok=True)

    history_frames: list[pd.DataFrame] = []
    asset_rows_by_symbol: dict[str, dict] = {}
    bars_30m_rows: list[pd.DataFrame] = []
    bars_1h_rows: list[pd.DataFrame] = []
    bars_4h_rows: list[pd.DataFrame] = []
    bars_1d_rows: list[pd.DataFrame] = []
    universe_rows: list[dict] = []
    health_rows: list[dict] = []
    symbol_memberships: dict[str, set[str]] = {}
    symbol_names: dict[str, str] = {}

    for source in sources:
        cache_path = raw_dir / f"{source.universe}_current.json"
        try:
            current = fetch_current_constituents(source)
            if limit:
                current = current.head(limit)
            write_json(cache_path, current.to_dict(orient="records"))
        except Exception:
            current = _load_cached_current(cache_path, source)
            if limit:
                current = current.head(limit)

        history = build_membership_history(source, start, as_of.date(), current=current)
        if limit:
            keep = set(current["symbol"])
            history = history[history["symbol"].isin(keep)]
        history_frames.append(history)

        for _, row in current.iterrows():
            symbol = row["symbol"]
            symbol_names[symbol] = row["name"]
            symbol_memberships.setdefault(symbol, set()).add(source.universe)

        universe_rows.append(
            {
                "market": source.market,
                "universe": source.universe,
                "coverageDate": as_of.date().isoformat(),
                "memberCount": int(len(current)),
                "policyNotes": ["Monthly PIT snapshots from yfiua", "Daily forward snapshots archived locally"],
                "tradableProxy": "IF main contract" if source.market == "cn_equity" else {"sp500": "SPY / SH", "nasdaq100": "QQQ / PSQ", "dow30": "DIA / DOG"}[source.universe],
                "dataSource": "yfiua + EastMoney" if source.market == "cn_equity" else "yfiua + Yahoo Finance",
                "coverageMode": "point_in_time",
                "historyStartDate": history["effective_from"].min().date().isoformat(),
                "coveragePct": 100.0,
                "refreshSchedule": "Asia/Shanghai 16:30" if source.market == "cn_equity" else "America/New_York 17:30",
                "lastRefreshAt": _current_refresh(as_of),
            }
        )
    for symbol, memberships in sorted(symbol_memberships.items()):
        if market == "cn_equity":
            bars = _fetch_cn_daily_with_fallback(raw_dir, symbol, start, as_of)
            intraday = _fetch_cn_intraday_with_fallback(raw_dir, symbol, intraday_start, as_of)
            primary_venue = "Shanghai/Shenzhen via EastMoney"
            hedge_proxy = "IF main contract"
            quote_asset = "CNY"
            timezone_name = "Asia/Shanghai"
        else:
            bars = _fetch_us_daily_with_cache(raw_dir, symbol, start, as_of)
            intraday = _fetch_us_intraday_with_cache(raw_dir, symbol, intraday_start, as_of)
            primary_venue = "Yahoo Finance"
            hedge_proxy = "Universe-specific ETF / inverse ETF proxy"
            quote_asset = "USD"
            timezone_name = "America/New_York"
        if bars.empty:
            continue
        if not intraday.empty:
            intraday["symbol"] = symbol
            intraday["market"] = market
            intraday_1h = _aggregate_hourly_from_30m(intraday[["timestamp", "open", "high", "low", "close", "volume"]])
            intraday_4h = _aggregate_4h_from_hourly(intraday_1h)
            bars_30m_rows.append(intraday[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
            if not intraday_1h.empty:
                intraday_1h["symbol"] = symbol
                intraday_1h["market"] = market
                bars_1h_rows.append(intraday_1h[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
            if not intraday_4h.empty:
                intraday_4h["symbol"] = symbol
                intraday_4h["market"] = market
                bars_4h_rows.append(intraday_4h[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        bars["symbol"] = symbol
        bars["market"] = market
        bars_1d_rows.append(bars[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        asset_rows_by_symbol[symbol] = {
            "symbol": symbol,
            "name": symbol_names.get(symbol, symbol),
            "market": market,
            "timezone": timezone_name,
            "isTradable": True,
            "hedgeProxy": hedge_proxy,
            "memberships": sorted(memberships),
            "riskBucket": "equity-beta",
            "primaryVenue": primary_venue,
            "tradableSymbol": symbol,
            "quoteAsset": quote_asset,
            "hasPerpetualProxy": False,
            "historyCoverageStart": bars["timestamp"].min().date().isoformat(),
        }

    asset_master = pd.DataFrame(asset_rows_by_symbol.values()) if asset_rows_by_symbol else pd.DataFrame()
    universe_membership = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame()
    bars_30m = pd.concat(bars_30m_rows, ignore_index=True) if bars_30m_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_1d = pd.concat(bars_1d_rows, ignore_index=True) if bars_1d_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_1h = pd.concat(bars_1h_rows, ignore_index=True) if bars_1h_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_4h = pd.concat(bars_4h_rows, ignore_index=True) if bars_4h_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])

    if market == "cn_equity":
        index_symbols = [("000001.SS", "sse_composite"), ("000300.SS", "csi300_index")]
        proxy_label = None
        index_market = "index"
    else:
        index_symbols = [("^DJI", "dow_index"), ("^NDX", "nasdaq100_index"), ("^GSPC", "sp500_index")]
        proxy_label = {"dow_index": "DIA / DOG", "nasdaq100_index": "QQQ / PSQ", "sp500_index": "SPY / SH"}
        index_market = "index"

    index_asset_rows: list[dict] = []
    index_bars_rows: list[pd.DataFrame] = []
    index_universe_rows: list[dict] = []
    index_membership_rows: list[dict] = []
    index_history_starts: list[str] = []
    for symbol, universe in index_symbols:
        if market == "cn_equity":
            bars = _fetch_cn_daily_with_fallback(raw_dir, symbol, start, as_of)
            intraday = _fetch_cn_intraday_with_fallback(raw_dir, symbol, intraday_start, as_of)
            venue = "EastMoney"
        else:
            bars = _fetch_us_daily_with_cache(raw_dir, symbol, start, as_of)
            intraday = _fetch_us_intraday_with_cache(raw_dir, symbol, intraday_start, as_of)
            venue = "Yahoo Finance"
        if bars.empty:
            continue
        if not intraday.empty:
            intraday["symbol"] = symbol
            intraday["market"] = index_market
            intraday_1h = _aggregate_hourly_from_30m(intraday[["timestamp", "open", "high", "low", "close", "volume"]])
            intraday_4h = _aggregate_4h_from_hourly(intraday_1h)
            bars_30m_rows.append(intraday[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
            if not intraday_1h.empty:
                intraday_1h["symbol"] = symbol
                intraday_1h["market"] = index_market
                bars_1h_rows.append(intraday_1h[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
            if not intraday_4h.empty:
                intraday_4h["symbol"] = symbol
                intraday_4h["market"] = index_market
                bars_4h_rows.append(intraday_4h[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        bars["symbol"] = symbol
        bars["market"] = index_market
        index_bars_rows.append(bars[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
        index_asset_rows.append(
            {
                "symbol": symbol,
                "name": universe,
                "market": index_market,
                "timezone": "Asia/Shanghai" if market == "cn_equity" else "America/New_York",
                "isTradable": proxy_label is not None,
                "hedgeProxy": None if proxy_label is None else proxy_label[universe],
                "memberships": [universe],
                "riskBucket": "benchmark",
                "primaryVenue": venue,
                "tradableSymbol": symbol,
                "quoteAsset": "CNY" if market == "cn_equity" else "USD",
                "hasPerpetualProxy": False,
                "historyCoverageStart": bars["timestamp"].min().date().isoformat(),
            }
        )
        index_membership_rows.append(
            _current_membership_rows(
                symbol=symbol,
                name=universe,
                universe=universe,
                market="index",
                as_of=as_of,
                coverage_mode="point_in_time",
                data_source="EastMoney" if market == "cn_equity" else "Yahoo Finance",
            )
        )
        index_universe_rows.append(
            {
                "market": "index",
                "universe": universe,
                "coverageDate": as_of.date().isoformat(),
                "memberCount": 1,
                "policyNotes": ["Index baseline forecast universe"],
                "tradableProxy": None if proxy_label is None else proxy_label[universe],
                "dataSource": "EastMoney" if market == "cn_equity" else "Yahoo Finance",
                "coverageMode": "point_in_time" if market != "cn_equity" else "point_in_time",
                "historyStartDate": bars["timestamp"].min().date().isoformat(),
                "coveragePct": 100.0,
                "refreshSchedule": "Asia/Shanghai 16:30" if market == "cn_equity" else "America/New_York 17:30",
                "lastRefreshAt": _current_refresh(as_of),
            }
        )
        index_history_starts.append(bars["timestamp"].min().date().isoformat())

    if index_asset_rows:
        asset_master = pd.concat([asset_master, pd.DataFrame(index_asset_rows)], ignore_index=True)
        bars_1d = pd.concat([bars_1d, pd.concat(index_bars_rows, ignore_index=True)], ignore_index=True)
        universe_membership = pd.concat([universe_membership, pd.DataFrame(index_membership_rows)], ignore_index=True)
        universe_rows.extend(index_universe_rows)
        health_rows.append(
            {
                "market": "index",
                "lastRefreshAt": _current_refresh(as_of),
                "coveragePct": 100.0,
                "missingBarPct": _missing_bar_pct(pd.concat(index_bars_rows, ignore_index=True), "1d"),
                "tradableCoveragePct": 100.0 if proxy_label else 0.0,
                "membershipMode": "point_in_time",
                "historyStartDate": min(index_history_starts) if index_history_starts else start.isoformat(),
                "stale": False,
                "notes": [f"Index bars loaded from {'EastMoney' if market == 'cn_equity' else 'Yahoo Finance'}"],
            }
        )

    if not bars_1d.empty:
        health_rows.append(
            {
                "market": market,
                "lastRefreshAt": _current_refresh(as_of),
                "coveragePct": 100.0,
                "missingBarPct": _missing_bar_pct(bars_1d[bars_1d["market"] == market], "1d"),
                "tradableCoveragePct": 100.0,
                "membershipMode": "point_in_time",
                "historyStartDate": bars_1d[bars_1d["market"] == market]["timestamp"].min().date().isoformat(),
                "stale": False,
                "notes": [f"{market} bars refreshed from {'EastMoney' if market == 'cn_equity' else 'Yahoo Finance'}"],
            }
        )

    return IngestResult(
        market,
        asset_master,
        universe_membership,
        bars_30m,
        bars_1h,
        bars_4h,
        bars_1d,
        pd.DataFrame(universe_rows),
        pd.DataFrame(health_rows),
    )


def persist_ingest_result(paths: AppPaths, result: IngestResult) -> None:
    asset_master = replace_rows_by_keys(read_frame(paths, "asset_master"), result.asset_master, ["symbol"])
    bars_30m = replace_rows_by_keys(read_frame(paths, "bars_30m"), result.bars_30m, ["symbol", "timestamp"]) if not result.bars_30m.empty else read_frame(paths, "bars_30m")
    bars_1d = replace_rows_by_keys(read_frame(paths, "bars_1d"), result.bars_1d, ["symbol", "timestamp"])
    bars_1h = replace_rows_by_keys(read_frame(paths, "bars_1h"), result.bars_1h, ["symbol", "timestamp"]) if not result.bars_1h.empty else read_frame(paths, "bars_1h")
    bars_4h = replace_rows_by_keys(read_frame(paths, "bars_4h"), result.bars_4h, ["symbol", "timestamp"]) if not result.bars_4h.empty else read_frame(paths, "bars_4h")
    existing_membership = read_frame(paths, "universe_membership")
    if existing_membership.empty:
        membership = result.universe_membership
    elif result.universe_membership.empty:
        membership = existing_membership
    else:
        membership = replace_rows_by_keys(existing_membership, result.universe_membership, ["symbol", "universe", "effective_from"])
    health = replace_rows_by_keys(read_frame(paths, "data_health"), result.data_health, ["market"])

    write_frame(paths, "asset_master", asset_master)
    write_frame(paths, "universe_membership", membership)
    if not bars_30m.empty:
        write_frame(paths, "bars_30m", bars_30m)
    write_frame(paths, "bars_1d", bars_1d)
    if not bars_1h.empty:
        write_frame(paths, "bars_1h", bars_1h)
    if not bars_4h.empty:
        write_frame(paths, "bars_4h", bars_4h)
    write_frame(paths, "data_health", health)
    existing_universes = read_json(paths.reference_dir / "universes_reference.json", [])
    if existing_universes:
        existing_universe_frame = pd.DataFrame(existing_universes)
        if not existing_universe_frame.empty:
            merged_universes = replace_rows_by_keys(existing_universe_frame, result.universe_records, ["universe"])
        else:
            merged_universes = result.universe_records
    else:
        merged_universes = result.universe_records
    write_json(paths.reference_dir / "universes_reference.json", merged_universes.to_dict(orient="records"))
    sync_duckdb(paths)
