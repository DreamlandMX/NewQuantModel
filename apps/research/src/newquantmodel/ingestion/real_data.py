from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

import pandas as pd

from newquantmodel.config.settings import AppPaths
from newquantmodel.providers.crypto.binance import fetch_hourly_history, has_perpetual_proxy, recent_spot_symbols, to_spot_pair
from newquantmodel.providers.crypto.coingecko import fetch_top_market_cap
from newquantmodel.providers.market.eastmoney import fetch_daily_history as fetch_eastmoney_daily
from newquantmodel.providers.market.yahoo import fetch_daily_history as fetch_yahoo_daily
from newquantmodel.providers.market.yfiua import CURRENT_SOURCES, ConstituentSource, build_membership_history, fetch_current_constituents
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel.storage.parquet_store import read_frame, replace_market_rows, replace_rows_by_keys, sync_duckdb, write_frame


INDEX_SYMBOLS = {
    "cn_equity": ["000001.SS", "000300.SS"],
    "us_equity": ["^DJI", "^NDX", "^GSPC"],
}


@dataclass(slots=True)
class IngestResult:
    market: str
    asset_master: pd.DataFrame
    universe_membership: pd.DataFrame
    bars_1h: pd.DataFrame
    bars_1d: pd.DataFrame
    universe_records: pd.DataFrame
    data_health: pd.DataFrame


def _today_utc() -> datetime:
    return datetime.now(timezone.utc)


def _start_date(years: int) -> date:
    now = _today_utc().date()
    return date(now.year - years, now.month, max(1, min(now.day, 28)))


def _aggregate_daily_from_hourly(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    indexed = frame.set_index("timestamp").sort_index()
    daily = indexed.resample("1D").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["close"]).reset_index()
    return daily


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


def ingest_crypto(paths: AppPaths, years: int, limit: int | None = None) -> IngestResult:
    as_of = _today_utc()
    start = _start_date(years)
    coins = fetch_top_market_cap(limit or 50)
    spot_symbols = recent_spot_symbols()
    raw_dir = paths.raw_dir / "crypto"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_json(raw_dir / "coingecko_top.json", coins.to_dict(orient="records"))

    asset_rows: list[dict] = []
    bars_1h_rows: list[pd.DataFrame] = []
    bars_1d_rows: list[pd.DataFrame] = []

    for _, row in coins.iterrows():
        pair = to_spot_pair(row["symbol"])
        if pair not in spot_symbols:
            continue
        hourly = fetch_hourly_history(pair, start, as_of.date())
        if hourly.empty:
            continue
        hourly["symbol"] = pair
        hourly["market"] = "crypto"
        hourly["primaryVenue"] = "Binance Spot"
        daily = _aggregate_daily_from_hourly(hourly[["timestamp", "open", "high", "low", "close", "volume"]])
        daily["symbol"] = pair
        daily["market"] = "crypto"
        bars_1h_rows.append(hourly[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
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
    bars_1h = pd.concat(bars_1h_rows, ignore_index=True) if bars_1h_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
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
    return IngestResult("crypto", asset_master, membership, bars_1h, bars_1d, universe_records, data_health)


def _fetch_universe_sources(market: str) -> list[ConstituentSource]:
    if market == "cn_equity":
        return [source for source in CURRENT_SOURCES if source.market == "cn_equity"]
    if market == "us_equity":
        return [source for source in CURRENT_SOURCES if source.market == "us_equity"]
    return []


def ingest_equities(paths: AppPaths, market: str, years: int, limit: int | None = None) -> IngestResult:
    as_of = _today_utc()
    start = _start_date(years)
    sources = _fetch_universe_sources(market)
    raw_dir = paths.raw_dir / market
    raw_dir.mkdir(parents=True, exist_ok=True)

    history_frames: list[pd.DataFrame] = []
    asset_rows_by_symbol: dict[str, dict] = {}
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
            primary_venue = "Shanghai/Shenzhen via EastMoney"
            hedge_proxy = "IF main contract"
            quote_asset = "CNY"
            timezone_name = "Asia/Shanghai"
        else:
            bars = _fetch_us_daily_with_cache(raw_dir, symbol, start, as_of)
            primary_venue = "Yahoo Finance"
            hedge_proxy = "Universe-specific ETF / inverse ETF proxy"
            quote_asset = "USD"
            timezone_name = "America/New_York"
        if bars.empty:
            continue
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
    bars_1d = pd.concat(bars_1d_rows, ignore_index=True) if bars_1d_rows else pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])
    bars_1h = pd.DataFrame(columns=["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"])

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
            venue = "EastMoney"
        else:
            bars = _fetch_us_daily_with_cache(raw_dir, symbol, start, as_of)
            venue = "Yahoo Finance"
        if bars.empty:
            continue
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
        bars_1h,
        bars_1d,
        pd.DataFrame(universe_rows),
        pd.DataFrame(health_rows),
    )


def persist_ingest_result(paths: AppPaths, result: IngestResult) -> None:
    asset_master = replace_rows_by_keys(read_frame(paths, "asset_master"), result.asset_master, ["symbol"])
    bars_1d = replace_rows_by_keys(read_frame(paths, "bars_1d"), result.bars_1d, ["symbol", "timestamp"])
    bars_1h = replace_rows_by_keys(read_frame(paths, "bars_1h"), result.bars_1h, ["symbol", "timestamp"]) if not result.bars_1h.empty else read_frame(paths, "bars_1h")
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
    write_frame(paths, "bars_1d", bars_1d)
    if not bars_1h.empty:
        write_frame(paths, "bars_1h", bars_1h)
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
