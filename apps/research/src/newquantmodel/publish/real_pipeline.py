from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone

import pandas as pd

from newquantmodel.analytics.backtest import build_backtests
from newquantmodel.analytics.factor_library import build_external_factor_panel_view
from newquantmodel.analytics.signals import build_signal_panel
from newquantmodel.analytics.trade_plans import build_trade_plan_panel
from newquantmodel.config.settings import AppPaths
from newquantmodel.ingestion.real_data import build_external_factor_panel, ingest_crypto, ingest_equities, persist_ingest_result
from newquantmodel.models.pipeline import BASELINE_MODEL_VERSION, bootstrap_baseline_outputs, build_ml_overlay
from newquantmodel.providers.crypto.coingecko import STABLE_IDS, STABLE_SYMBOLS
from newquantmodel.reporting.report_bundle import generate_report_bundle
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel.storage.parquet_store import read_frame, sync_duckdb, write_frame


MARKET_REFRESH_ORDER = ["crypto", "cn_equity", "us_equity"]
MARKET_STALE_GROUPS = {
    "crypto": ["crypto"],
    "cn_equity": ["cn_equity", "index"],
    "us_equity": ["us_equity", "index"],
}
INDEX_SORT_ORDER = {
    "000001.SS": 1,
    "000300.SS": 2,
    "^DJI": 3,
    "^NDX": 4,
    "^GSPC": 5,
}
CRYPTO_FORCED_TOP3 = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def _publish_context(paths: AppPaths) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    context = {
        "publishedAt": now.isoformat(),
        "dataSnapshotVersion": now.strftime("%Y%m%dT%H%M%SZ"),
    }
    write_json(paths.reference_dir / "publish_context.json", context)
    return context


def _load_publish_context(paths: AppPaths) -> dict[str, str]:
    payload = read_json(paths.reference_dir / "publish_context.json", {})
    if payload.get("publishedAt") and payload.get("dataSnapshotVersion"):
        return payload
    return _publish_context(paths)


def _serialize_frame(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    payload = frame.copy()
    for column in payload.columns:
        if pd.api.types.is_datetime64_any_dtype(payload[column]):
            payload[column] = payload[column].astype("datetime64[ns, UTC]").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return payload.to_dict(orient="records")


def _with_publish_context(items: list[dict], context: dict[str, str], stale_map: dict[str, bool], *, market_key: str = "market") -> list[dict]:
    stamped: list[dict] = []
    for item in items:
        row = dict(item)
        row["publishedAt"] = context["publishedAt"]
        row["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        market = row.get(market_key)
        row["stale"] = bool(stale_map.get(str(market), False))
        stamped.append(row)
    return stamped


def _coverage_lookup(universes: list[dict]) -> dict[str, dict]:
    return {str(item["universe"]): item for item in universes}


def _stale_lookup_from_health(data_health: pd.DataFrame) -> dict[str, bool]:
    if data_health.empty:
        return {}
    return {
        str(row["market"]): bool(row.get("stale", False))
        for row in data_health.to_dict(orient="records")
    }


def _safe_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _crypto_symbol_from_base(symbol: str) -> str:
    clean = str(symbol).upper().strip()
    return clean if clean.endswith("USDT") else f"{clean}USDT"


def _is_stable_crypto_item(item: dict[str, object]) -> bool:
    coingecko_id = str(item.get("coingecko_id") or item.get("id") or "").strip().lower()
    symbol = str(item.get("symbol") or "").strip().upper()
    return coingecko_id in STABLE_IDS or symbol in STABLE_SYMBOLS


def _latest_turnover_by_symbol(frame: pd.DataFrame, *, market: str, window_rows: int) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["market", "symbol", "sortMetric"])
    scoped = frame.loc[frame["market"] == market, ["market", "symbol", "timestamp", "close", "volume"]].copy()
    if scoped.empty:
        return pd.DataFrame(columns=["market", "symbol", "sortMetric"])
    scoped["close"] = pd.to_numeric(scoped["close"], errors="coerce")
    scoped["volume"] = pd.to_numeric(scoped["volume"], errors="coerce")
    scoped["turnover"] = scoped["close"] * scoped["volume"]
    scoped = scoped.dropna(subset=["timestamp", "turnover"]).sort_values(["symbol", "timestamp"])
    if scoped.empty:
        return pd.DataFrame(columns=["market", "symbol", "sortMetric"])
    latest = (
        scoped.groupby("symbol", group_keys=False)
        .tail(window_rows)
        .groupby(["market", "symbol"], as_index=False)
        .agg(sortMetric=("turnover", "sum"))
    )
    return latest.sort_values(["sortMetric", "symbol"], ascending=[False, True]).reset_index(drop=True)


def _build_sort_lookup(paths: AppPaths, asset_master: pd.DataFrame, rankings: pd.DataFrame, bars_1d: pd.DataFrame, bars_1h: pd.DataFrame) -> dict[tuple[str, str], dict[str, object]]:
    lookup: dict[tuple[str, str], dict[str, object]] = {}

    crypto_top = read_json(paths.raw_dir / "crypto" / "coingecko_top.json", [])
    crypto_rows: list[dict[str, object]] = []
    for item in crypto_top:
        if _is_stable_crypto_item(item):
            continue
        symbol = _crypto_symbol_from_base(item.get("symbol", ""))
        if not symbol or symbol == "USDT":
            continue
        crypto_rows.append(
            {
                "symbol": symbol,
                "market_cap_rank": int(item.get("market_cap_rank") or 999999),
                "market_cap": _safe_float(item.get("market_cap")),
            }
        )

    crypto_turnover = _latest_turnover_by_symbol(bars_1h, market="crypto", window_rows=24)
    crypto_turnover_map = {
        str(row["symbol"]): float(row["sortMetric"])
        for row in crypto_turnover.to_dict(orient="records")
    }

    if crypto_rows:
        forced_rows = [row for symbol in CRYPTO_FORCED_TOP3 for row in crypto_rows if row["symbol"] == symbol]
        trailing_rows = [row for row in crypto_rows if row["symbol"] not in set(CRYPTO_FORCED_TOP3)]
        ordered_crypto_rows = forced_rows + sorted(
            trailing_rows,
            key=lambda row: (
                -(crypto_turnover_map.get(str(row["symbol"]), float("-inf")) if crypto_turnover_map.get(str(row["symbol"])) is not None else float("-inf")),
                int(row["market_cap_rank"]),
                str(row["symbol"]),
            ),
        )
        for display_rank, item in enumerate(ordered_crypto_rows, start=1):
            lookup[("crypto", str(item["symbol"]))] = {
                "sortRank": display_rank,
                "sortMetric": crypto_turnover_map.get(str(item["symbol"])),
                "sortMetricLabel": "24h turnover rank",
            }

    for market in ["cn_equity", "us_equity", "index"]:
        market_turnover = _latest_turnover_by_symbol(bars_1d, market=market, window_rows=1)
        if market_turnover.empty:
            continue
        for idx, row in market_turnover.iterrows():
            lookup[(str(market), str(row["symbol"]))] = {
                "sortRank": idx + 1,
                "sortMetric": float(row["sortMetric"]),
                "sortMetricLabel": "1d turnover rank",
            }

    index_assets = asset_master[asset_master["market"] == "index"].copy()
    if not index_assets.empty:
        existing_index = {symbol for (market, symbol) in lookup.keys() if market == "index"}
        index_assets = index_assets.loc[~index_assets["symbol"].isin(existing_index)].copy()
        if not index_assets.empty:
            index_assets = index_assets.assign(
                _order=index_assets["symbol"].map(lambda symbol: INDEX_SORT_ORDER.get(str(symbol), 9999))
            ).sort_values(["_order", "symbol"])
            next_rank = max(
                [int(meta["sortRank"]) for (market, _symbol), meta in lookup.items() if market == "index"],
                default=0,
            ) + 1
            for offset, row in enumerate(index_assets.reset_index(drop=True).itertuples(index=False), start=0):
                lookup[("index", str(row.symbol))] = {
                    "sortRank": next_rank + offset,
                    "sortMetric": None,
                    "sortMetricLabel": "1d turnover rank",
                }

    label_by_market = {
        "crypto": "24h turnover rank",
        "cn_equity": "1d turnover rank",
        "us_equity": "1d turnover rank",
        "index": "1d turnover rank",
    }
    for market, market_frame in asset_master.groupby("market", sort=False):
        assigned = {
            str(symbol): int(meta["sortRank"])
            for (meta_market, symbol), meta in lookup.items()
            if meta_market == market
        }
        next_rank = max(assigned.values(), default=0) + 1
        for symbol in sorted({str(value) for value in market_frame["symbol"].tolist()}):
            if (str(market), symbol) in lookup:
                continue
            lookup[(str(market), symbol)] = {
                "sortRank": next_rank,
                "sortMetric": None,
                "sortMetricLabel": label_by_market.get(str(market), "Symbol order"),
            }
            next_rank += 1
    return lookup


def _allowed_crypto_symbols(paths: AppPaths) -> set[str]:
    crypto_top = read_json(paths.raw_dir / "crypto" / "coingecko_top.json", [])
    allowed: set[str] = set()
    for item in crypto_top:
        if _is_stable_crypto_item(item):
            continue
        symbol = _crypto_symbol_from_base(item.get("symbol", ""))
        if symbol and symbol != "USDT":
            allowed.add(symbol)
    return allowed


def _latest_timestamp(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty or "timestamp" not in frame.columns:
        return None
    try:
        return pd.Timestamp(frame["timestamp"].max())
    except Exception:
        return None


def _can_reuse_signal_panel(signal_panel: pd.DataFrame, bars_1d: pd.DataFrame, bars_30m: pd.DataFrame) -> bool:
    if signal_panel.empty:
        return False
    required_columns = {"ret_20", "trend_10_50", "macro_regime_score", "signalFrequency", "sourceFrequency", "isDerivedSignal"}
    if not required_columns.issubset(set(signal_panel.columns)):
        return False
    signal_latest = _latest_timestamp(signal_panel)
    daily_latest = _latest_timestamp(bars_1d)
    intraday_latest = _latest_timestamp(bars_30m)
    if daily_latest is not None and signal_latest is not None and signal_latest < daily_latest:
        return False
    if intraday_latest is not None and signal_latest is not None and signal_latest < intraday_latest:
        return False
    return True


@contextmanager
def _research_runtime_mode(*, fast: bool = False, full: bool = False):
    previous = os.environ.get("NQM_GA_FAST")
    if full:
        os.environ["NQM_GA_FAST"] = "0"
    elif fast:
        os.environ["NQM_GA_FAST"] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("NQM_GA_FAST", None)
        else:
            os.environ["NQM_GA_FAST"] = previous


def _mark_stale(paths: AppPaths, market: str, reason: str) -> None:
    stale_markets = MARKET_STALE_GROUPS.get(market, [market])
    data_health = read_frame(paths, "data_health")
    if not data_health.empty:
        for stale_market in stale_markets:
            mask = data_health["market"] == stale_market
            if not mask.any():
                continue
            note_series = data_health.loc[mask, "notes"].apply(
                lambda notes: [*notes, f"stale fallback: {reason}"] if isinstance(notes, list) else [f"stale fallback: {reason}"]
            )
            data_health["stale"] = data_health["stale"].astype("object")
            data_health.loc[mask, "stale"] = True
            data_health.loc[mask, "notes"] = pd.Series(note_series.tolist(), index=data_health.index[mask], dtype="object")
        write_frame(paths, "data_health", data_health)

    universes = read_json(paths.reference_dir / "universes_reference.json", [])
    if universes:
        frame = pd.DataFrame(universes)
        if not frame.empty:
            if "stale" not in frame.columns:
                frame["stale"] = False
            if "policyNotes" not in frame.columns:
                frame["policyNotes"] = [[] for _ in range(len(frame))]
            for stale_market in stale_markets:
                mask = frame["market"] == stale_market
                if not mask.any():
                    continue
                policy_notes = []
                for notes in frame.loc[mask, "policyNotes"].tolist():
                    if isinstance(notes, list):
                        policy_notes.append([*notes, f"stale fallback: {reason}"])
                    else:
                        policy_notes.append([f"stale fallback: {reason}"])
                frame["stale"] = frame["stale"].astype("object")
                frame.loc[mask, "stale"] = True
                frame.loc[mask, "policyNotes"] = pd.Series(policy_notes, index=frame.index[mask], dtype="object")
            write_json(paths.reference_dir / "universes_reference.json", frame.to_dict(orient="records"))
    sync_duckdb(paths)


def ingest_market(paths: AppPaths, market: str, years: int = 5, limit: int | None = None) -> None:
    if market == "crypto":
        result = ingest_crypto(paths, years=years, limit=limit)
    elif market in {"cn_equity", "us_equity"}:
        result = ingest_equities(paths, market=market, years=years, limit=limit)
    else:
        raise ValueError(f"Unsupported market {market}")
    persist_ingest_result(paths, result)


def build_baseline_signals(paths: AppPaths, *, fast: bool = False, full: bool = False) -> None:
    with _research_runtime_mode(fast=fast, full=full):
        _build_baseline_signals_impl(paths, fast=fast, full=full)


def _build_baseline_signals_impl(paths: AppPaths, *, fast: bool = False, full: bool = False) -> None:
    bars_1d = read_frame(paths, "bars_1d")
    bars_30m = read_frame(paths, "bars_30m")
    bars_1h = read_frame(paths, "bars_1h")
    cached_signal_panel = read_frame(paths, "signal_panel")
    if not full and _can_reuse_signal_panel(cached_signal_panel, bars_1d, bars_30m):
        preliminary_signal_panel = cached_signal_panel.copy()
    else:
        preliminary_signal_panel = build_signal_panel(bars_1d, bars_30m)
    cached_external_panel = read_frame(paths, "external_factor_panel")
    latest_ts = preliminary_signal_panel["timestamp"].max() if not preliminary_signal_panel.empty else None
    if (
        not full
        and
        latest_ts is not None
        and not cached_external_panel.empty
        and "timestamp" in cached_external_panel.columns
        and pd.Timestamp(cached_external_panel["timestamp"].max()) >= pd.Timestamp(latest_ts)
    ):
        external_factor_panel = cached_external_panel.copy()
    else:
        external_factor_panel = build_external_factor_panel(paths, bars_1d, bars_1h, preliminary_signal_panel)
    signal_panel = build_external_factor_panel_view(preliminary_signal_panel, external_factor_panel)
    write_frame(paths, "external_factor_panel", external_factor_panel)
    write_frame(paths, "signal_panel", signal_panel)
    bootstrap_baseline_outputs(paths, reuse_cached=not full)
    build_trade_plans(paths)
    sync_duckdb(paths)


def build_ml_signals(paths: AppPaths, *, fast: bool = False, full: bool = False) -> None:
    with _research_runtime_mode(fast=fast, full=full):
        build_ml_overlay(paths, reuse_cached=not full)
        build_trade_plans(paths)


def build_trade_plans(paths: AppPaths) -> None:
    asset_master = read_frame(paths, "asset_master")
    forecasts = read_frame(paths, "forecast_panel")
    rankings = read_frame(paths, "ranking_panel")
    bars_30m = read_frame(paths, "bars_30m")
    bars_1d = read_frame(paths, "bars_1d")
    bars_1h = read_frame(paths, "bars_1h")
    universes = read_json(paths.reference_dir / "universes_reference.json", [])
    trade_plans = build_trade_plan_panel(
        asset_master,
        forecasts,
        rankings,
        bars_1d,
        bars_1h,
        bars_30m=bars_30m,
        universes=universes,
    )
    write_frame(paths, "trade_plan_panel", trade_plans)
    sync_duckdb(paths)


def backtest_models(paths: AppPaths) -> None:
    history = read_frame(paths, "prediction_history_panel")
    if history.empty:
        history = read_frame(paths, "signal_panel")
        if not history.empty:
            history = history[["market", "symbol", "timestamp", "score"]].copy()
            history["predictedReturn"] = history["score"]
            history["modelVersion"] = BASELINE_MODEL_VERSION
    bars_1d = read_frame(paths, "bars_1d")
    backtests = build_backtests(history, bars_1d)
    write_frame(paths, "backtest_panel", backtests)
    sync_duckdb(paths)


def publish_real(paths: AppPaths, *, renew_context: bool = True) -> None:
    context = _publish_context(paths) if renew_context else _load_publish_context(paths)
    asset_master = read_frame(paths, "asset_master")
    forecasts = read_frame(paths, "forecast_panel")
    rankings = read_frame(paths, "ranking_panel")
    trade_plans = read_frame(paths, "trade_plan_panel")
    backtests = read_frame(paths, "backtest_panel")
    data_health = read_frame(paths, "data_health")
    bars_1d = read_frame(paths, "bars_1d")
    bars_1h = read_frame(paths, "bars_1h")
    universes = read_json(paths.reference_dir / "universes_reference.json", [])
    sort_lookup = _build_sort_lookup(paths, asset_master, rankings, bars_1d, bars_1h)
    allowed_crypto_symbols = _allowed_crypto_symbols(paths)

    stale_map = _stale_lookup_from_health(data_health)
    universes_items = _with_publish_context(universes, context, stale_map)
    universe_lookup = _coverage_lookup(universes_items)

    asset_items: list[dict] = []
    for item in _serialize_frame(asset_master):
        if str(item.get("market")) == "crypto" and str(item.get("symbol")) not in allowed_crypto_symbols:
            continue
        sort_meta = sort_lookup.get((str(item.get("market")), str(item.get("symbol"))), {})
        row = {
            **item,
            "sortRank": int(sort_meta.get("sortRank", 999999)),
            "sortMetric": sort_meta.get("sortMetric"),
            "sortMetricLabel": str(sort_meta.get("sortMetricLabel", "Symbol order")),
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(stale_map.get(str(item.get("market")), False)),
        }
        asset_items.append(row)
    forecast_items: list[dict] = []
    for item in _serialize_frame(forecasts):
        if str(item.get("market")) == "crypto" and str(item.get("symbol")) not in allowed_crypto_symbols:
            continue
        universe_meta = universe_lookup.get(str(item.get("universe")), {})
        row = {
            **item,
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(universe_meta.get("stale", stale_map.get(str(item.get("market")), False))),
            "coverageMode": universe_meta.get("coverageMode", "point_in_time"),
            "coveragePct": float(universe_meta.get("coveragePct", 100.0)),
        }
        forecast_items.append(row)

    ranking_items: list[dict] = []
    for item in _serialize_frame(rankings):
        market = str(item.get("market", ""))
        if not market:
            market = str(asset_master.loc[asset_master["symbol"] == item.get("symbol"), "market"].iloc[0]) if not asset_master.loc[asset_master["symbol"] == item.get("symbol")].empty else ""
        if market == "crypto" and str(item.get("symbol")) not in allowed_crypto_symbols:
            continue
        universe_meta = universe_lookup.get(str(item.get("universe")), {})
        row = {
            **item,
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(universe_meta.get("stale", False)),
            "coverageMode": universe_meta.get("coverageMode", "point_in_time"),
            "coveragePct": float(universe_meta.get("coveragePct", 100.0)),
        }
        ranking_items.append(row)

    trade_plan_items: list[dict] = []
    for item in _serialize_frame(trade_plans):
        if str(item.get("market")) == "crypto" and str(item.get("symbol")) not in allowed_crypto_symbols:
            continue
        universe_meta = universe_lookup.get(str(item.get("universe")), {})
        sort_meta = sort_lookup.get((str(item.get("market")), str(item.get("symbol"))), {})
        row = {
            **item,
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(universe_meta.get("stale", False)),
            "coverageMode": universe_meta.get("coverageMode", "point_in_time"),
            "coveragePct": float(universe_meta.get("coveragePct", 100.0)),
            "sortRank": int(sort_meta.get("sortRank", 999999)),
            "sortMetric": sort_meta.get("sortMetric"),
            "sortMetricLabel": str(sort_meta.get("sortMetricLabel", "Symbol order")),
        }
        trade_plan_items.append(row)

    backtest_items: list[dict] = []
    for item in _serialize_frame(backtests):
        market = str(item.get("strategyId", "")).split("-", 1)[0]
        row = {
            **item,
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(stale_map.get(market, False)),
        }
        backtest_items.append(row)

    data_health_items = _with_publish_context(_serialize_frame(data_health), context, stale_map)

    write_json(paths.published_dir / "universes.json", {"items": universes_items})
    write_json(paths.published_dir / "assets.json", {"items": asset_items})
    write_json(paths.published_dir / "forecasts.json", {"items": forecast_items})
    write_json(paths.published_dir / "rankings.json", {"items": ranking_items})
    write_json(paths.published_dir / "trade-plans.json", {"items": trade_plan_items})
    write_json(paths.published_dir / "trade-plans-active.json", {"items": [item for item in trade_plan_items if bool(item.get("actionable"))]})
    write_json(paths.published_dir / "trade-plans-inactive.json", {"items": [item for item in trade_plan_items if not bool(item.get("actionable"))]})
    write_json(paths.published_dir / "backtests.json", {"items": backtest_items})
    write_json(paths.published_dir / "data-health.json", {"items": data_health_items})
    report_manifest = generate_report_bundle(paths.exports_dir, published_dir=paths.published_dir)
    write_json(paths.published_dir / "report-manifest.json", report_manifest)


def refresh_market(paths: AppPaths, market: str, years: int = 5, limit: int | None = None, *, fast: bool = False, full: bool = False) -> None:
    try:
        ingest_market(paths, market=market, years=years, limit=limit)
    except Exception as exc:
        _mark_stale(paths, market, str(exc))
    build_baseline_signals(paths, fast=fast, full=full)
    build_ml_signals(paths, fast=fast, full=full)
    backtest_models(paths)
    _publish_context(paths)
    publish_real(paths, renew_context=False)


def refresh_real(paths: AppPaths, years: int = 5, limit: int | None = None, *, fast: bool = False, full: bool = False) -> None:
    for market in MARKET_REFRESH_ORDER:
        try:
            ingest_market(paths, market=market, years=years, limit=limit)
        except Exception as exc:
            _mark_stale(paths, market, str(exc))
    build_baseline_signals(paths, fast=fast, full=full)
    build_ml_signals(paths, fast=fast, full=full)
    backtest_models(paths)
    _publish_context(paths)
    publish_real(paths, renew_context=False)
