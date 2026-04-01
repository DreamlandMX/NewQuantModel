from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from newquantmodel.analytics.backtest import build_backtests
from newquantmodel.analytics.signals import build_signal_panel
from newquantmodel.analytics.trade_plans import build_trade_plan_panel
from newquantmodel.config.settings import AppPaths
from newquantmodel.ingestion.real_data import ingest_crypto, ingest_equities, persist_ingest_result
from newquantmodel.models.pipeline import BASELINE_MODEL_VERSION, bootstrap_baseline_outputs, build_ml_overlay
from newquantmodel.reporting.report_bundle import generate_report_bundle
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel.storage.parquet_store import read_frame, sync_duckdb, write_frame


MARKET_REFRESH_ORDER = ["crypto", "cn_equity", "us_equity"]
MARKET_STALE_GROUPS = {
    "crypto": ["crypto"],
    "cn_equity": ["cn_equity", "index"],
    "us_equity": ["us_equity", "index"],
}


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


def _mark_stale(paths: AppPaths, market: str, reason: str) -> None:
    stale_markets = MARKET_STALE_GROUPS.get(market, [market])
    data_health = read_frame(paths, "data_health")
    if not data_health.empty:
        for stale_market in stale_markets:
            mask = data_health["market"] == stale_market
            if not mask.any():
                continue
            data_health.loc[mask, "stale"] = True
            data_health.loc[mask, "notes"] = data_health.loc[mask, "notes"].apply(
                lambda notes: [*notes, f"stale fallback: {reason}"] if isinstance(notes, list) else [f"stale fallback: {reason}"]
            )
        write_frame(paths, "data_health", data_health)

    universes = read_json(paths.reference_dir / "universes_reference.json", [])
    if universes:
        frame = pd.DataFrame(universes)
        if not frame.empty:
            for stale_market in stale_markets:
                mask = frame["market"] == stale_market
                if not mask.any():
                    continue
                frame.loc[mask, "stale"] = True
                policy_notes = []
                for notes in frame.loc[mask, "policyNotes"].tolist():
                    if isinstance(notes, list):
                        policy_notes.append([*notes, f"stale fallback: {reason}"])
                    else:
                        policy_notes.append([f"stale fallback: {reason}"])
                frame.loc[mask, "policyNotes"] = policy_notes
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


def build_baseline_signals(paths: AppPaths) -> None:
    bars_1d = read_frame(paths, "bars_1d")
    signal_panel = build_signal_panel(bars_1d)
    write_frame(paths, "signal_panel", signal_panel)
    bootstrap_baseline_outputs(paths)
    build_trade_plans(paths)
    sync_duckdb(paths)


def build_ml_signals(paths: AppPaths) -> None:
    build_ml_overlay(paths)
    build_trade_plans(paths)


def build_trade_plans(paths: AppPaths) -> None:
    asset_master = read_frame(paths, "asset_master")
    forecasts = read_frame(paths, "forecast_panel")
    rankings = read_frame(paths, "ranking_panel")
    bars_1d = read_frame(paths, "bars_1d")
    bars_1h = read_frame(paths, "bars_1h")
    universes = read_json(paths.reference_dir / "universes_reference.json", [])
    trade_plans = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=universes)
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
    universes = read_json(paths.reference_dir / "universes_reference.json", [])

    stale_map = _stale_lookup_from_health(data_health)
    universes_items = _with_publish_context(universes, context, stale_map)
    universe_lookup = _coverage_lookup(universes_items)

    asset_items = _with_publish_context(_serialize_frame(asset_master), context, stale_map)
    forecast_items: list[dict] = []
    for item in _serialize_frame(forecasts):
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
        universe_meta = universe_lookup.get(str(item.get("universe")), {})
        row = {
            **item,
            "publishedAt": context["publishedAt"],
            "dataSnapshotVersion": context["dataSnapshotVersion"],
            "stale": bool(universe_meta.get("stale", False)),
            "coverageMode": universe_meta.get("coverageMode", "point_in_time"),
            "coveragePct": float(universe_meta.get("coveragePct", 100.0)),
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
    write_json(paths.published_dir / "backtests.json", {"items": backtest_items})
    write_json(paths.published_dir / "data-health.json", {"items": data_health_items})
    report_manifest = generate_report_bundle(paths.exports_dir, published_dir=paths.published_dir)
    write_json(paths.published_dir / "report-manifest.json", report_manifest)


def refresh_market(paths: AppPaths, market: str, years: int = 5, limit: int | None = None) -> None:
    try:
        ingest_market(paths, market=market, years=years, limit=limit)
    except Exception as exc:
        _mark_stale(paths, market, str(exc))
    build_baseline_signals(paths)
    build_ml_signals(paths)
    backtest_models(paths)
    _publish_context(paths)
    publish_real(paths, renew_context=False)


def refresh_real(paths: AppPaths, years: int = 5, limit: int | None = None) -> None:
    for market in MARKET_REFRESH_ORDER:
        try:
            ingest_market(paths, market=market, years=years, limit=limit)
        except Exception as exc:
            _mark_stale(paths, market, str(exc))
    build_baseline_signals(paths)
    build_ml_signals(paths)
    backtest_models(paths)
    _publish_context(paths)
    publish_real(paths, renew_context=False)
