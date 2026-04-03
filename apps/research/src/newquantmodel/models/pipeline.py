from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression, LogisticRegression

try:  # pragma: no cover - imported opportunistically for runtime environment
    from lightgbm import LGBMClassifier, LGBMRanker, LGBMRegressor
except Exception:  # pragma: no cover - fallback path
    LGBMClassifier = None
    LGBMRanker = None
    LGBMRegressor = None

from newquantmodel.analytics.signals import build_rankings_and_forecasts, enrich_with_technical_indicators, indicator_payload_from_row
from newquantmodel.analytics.factor_library import (
    CRYPTO_FACTOR_COLUMNS,
    CRYPTO_HOURLY_FACTOR_COLUMNS,
    EQUITY_FACTOR_COLUMNS,
    INDEX_FACTOR_COLUMNS,
    candidate_factor_columns,
)
from newquantmodel.config.settings import AppPaths
from newquantmodel.models.genetic import (
    GAConfig,
    decode_feature_subset,
    decode_weight_map,
    ga_summary_json,
    run_genetic_search,
    score_candidate_history,
)
from newquantmodel.storage.parquet_store import (
    read_frame,
    replace_rows_by_keys,
    sync_duckdb,
    write_frame,
)


STOCK_MODEL_VERSION = "equity-lgbm-ranker-ga-mf-v2"
CRYPTO_MODEL_VERSION = "crypto-ts-ga-mf-v2"
INDEX_MODEL_VERSION = "index-regime-ga-mf-v2"
BASELINE_MODEL_VERSION = "baseline-ga-mf-v2"

STOCK_FEATURES = EQUITY_FACTOR_COLUMNS
CRYPTO_SIGNAL_FEATURES = CRYPTO_FACTOR_COLUMNS
CRYPTO_HOURLY_FEATURES = CRYPTO_HOURLY_FACTOR_COLUMNS
INDEX_FEATURES = INDEX_FACTOR_COLUMNS

DAILY_HORIZON_TO_PERIODS = {"1D": 1, "5D": 5, "20D": 20}
WEEKLY_HORIZON_TO_PERIODS = {"1W": 1, "5W": 5, "20W": 20}
CRYPTO_HORIZON_TO_PERIODS = {"1H": 1, "4H": 4, "1D": 24}
CRYPTO_WEEKLY_HORIZON_TO_PERIODS = {"1W": 1}
SIGNAL_FREQUENCIES = ["daily", "weekly"]
GENETIC_CONFIG = GAConfig()


@dataclass(slots=True)
class MarketPredictionBundle:
    rankings: pd.DataFrame
    forecasts: pd.DataFrame
    history: pd.DataFrame
    runs: pd.DataFrame


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _sigmoid(value: float) -> float:
    value = max(min(value, 12.0), -12.0)
    return 1.0 / (1.0 + math.exp(-value))


def _clamp_probability(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _zscore_by_date(frame: pd.DataFrame, column: str) -> pd.Series:
    grouped = frame.groupby("timestamp")[column]
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, np.nan)
    return ((frame[column] - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _series_future_return(series: pd.Series, periods: int) -> pd.Series:
    return series.shift(-periods) / series - 1.0


def _ensure_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    return frame


def _artifact_path(paths: AppPaths, market: str, stem: str) -> Path:
    trained_slug = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    target_dir = paths.models_dir / trained_slug / market
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / stem


def _feature_importance_map(model: object, feature_cols: list[str]) -> dict[str, float]:
    importances: np.ndarray
    if hasattr(model, "feature_importances_"):
        importances = np.asarray(getattr(model, "feature_importances_"), dtype="float64")
    elif hasattr(model, "coef_"):
        coef = np.asarray(getattr(model, "coef_"), dtype="float64")
        importances = np.abs(coef.ravel())
    else:
        importances = np.ones(len(feature_cols), dtype="float64")
    if importances.size != len(feature_cols):
        importances = np.ones(len(feature_cols), dtype="float64")
    if float(importances.sum() or 0.0) <= 0.0:
        importances = np.ones(len(feature_cols), dtype="float64")
    importances = importances / importances.sum()
    return {feature: float(value) for feature, value in zip(feature_cols, importances)}


def _breakdown_from_row(row: pd.Series, importance_map: dict[str, float], name_map: dict[str, str]) -> dict[str, float]:
    contributions = {
        name_map.get(feature, feature): float(row.get(feature, 0.0) or 0.0) * float(weight)
        for feature, weight in importance_map.items()
    }
    normalizer = sum(abs(value) for value in contributions.values()) or 1.0
    return {key: float(value / normalizer) for key, value in contributions.items()}


def _liquidity_bucket(volume: float, median_volume: float) -> str:
    if median_volume <= 0:
        return "medium"
    if volume >= median_volume * 1.5:
        return "high"
    if volume <= median_volume * 0.5:
        return "low"
    return "medium"


def _normalize_weights(scores: pd.Series, strategy_mode: str) -> pd.Series:
    if scores.empty:
        return pd.Series(dtype="float64")
    positive = scores.clip(lower=0)
    long_denominator = float(positive.sum() or 1.0)
    long_weights = positive / long_denominator
    if strategy_mode == "long_only":
        return long_weights
    negative = scores.clip(upper=0).abs()
    short_denominator = float(negative.sum() or 1.0)
    short_weights = negative / short_denominator
    return long_weights - short_weights


def _safe_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    try:
        from sklearn.metrics import roc_auc_score

        return float(roc_auc_score(y_true, y_prob))
    except Exception:
        return None


def _safe_pinball(y_true: np.ndarray, y_pred: np.ndarray, alpha: float) -> float | None:
    if len(y_true) == 0:
        return None
    try:
        from sklearn.metrics import mean_pinball_loss

        return float(mean_pinball_loss(y_true, y_pred, alpha=alpha))
    except Exception:
        return None


def _group_dates(frame: pd.DataFrame) -> list[pd.Timestamp]:
    return sorted(pd.to_datetime(frame["timestamp"], utc=True).dropna().drop_duplicates().tolist())


def _window_params(unique_dates: list[pd.Timestamp], preferred_step: int) -> tuple[int, int]:
    if not unique_dates:
        return 0, 1
    min_train = max(30, min(252, len(unique_dates) // 2))
    if len(unique_dates) <= min_train:
        min_train = max(10, len(unique_dates) // 2)
    step = max(1, min(preferred_step, max(1, len(unique_dates) // 8)))
    return min_train, step


def _market_universe_map(universe_membership: pd.DataFrame) -> dict[str, tuple[str, list[str]]]:
    if universe_membership.empty:
        return {}
    current_membership = universe_membership.sort_values("effective_from").drop_duplicates(subset=["symbol", "universe"], keep="last")
    grouped = current_membership.groupby("universe")
    return {
        universe: (str(group["market"].iloc[0]), sorted(group["symbol"].dropna().astype(str).unique().tolist()))
        for universe, group in grouped
    }


def _horizon_periods_for_frequency(signal_frequency: str, market: str) -> dict[str, int]:
    if market == "crypto":
        return {"1D": 1} if signal_frequency == "daily" else CRYPTO_WEEKLY_HORIZON_TO_PERIODS
    return DAILY_HORIZON_TO_PERIODS if signal_frequency == "daily" else WEEKLY_HORIZON_TO_PERIODS


def _feature_candidates_for_market(market: str, signal_frequency: str) -> list[str]:
    return [column for column in candidate_factor_columns(market, "hourly" if market == "crypto" and signal_frequency == "hourly" else signal_frequency)]


def _trim_frame_for_optimization(frame: pd.DataFrame, signal_frequency: str) -> pd.DataFrame:
    if frame.empty or "timestamp" not in frame.columns:
        return frame
    unique_dates = _group_dates(frame)
    if not unique_dates:
        return frame
    if signal_frequency == "hourly":
        keep_dates = unique_dates[-720:]
    elif signal_frequency == "weekly":
        keep_dates = unique_dates[-120:]
    else:
        keep_dates = unique_dates[-220:]
    return frame[frame["timestamp"].isin(keep_dates)].copy()


def _frame_signature(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty or "timestamp" not in frame.columns:
        return {"latestTimestamp": None, "rows": 0}
    return {
        "latestTimestamp": pd.Timestamp(frame["timestamp"].max()).isoformat(),
        "rows": int(len(frame)),
    }


def _ga_config_for_frame(frame: pd.DataFrame, dimensions: int) -> GAConfig:
    if os.getenv("NQM_GA_FAST", "0") == "1":
        return GAConfig(population=8, generations=6, patience=3, seed=7)
    rows = len(frame)
    if rows >= 250_000 or dimensions >= 60:
        return GAConfig(population=12, generations=8, patience=4, seed=7)
    if rows >= 75_000 or dimensions >= 40:
        return GAConfig(population=16, generations=12, patience=5, seed=7)
    if rows >= 25_000:
        return GAConfig(population=20, generations=16, patience=6, seed=7)
    return GENETIC_CONFIG


def _parse_json_object(value: object) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _find_cached_ga_row(
    ga_runs: pd.DataFrame,
    *,
    market: str,
    pipeline: str,
    signal_frequency: str,
    model_version: str,
    latest_timestamp: str | None,
) -> pd.Series | None:
    if ga_runs.empty:
        return None
    scoped = ga_runs[
        (ga_runs["market"] == market)
        & (ga_runs["pipeline"] == pipeline)
        & (ga_runs["signalFrequency"] == signal_frequency)
        & (ga_runs["modelVersion"] == model_version)
    ].copy()
    if scoped.empty:
        return None
    if latest_timestamp is not None and "latestTimestamp" in scoped.columns:
        scoped = scoped[scoped["latestTimestamp"].astype(str) == latest_timestamp]
        if scoped.empty:
            return None
    scoped = scoped.sort_values("trainedAt")
    return scoped.iloc[-1]


def _decode_model_params(chromosome: np.ndarray, feature_count: int) -> dict[str, float | int]:
    tail = chromosome[feature_count:]
    if tail.size < 6:
        tail = np.pad(tail, (0, 6 - tail.size), constant_values=0.5)
    return {
        "n_estimators": int(80 + round(tail[0] * 140)),
        "learning_rate": float(0.03 + tail[1] * 0.12),
        "num_leaves": int(15 + round(tail[2] * 48)),
        "min_child_samples": int(8 + round(tail[3] * 24)),
        "subsample": float(0.60 + tail[4] * 0.40),
        "colsample_bytree": float(0.60 + tail[5] * 0.40),
    }


def _decode_baseline_genes(chromosome: np.ndarray, factor_names: list[str], market: str) -> tuple[dict[str, float], int, float]:
    weights = decode_weight_map(chromosome, factor_names)
    tail = chromosome[len(factor_names) * 2 :]
    if tail.size < 2:
        tail = np.pad(tail, (0, 2 - tail.size), constant_values=0.5)
    top_bounds = {"crypto": (3, 12), "cn_equity": (5, 30), "us_equity": (5, 25), "index": (1, 5)}
    low, high = top_bounds.get(market, (3, 10))
    top_n = int(low + round(tail[0] * max(high - low, 1)))
    max_position = float(0.04 + tail[1] * 0.16)
    return weights, top_n, max_position


def _apply_weighted_score(frame: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    score = pd.Series(0.0, index=frame.index, dtype="float64")
    for column, weight in weights.items():
        if column in frame.columns:
            score = score + pd.to_numeric(frame[column], errors="coerce").fillna(0.0) * float(weight)
    return score


def _optimize_baseline_spec(
    frame: pd.DataFrame,
    market: str,
    signal_frequency: str,
    *,
    ga_runs: pd.DataFrame | None = None,
    reuse_cached: bool = True,
) -> dict[str, object]:
    optimization_frame = _trim_frame_for_optimization(frame, signal_frequency)
    factor_names = [column for column in _feature_candidates_for_market(market, signal_frequency) if column in optimization_frame.columns]
    if not factor_names:
        return {"weights": {}, "top_n": 10, "max_position": 0.10, "fitness": 0.0, "metrics": {}, "selected": []}
    eval_frame = optimization_frame.dropna(subset=["timestamp", "symbol"]).copy()
    signature = _frame_signature(eval_frame)
    cached_row = (
        _find_cached_ga_row(
            ga_runs if ga_runs is not None else pd.DataFrame(),
            market=market,
            pipeline="baseline-ga",
            signal_frequency=signal_frequency,
            model_version=BASELINE_MODEL_VERSION,
            latest_timestamp=str(signature["latestTimestamp"]),
        )
        if reuse_cached
        else None
    )
    if cached_row is not None:
        config_payload = _parse_json_object(cached_row.get("config"))
        metric_payload = _parse_json_object(cached_row.get("metricSummary"))
        weights = config_payload.get("weights", {})
        if isinstance(weights, dict) and weights:
            return {
                "weights": {str(key): float(value) for key, value in weights.items()},
                "top_n": int(config_payload.get("top_n", 10)),
                "max_position": float(config_payload.get("max_position", 0.10)),
                "fitness": float(cached_row.get("fitness", 0.0) or 0.0),
                "metrics": metric_payload.get("metrics", {}) if isinstance(metric_payload.get("metrics"), dict) else {},
                "selected": list(metric_payload.get("selected", config_payload.get("selected", []))),
                "summary": str(cached_row.get("metricSummary") or json.dumps({})),
                "latestTimestamp": signature["latestTimestamp"],
                "cacheHit": True,
            }
    target_col = f"target_{list(_horizon_periods_for_frequency(signal_frequency, market))[0]}"

    def _evaluator(chromosome: np.ndarray) -> tuple[float, dict[str, float], dict[str, object]]:
        weights, top_n, max_position = _decode_baseline_genes(chromosome, factor_names, market)
        scored = eval_frame[["timestamp", "symbol", target_col, *factor_names]].copy()
        scored["score"] = _apply_weighted_score(scored, weights)
        cutoff_dates = _group_dates(scored)
        if len(cutoff_dates) > 30:
            scored = scored[scored["timestamp"].isin(cutoff_dates[len(cutoff_dates) // 3 :])].copy()
        selected = [name for name, weight in weights.items() if abs(weight) > 1e-6]
        fitness, metrics = score_candidate_history(
            scored,
            market=signal_frequency,
            target_col=target_col,
            top_n=top_n,
            feature_count=len(selected),
            total_features=len(factor_names),
        )
        return fitness, metrics, {"selected": selected, "top_n": top_n, "max_position": max_position, "weights": weights}

    ga_config = _ga_config_for_frame(eval_frame, len(factor_names) * 2 + 2)
    result = run_genetic_search(dimensions=len(factor_names) * 2 + 2, evaluator=_evaluator, config=ga_config)
    payload = result.payload
    return {
        "weights": payload.get("weights", {}),
        "top_n": int(payload.get("top_n", 10)),
        "max_position": float(payload.get("max_position", 0.10)),
        "fitness": result.fitness,
        "metrics": result.metrics,
        "selected": payload.get("selected", []),
        "latestTimestamp": signature["latestTimestamp"],
        "cacheHit": False,
        "summary": ga_summary_json(
            result,
            selected=list(payload.get("selected", [])),
            extra={"market": market, "signalFrequency": signal_frequency, "rowsOptimized": int(len(eval_frame)), "gaBudget": {"population": ga_config.population, "generations": ga_config.generations}},
        ),
    }


def _optimize_ml_feature_subset(
    frame: pd.DataFrame,
    market: str,
    signal_frequency: str,
    *,
    evaluator_builder,
    ga_runs: pd.DataFrame | None = None,
    pipeline_name: str = "ml-ga",
    model_version: str = STOCK_MODEL_VERSION,
    reuse_cached: bool = True,
) -> tuple[list[str], dict[str, float | int], str, dict[str, float], str | None]:
    optimization_frame = _trim_frame_for_optimization(frame, signal_frequency)
    feature_names = [column for column in _feature_candidates_for_market(market, signal_frequency) if column in optimization_frame.columns]
    if not feature_names:
        return [], {}, json.dumps({}), {}, None
    signature = _frame_signature(optimization_frame)
    cached_row = (
        _find_cached_ga_row(
            ga_runs if ga_runs is not None else pd.DataFrame(),
            market=market,
            pipeline=pipeline_name,
            signal_frequency=signal_frequency,
            model_version=model_version,
            latest_timestamp=str(signature["latestTimestamp"]),
        )
        if reuse_cached
        else None
    )
    if cached_row is not None:
        config_payload = _parse_json_object(cached_row.get("config"))
        selected = json.loads(str(cached_row.get("selectedFactors") or "[]"))
        selected_list = [feature for feature in selected if feature in feature_names]
        if selected_list:
            return selected_list, dict(config_payload), str(cached_row.get("metricSummary") or json.dumps({})), {"fitness": float(cached_row.get("fitness", 0.0) or 0.0)}, str(signature["latestTimestamp"])

    def _evaluator(chromosome: np.ndarray) -> tuple[float, dict[str, float], dict[str, object]]:
        selected = decode_feature_subset(chromosome, feature_names)
        model_kwargs = _decode_model_params(chromosome, len(feature_names))
        fitness, metrics = evaluator_builder(optimization_frame, selected, model_kwargs)
        return fitness, metrics, {"selected": selected, "model_kwargs": model_kwargs}

    ga_config = _ga_config_for_frame(optimization_frame, len(feature_names) + 6)
    result = run_genetic_search(dimensions=len(feature_names) + 6, evaluator=_evaluator, config=ga_config)
    payload = result.payload
    summary = ga_summary_json(
        result,
        selected=list(payload.get("selected", [])),
        extra={
            "market": market,
            "signalFrequency": signal_frequency,
            "modelKwargs": payload.get("model_kwargs", {}),
            "rowsOptimized": int(len(optimization_frame)),
            "gaBudget": {"population": ga_config.population, "generations": ga_config.generations},
        },
    )
    return list(payload.get("selected", feature_names[: min(6, len(feature_names))])), dict(payload.get("model_kwargs", {})), summary, result.metrics, str(signature["latestTimestamp"])


def _frequency_provenance(signal_frequency: str) -> dict[str, object]:
    return {
        "signalFrequency": signal_frequency,
        "sourceFrequency": signal_frequency,
        "isDerivedSignal": False,
    }


def _build_crypto_hourly_panel(bars_1h: pd.DataFrame) -> pd.DataFrame:
    if bars_1h.empty:
        return pd.DataFrame()
    from newquantmodel.analytics.factor_library import build_market_factor_panel

    frame = build_market_factor_panel(
        bars_1h,
        market="crypto",
        signal_frequency="hourly",
        source_frequency="hourly",
    )
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("symbol", group_keys=False)
    for horizon, periods in CRYPTO_HORIZON_TO_PERIODS.items():
        frame[f"target_{horizon}"] = grouped["close"].transform(lambda series, p=periods: _series_future_return(series, p))
        frame[f"class_{horizon}"] = (frame[f"target_{horizon}"] > 0).astype(int)
    return frame


def _prepare_stock_panel(signal_panel: pd.DataFrame, market: str, signal_frequency: str) -> pd.DataFrame:
    frame = signal_panel[(signal_panel["market"] == market) & (signal_panel["signalFrequency"] == signal_frequency)].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    horizon_periods = _horizon_periods_for_frequency(signal_frequency, market)
    grouped = frame.groupby("symbol", group_keys=False)
    ordered_horizons = list(horizon_periods)
    horizon_weights = [1.0] if len(ordered_horizons) == 1 else [0.20, 0.30, 0.50][: len(ordered_horizons)]
    normalized_weights = np.asarray(horizon_weights, dtype="float64")
    normalized_weights = normalized_weights / normalized_weights.sum()
    target_alpha = pd.Series(0.0, index=frame.index, dtype="float64")
    for horizon, periods in horizon_periods.items():
        frame[f"target_{horizon}"] = grouped["close"].transform(lambda series, p=periods: _series_future_return(series, p))
    for weight, horizon in zip(normalized_weights, ordered_horizons, strict=False):
        periods = horizon_periods[horizon]
        target_alpha += weight * (frame[f"target_{horizon}"].fillna(0.0) / max(periods, 1))
    frame["target_alpha"] = target_alpha
    frame["rank_label"] = 0
    for timestamp, group in frame.groupby("timestamp"):
        percentile = group["target_alpha"].rank(method="first", pct=True, ascending=True)
        relevance = np.floor(percentile * 5).astype(int).clip(0, 4)
        frame.loc[group.index, "rank_label"] = relevance
    return frame


def _prepare_crypto_panel(signal_panel: pd.DataFrame, signal_frequency: str) -> pd.DataFrame:
    frame = signal_panel[(signal_panel["market"] == "crypto") & (signal_panel["signalFrequency"] == signal_frequency)].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    horizon_periods = _horizon_periods_for_frequency(signal_frequency, "crypto")
    grouped = frame.groupby("symbol", group_keys=False)
    for horizon, periods in horizon_periods.items():
        frame[f"target_{horizon}"] = grouped["close"].transform(lambda series, p=periods: _series_future_return(series, p))
    return frame


def _prepare_index_panel(signal_panel: pd.DataFrame, signal_frequency: str) -> pd.DataFrame:
    frame = signal_panel[(signal_panel["market"] == "index") & (signal_panel["signalFrequency"] == signal_frequency)].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    horizon_periods = _horizon_periods_for_frequency(signal_frequency, "index")
    grouped = frame.groupby("symbol", group_keys=False)
    for horizon, periods in horizon_periods.items():
        frame[f"target_{horizon}"] = grouped["close"].transform(lambda series, p=periods: _series_future_return(series, p))
        frame[f"class_{horizon}"] = (frame[f"target_{horizon}"] > 0).astype(int)
    return frame


def _make_regressor(**kwargs):
    objective = kwargs.pop("objective", "regression")
    if LGBMRegressor is not None:
        return LGBMRegressor(
            objective=objective,
            n_estimators=kwargs.pop("n_estimators", 120),
            learning_rate=kwargs.pop("learning_rate", 0.05),
            num_leaves=kwargs.pop("num_leaves", 31),
            min_child_samples=kwargs.pop("min_child_samples", 12),
            subsample=kwargs.pop("subsample", 0.85),
            colsample_bytree=kwargs.pop("colsample_bytree", 0.85),
            verbosity=-1,
            random_state=7,
            **kwargs,
        )
    return LinearRegression()


def _make_classifier(**kwargs):
    if LGBMClassifier is not None:
        return LGBMClassifier(
            objective="binary",
            n_estimators=kwargs.pop("n_estimators", 120),
            learning_rate=kwargs.pop("learning_rate", 0.05),
            num_leaves=kwargs.pop("num_leaves", 31),
            min_child_samples=kwargs.pop("min_child_samples", 12),
            subsample=kwargs.pop("subsample", 0.85),
            colsample_bytree=kwargs.pop("colsample_bytree", 0.85),
            verbosity=-1,
            random_state=7,
            **kwargs,
        )
    return LogisticRegression(max_iter=500)


def _make_ranker(**kwargs):
    if LGBMRanker is not None:
        return LGBMRanker(
            objective="lambdarank",
            metric="ndcg",
            n_estimators=kwargs.pop("n_estimators", 140),
            learning_rate=kwargs.pop("learning_rate", 0.05),
            num_leaves=kwargs.pop("num_leaves", 31),
            min_child_samples=kwargs.pop("min_child_samples", 12),
            subsample=kwargs.pop("subsample", 0.85),
            colsample_bytree=kwargs.pop("colsample_bytree", 0.85),
            verbosity=-1,
            random_state=7,
            **kwargs,
        )
    return LinearRegression()


def _walk_forward_regression(
    frame: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    *,
    preferred_step: int,
    market: str,
    model_version: str,
    signal_frequency: str,
    model_kwargs: dict[str, float | int] | None = None,
) -> pd.DataFrame:
    clean = frame.dropna(subset=[*feature_cols, target_col]).copy()
    if clean.empty:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "score", "predictedReturn", "modelVersion", "signalFrequency", "sourceFrequency", "isDerivedSignal"])

    dates = _group_dates(clean)
    min_train, step = _window_params(dates, preferred_step)
    rows: list[dict] = []

    for offset in range(min_train, len(dates), step):
        train_dates = dates[:offset]
        test_dates = dates[offset : offset + step]
        train = clean[clean["timestamp"].isin(train_dates)].copy()
        test = clean[clean["timestamp"].isin(test_dates)].copy()
        if train.empty or test.empty:
            continue
        model = _make_regressor(**(model_kwargs or {}))
        model.fit(train[feature_cols].fillna(0.0), train[target_col].astype("float64"))
        predictions = np.asarray(model.predict(test[feature_cols].fillna(0.0)), dtype="float64")
        for (_, row), prediction in zip(test.iterrows(), predictions, strict=False):
            rows.append(
                {
                    "market": market,
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"],
                    "score": float(prediction),
                    "predictedReturn": float(prediction),
                    "modelVersion": model_version,
                    **_frequency_provenance(signal_frequency),
                }
            )

    history = pd.DataFrame(rows)
    if history.empty:
        return history
    history["score"] = _zscore_by_date(history, "score")
    return history


def _walk_forward_ranker(
    frame: pd.DataFrame,
    feature_cols: list[str],
    *,
    horizons: list[str],
    horizon_periods: dict[str, int],
    market: str,
    model_version: str,
    signal_frequency: str,
    preferred_step: int = 20,
    model_kwargs: dict[str, float | int] | None = None,
) -> pd.DataFrame:
    clean = frame.dropna(subset=[*feature_cols, "target_alpha", "rank_label"]).copy()
    if clean.empty:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "score", "predictedReturn", "modelVersion", "signalFrequency", "sourceFrequency", "isDerivedSignal", *[f"pred_{horizon}" for horizon in horizons]])

    dates = _group_dates(clean)
    min_train, step = _window_params(dates, preferred_step)
    rows: list[dict] = []

    for offset in range(min_train, len(dates), step):
        train_dates = dates[:offset]
        test_dates = dates[offset : offset + step]
        train = clean[clean["timestamp"].isin(train_dates)].copy()
        test = clean[clean["timestamp"].isin(test_dates)].copy()
        if train.empty or test.empty:
            continue
        group_sizes = train.groupby("timestamp").size().tolist()
        if not group_sizes or min(group_sizes) <= 1:
            continue

        ranker = _make_ranker(**(model_kwargs or {}))
        if LGBMRanker is not None:
            ranker.fit(train[feature_cols].fillna(0.0), train["rank_label"], group=group_sizes)
        else:
            ranker.fit(train[feature_cols].fillna(0.0), train["target_alpha"].fillna(0.0))

        regressors = {}
        for horizon in horizons:
            regressor = _make_regressor(**(model_kwargs or {}))
            regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
            regressors[horizon] = regressor

        scores = np.asarray(ranker.predict(test[feature_cols].fillna(0.0)), dtype="float64")
        predictions_by_horizon = {
            horizon: np.asarray(regressors[horizon].predict(test[feature_cols].fillna(0.0)), dtype="float64")
            for horizon in horizons
        }
        horizon_weights = np.asarray([1.0] if len(horizons) == 1 else [0.20, 0.30, 0.50][: len(horizons)], dtype="float64")
        horizon_weights = horizon_weights / horizon_weights.sum()

        for index, (_, row) in enumerate(test.iterrows()):
            weighted_return = 0.0
            payload = {
                "market": market,
                "symbol": row["symbol"],
                "timestamp": row["timestamp"],
                "score": float(scores[index]),
                "modelVersion": model_version,
                **_frequency_provenance(signal_frequency),
            }
            for weight, horizon in zip(horizon_weights, horizons, strict=False):
                prediction = float(predictions_by_horizon[horizon][index])
                payload[f"pred_{horizon}"] = prediction
                weighted_return += weight * (prediction / max(horizon_periods[horizon], 1))
            rows.append(
                {
                    **payload,
                    "predictedReturn": float(weighted_return),
                }
            )

    history = pd.DataFrame(rows)
    if history.empty:
        return history
    history["score"] = _zscore_by_date(history, "score")
    return history


def _fit_stock_latest_models(
    frame: pd.DataFrame,
    feature_cols: list[str],
    market: str,
    paths: AppPaths,
    signal_frequency: str,
    horizon_periods: dict[str, int],
    model_kwargs: dict[str, float | int] | None = None,
) -> tuple[pd.DataFrame, dict[str, float], list[dict]]:
    clean = frame.dropna(subset=[*feature_cols, "target_alpha", "rank_label"]).copy()
    if clean.empty:
        raise ValueError(f"No clean stock training rows for {market}")

    group_sizes = clean.groupby("timestamp").size().tolist()
    ranker = _make_ranker(**(model_kwargs or {}))
    if LGBMRanker is not None:
        ranker.fit(clean[feature_cols].fillna(0.0), clean["rank_label"], group=group_sizes)
    else:
        ranker.fit(clean[feature_cols].fillna(0.0), clean["target_alpha"].fillna(0.0))

    horizons = list(horizon_periods)
    regressors = {}
    for horizon in horizons:
        regressor = _make_regressor(**(model_kwargs or {}))
        regressor.fit(clean[feature_cols].fillna(0.0), clean[f"target_{horizon}"].fillna(0.0))
        regressors[horizon] = regressor

    latest_ts = clean["timestamp"].max()
    latest = frame[frame["timestamp"] == latest_ts].dropna(subset=feature_cols).copy()
    if latest.empty:
        raise ValueError(f"No latest stock rows for {market}")

    latest["score"] = np.asarray(ranker.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["score"] = ((latest["score"] - latest["score"].mean()) / (latest["score"].std() or 1.0)).fillna(0.0)
    horizon_weights = np.asarray([1.0] if len(horizons) == 1 else [0.20, 0.30, 0.50][: len(horizons)], dtype="float64")
    horizon_weights = horizon_weights / horizon_weights.sum()
    latest["predictedReturn"] = 0.0
    for weight, horizon in zip(horizon_weights, horizons, strict=False):
        latest[f"pred_{horizon}"] = np.asarray(regressors[horizon].predict(latest[feature_cols].fillna(0.0)), dtype="float64")
        latest["predictedReturn"] = latest["predictedReturn"] + weight * (latest[f"pred_{horizon}"] / max(horizon_periods[horizon], 1))
    latest["signalFrequency"] = signal_frequency
    latest["sourceFrequency"] = signal_frequency
    latest["isDerivedSignal"] = False

    importance_map = _feature_importance_map(ranker, feature_cols)
    artifact = _artifact_path(paths, market, f"{signal_frequency}-ranker.joblib")
    joblib.dump(
        {
            "ranker": ranker,
            "regressors": regressors,
            "feature_cols": feature_cols,
            "importance_map": importance_map,
        },
        artifact,
    )

    metric_rows = [
        {
            "market": market,
            "pipeline": "equity-ranker",
            "horizon": "/".join(horizons),
            "modelVersion": STOCK_MODEL_VERSION,
            "status": "completed",
            "trainedAt": _now_utc().isoformat(),
            "trainStart": clean["timestamp"].min().isoformat(),
            "trainEnd": clean["timestamp"].max().isoformat(),
            "artifactPath": str(artifact),
            "fallbackUsed": False,
            "metricSummary": json.dumps(
                {
                    "rows": int(len(clean)),
                    "latestRows": int(len(latest)),
                    "signalFrequency": signal_frequency,
                    "meanPredictions": {horizon: float(latest[f"pred_{horizon}"].mean()) for horizon in horizons},
                }
            ),
            "message": "LightGBM ranker overlay completed",
        }
    ]
    return latest, importance_map, metric_rows


def _fit_crypto_latest(
    frame: pd.DataFrame,
    feature_cols: list[str],
    paths: AppPaths,
    signal_frequency: str,
    horizon: str,
    model_kwargs: dict[str, float | int] | None = None,
) -> tuple[pd.DataFrame, dict[str, float], list[dict]]:
    clean = frame.dropna(subset=[*feature_cols, f"target_{horizon}"]).copy()
    if clean.empty:
        raise ValueError(f"No clean crypto {signal_frequency} rows")
    regressor = _make_regressor(**(model_kwargs or {}))
    regressor.fit(clean[feature_cols].fillna(0.0), clean[f"target_{horizon}"].fillna(0.0))
    latest_ts = clean["timestamp"].max()
    latest = frame[frame["timestamp"] == latest_ts].dropna(subset=feature_cols).copy()
    if latest.empty:
        raise ValueError(f"No latest crypto {signal_frequency} rows")
    latest["score"] = np.asarray(regressor.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["score"] = ((latest["score"] - latest["score"].mean()) / (latest["score"].std() or 1.0)).fillna(0.0)
    latest[f"pred_{horizon}"] = latest["score"] * latest["ret_1d"].abs().replace(0.0, latest["ret_1d"].abs().median() or 0.01)
    latest["signalFrequency"] = signal_frequency
    latest["sourceFrequency"] = signal_frequency
    latest["isDerivedSignal"] = False
    importance_map = _feature_importance_map(regressor, feature_cols)
    artifact = _artifact_path(paths, "crypto", f"{signal_frequency}-regressor.joblib")
    joblib.dump(
        {
            "regressor": regressor,
            "feature_cols": feature_cols,
            "importance_map": importance_map,
        },
        artifact,
    )
    metric_rows = [
        {
            "market": "crypto",
            "pipeline": f"crypto-{signal_frequency}",
            "horizon": horizon,
            "modelVersion": CRYPTO_MODEL_VERSION,
            "status": "completed",
            "trainedAt": _now_utc().isoformat(),
            "trainStart": clean["timestamp"].min().isoformat(),
            "trainEnd": clean["timestamp"].max().isoformat(),
            "artifactPath": str(artifact),
            "fallbackUsed": False,
            "metricSummary": json.dumps(
                {
                    "rows": int(len(clean)),
                    "latestRows": int(len(latest)),
                    "signalFrequency": signal_frequency,
                    "meanPrediction": float(latest[f"pred_{horizon}"].mean()),
                }
            ),
            "message": f"Crypto {signal_frequency} overlay completed",
        }
    ]
    return latest, importance_map, metric_rows


def _fit_calibrated_classifier(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    model_kwargs: dict[str, float | int] | None = None,
) -> tuple[object, IsotonicRegression | None, float | None]:
    classifier = _make_classifier(**(model_kwargs or {}))
    classifier.fit(train[feature_cols].fillna(0.0), train[target_col].astype(int))
    if validation.empty or validation[target_col].nunique(dropna=True) < 2:
        return classifier, None, None
    raw = np.asarray(classifier.predict_proba(validation[feature_cols].fillna(0.0))[:, 1], dtype="float64")
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(raw, validation[target_col].astype("float64"))
    return classifier, calibrator, _safe_auc(validation[target_col].to_numpy(dtype="int64"), raw)


def _predict_probability(classifier: object, calibrator: IsotonicRegression | None, features: pd.DataFrame) -> np.ndarray:
    if hasattr(classifier, "predict_proba"):
        raw = np.asarray(classifier.predict_proba(features.fillna(0.0))[:, 1], dtype="float64")
    else:
        raw = np.asarray(classifier.predict(features.fillna(0.0)), dtype="float64")
        raw = 1.0 / (1.0 + np.exp(-raw))
    if calibrator is None:
        return raw
    return np.asarray(calibrator.transform(raw), dtype="float64")


def _fit_crypto_hourly_latest(
    panel: pd.DataFrame,
    paths: AppPaths,
    feature_cols: list[str] | None = None,
    model_kwargs: dict[str, float | int] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    if panel.empty:
        raise ValueError("No crypto hourly panel")
    feature_cols = feature_cols or CRYPTO_HOURLY_FEATURES
    latest_rows: list[pd.DataFrame] = []
    run_rows: list[dict] = []

    for horizon in CRYPTO_HORIZON_TO_PERIODS:
        clean = panel.dropna(subset=[*feature_cols, f"target_{horizon}"]).copy()
        if clean.empty:
            continue
        split_index = max(int(len(clean) * 0.8), 1)
        train = clean.iloc[:split_index].copy()
        validation = clean.iloc[split_index:].copy()
        if train.empty:
            train = clean.copy()
            validation = clean.iloc[0:0].copy()

        classifier, calibrator, auc = _fit_calibrated_classifier(train, validation, feature_cols, f"class_{horizon}", model_kwargs)
        mean_regressor = _make_regressor(**(model_kwargs or {}))
        mean_regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))

        lower_regressor = _make_regressor(objective="quantile", alpha=0.10, **(model_kwargs or {})) if LGBMRegressor is not None else _make_regressor(**(model_kwargs or {}))
        upper_regressor = _make_regressor(objective="quantile", alpha=0.90, **(model_kwargs or {})) if LGBMRegressor is not None else _make_regressor(**(model_kwargs or {}))
        lower_regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
        upper_regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))

        latest = panel.sort_values("timestamp").groupby("symbol", as_index=False).tail(1).copy()
        if latest.empty:
            continue
        latest[f"pUp_{horizon}"] = _predict_probability(classifier, calibrator, latest[feature_cols])
        latest[f"q50_{horizon}"] = np.asarray(mean_regressor.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
        latest[f"q10_{horizon}"] = np.asarray(lower_regressor.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
        latest[f"q90_{horizon}"] = np.asarray(upper_regressor.predict(latest[feature_cols].fillna(0.0)), dtype="float64")

        artifact = _artifact_path(paths, "crypto", f"hourly-{horizon.lower()}.joblib")
        joblib.dump(
            {
                "classifier": classifier,
                "calibrator": calibrator,
                "regressor": mean_regressor,
                "lower": lower_regressor,
                "upper": upper_regressor,
                "feature_cols": feature_cols,
            },
            artifact,
        )

        run_rows.append(
            {
                "market": "crypto",
                "pipeline": "crypto-hourly",
                "horizon": horizon,
                "modelVersion": CRYPTO_MODEL_VERSION,
                "status": "completed",
                "trainedAt": _now_utc().isoformat(),
                "trainStart": clean["timestamp"].min().isoformat(),
                "trainEnd": clean["timestamp"].max().isoformat(),
                "artifactPath": str(artifact),
                "fallbackUsed": False,
                "metricSummary": json.dumps(
                    {
                        "rows": int(len(clean)),
                        "validationRows": int(len(validation)),
                        "auc": auc,
                        "q10Pinball": _safe_pinball(
                            validation[f"target_{horizon}"].to_numpy(dtype="float64"),
                            np.asarray(lower_regressor.predict(validation[feature_cols].fillna(0.0)), dtype="float64"),
                            0.10,
                        )
                        if not validation.empty
                        else None,
                        "q90Pinball": _safe_pinball(
                            validation[f"target_{horizon}"].to_numpy(dtype="float64"),
                            np.asarray(upper_regressor.predict(validation[feature_cols].fillna(0.0)), dtype="float64"),
                            0.90,
                        )
                        if not validation.empty
                        else None,
                    }
                ),
                "message": f"Crypto hourly {horizon} model completed",
            }
        )
        latest_rows.append(latest[["symbol", f"pUp_{horizon}", f"q10_{horizon}", f"q50_{horizon}", f"q90_{horizon}"]])

    if not latest_rows:
        raise ValueError("No crypto hourly models completed")
    latest_frame = latest_rows[0]
    for addition in latest_rows[1:]:
        latest_frame = latest_frame.merge(addition, on="symbol", how="outer")
    return latest_frame, run_rows


def _index_quantile_direction(q10: float, q50: float, q90: float) -> str:
    if q10 < q50 < q90 and q50 > 0 and q90 > 0:
        return "bullish"
    if q10 < q50 < q90 and q10 < 0 and q50 < 0:
        return "bearish"
    return "mixed"


def _cohere_index_forecast(probability: float, expected: float, residual_scale: float) -> dict[str, float | str | bool | None]:
    scale = max(abs(residual_scale), 0.0025)
    q10 = float(expected - scale)
    q50 = float(expected)
    q90 = float(expected + scale)
    quantile_direction = _index_quantile_direction(q10, q50, q90)
    validity = "valid"
    adjusted = False
    reason: str | None = None
    coherent_probability = float(probability)

    if coherent_probability >= 0.55:
        probability_direction = "bullish"
    elif coherent_probability <= 0.45:
        probability_direction = "bearish"
    else:
        probability_direction = "neutral"

    if probability_direction == "bullish" and quantile_direction == "bearish":
        coherent_probability = min(_clamp_probability(1.0 - coherent_probability, 0.01, 0.45), 0.45)
        validity = "adjusted"
        adjusted = True
        reason = "direction_quantile_mismatch_auto_flipped_bearish"
    elif probability_direction == "bearish" and quantile_direction == "bullish":
        coherent_probability = max(_clamp_probability(1.0 - coherent_probability, 0.55, 0.99), 0.55)
        validity = "adjusted"
        adjusted = True
        reason = "direction_quantile_mismatch_auto_flipped_bullish"
    elif probability_direction == "neutral" and quantile_direction == "bullish":
        coherent_probability = max(coherent_probability, 0.55)
        validity = "adjusted"
        adjusted = True
        reason = "neutral_probability_aligned_bullish"
    elif probability_direction == "neutral" and quantile_direction == "bearish":
        coherent_probability = min(coherent_probability, 0.45)
        validity = "adjusted"
        adjusted = True
        reason = "neutral_probability_aligned_bearish"

    if coherent_probability >= 0.55:
        final_probability_direction = "bullish"
    elif coherent_probability <= 0.45:
        final_probability_direction = "bearish"
    else:
        final_probability_direction = "neutral"

    if final_probability_direction == "bullish" and quantile_direction != "bullish":
        coherent_probability = 0.50
        validity = "conflict"
        reason = "direction_quantile_mismatch"
    elif final_probability_direction == "bearish" and quantile_direction != "bearish":
        coherent_probability = 0.50
        validity = "conflict"
        reason = "direction_quantile_mismatch"
    elif quantile_direction == "mixed":
        coherent_probability = 0.50
        validity = "conflict"
        reason = reason or "quantile_geometry_mixed"

    regime = "risk-on" if q50 > 0.005 else ("risk-off" if q50 < -0.005 else "neutral")
    return {
        "pUp": float(_clamp_probability(coherent_probability, 0.01, 0.99)),
        "q10": q10,
        "q50": q50,
        "q90": q90,
        "regime": regime,
        "forecastValidity": validity,
        "forecastConflictReason": reason,
        "forecastAdjusted": adjusted,
    }


def _fit_index_latest(
    panel: pd.DataFrame,
    paths: AppPaths,
    signal_frequency: str,
    horizon_periods: dict[str, int],
    feature_cols: list[str] | None = None,
    model_kwargs: dict[str, float | int] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    if panel.empty:
        raise ValueError("No index panel")
    feature_cols = feature_cols or INDEX_FEATURES
    forecast_rows: list[dict] = []
    run_rows: list[dict] = []
    horizons = list(horizon_periods)

    for symbol, symbol_frame in panel.groupby("symbol"):
        latest_row = symbol_frame.sort_values("timestamp").tail(1)
        if latest_row.empty:
            continue
        latest_payload = latest_row.iloc[0]
        summary_by_horizon: dict[str, dict[str, float | str]] = {}
        for horizon in horizons:
            clean = symbol_frame.dropna(subset=[*feature_cols, f"target_{horizon}", f"class_{horizon}"]).copy()
            if clean.empty:
                continue
            split_index = max(int(len(clean) * 0.8), 1)
            train = clean.iloc[:split_index].copy()
            validation = clean.iloc[split_index:].copy()
            if train.empty:
                train = clean.copy()
                validation = clean.iloc[0:0].copy()
            classifier, calibrator, auc = _fit_calibrated_classifier(train, validation, feature_cols, f"class_{horizon}", model_kwargs)
            regressor = _make_regressor(**(model_kwargs or {}))
            regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
            latest_features = latest_row[feature_cols].fillna(0.0)
            probability = float(_predict_probability(classifier, calibrator, latest_features)[0])
            expected = float(np.asarray(regressor.predict(latest_features), dtype="float64")[0])
            residual_scale = float(train[f"target_{horizon}"].std() or 0.02) * math.sqrt(max(horizon_periods[horizon], 1))
            coherent_summary = _cohere_index_forecast(probability, expected, residual_scale)
            summary_by_horizon[horizon] = {
                **coherent_summary,
                "auc": auc if auc is not None else 0.0,
            }

            artifact = _artifact_path(paths, "index", f"{signal_frequency}-{symbol.replace('^', 'IDX_')}-{horizon.lower()}.joblib")
            joblib.dump(
                {
                    "classifier": classifier,
                    "calibrator": calibrator,
                    "regressor": regressor,
                    "feature_cols": feature_cols,
                },
                artifact,
            )
            run_rows.append(
                {
                    "market": "index",
                    "pipeline": "index-regime",
                    "horizon": f"{symbol}:{horizon}",
                    "modelVersion": INDEX_MODEL_VERSION,
                    "status": "completed",
                    "trainedAt": _now_utc().isoformat(),
                    "trainStart": clean["timestamp"].min().isoformat(),
                    "trainEnd": clean["timestamp"].max().isoformat(),
                    "artifactPath": str(artifact),
                    "fallbackUsed": False,
                    "metricSummary": json.dumps({"rows": int(len(clean)), "validationRows": int(len(validation)), "auc": auc, "signalFrequency": signal_frequency}),
                    "message": f"Index regime model completed for {symbol} {horizon} ({signal_frequency})",
                }
            )

        if summary_by_horizon:
            forecast_rows.append(
                {
                    "symbol": symbol,
                    "timestamp": latest_payload["timestamp"],
                    "payload": summary_by_horizon,
                    "score": float(latest_payload.get("score", 0.0)),
                    **_frequency_provenance(signal_frequency),
                }
            )

    if not forecast_rows:
        raise ValueError("No index forecasts completed")
    return pd.DataFrame(forecast_rows), run_rows


def _build_equity_rankings_and_forecasts(
    latest: pd.DataFrame,
    universe_membership: pd.DataFrame,
    market: str,
    importance_map: dict[str, float],
    signal_frequency: str,
    horizon_periods: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if latest.empty:
        return pd.DataFrame(), pd.DataFrame()

    friendly_names = {
        "z_mom20": "momentum_20d",
        "z_mom60": "momentum_60d",
        "z_low_vol": "low_vol",
        "z_liquidity": "liquidity",
        "z_trend50": "trend",
        "ret_1d": "recent_return",
        "macd_hist": "macd_hist",
        "rsi_bias": "rsi_bias",
        "atr_pct": "atr_pct",
        "bb_width": "bollinger_width",
        "bb_position_centered": "bollinger_position",
        "kdj_spread": "kdj_spread",
        "macd_state_code": "macd_state",
        "rsi_state_code": "rsi_state",
        "bb_state_code": "bollinger_state",
        "kdj_state_code": "kdj_state",
    }
    universe_map = _market_universe_map(universe_membership)
    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []
    horizons = list(horizon_periods)

    latest_by_symbol = latest.set_index("symbol")
    latest_ts = pd.Timestamp(latest["timestamp"].max())

    for universe, (row_market, symbols) in universe_map.items():
        if row_market != market:
            continue
        frame = latest_by_symbol.reindex(symbols).dropna(subset=["score"]).reset_index()
        if frame.empty:
            continue
        frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
        frame["rank"] = range(1, len(frame) + 1)
        volume_median = float(frame["volume"].median() or 0.0)

        for _, row in frame.iterrows():
            breakdown = _breakdown_from_row(row, importance_map, friendly_names)
            long_weights = _normalize_weights(frame["score"], "long_only")
            hedged_weights = _normalize_weights(frame["score"], "hedged")
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": signal_frequency,
                    "strategyMode": "long_only",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["predictedReturn"]),
                    "targetWeight": float(long_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_lgbm_ranker",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": STOCK_MODEL_VERSION,
                    **_frequency_provenance(signal_frequency),
                }
            )
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": signal_frequency,
                    "strategyMode": "hedged",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["predictedReturn"]),
                    "targetWeight": float(hedged_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_lgbm_ranker",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": STOCK_MODEL_VERSION,
                    **_frequency_provenance(signal_frequency),
                }
            )

            for horizon in horizons:
                expected = float(row[f"pred_{horizon}"])
                recent_return = row.get("ret_1d", 0.02)
                if recent_return is None or not math.isfinite(float(recent_return)):
                    recent_return = 0.02
                scale = float(abs(expected) + abs(float(recent_return)) + 0.02)
                forecast_rows.append(
                    {
                        "symbol": row["symbol"],
                        "market": market,
                        "universe": universe,
                        "horizon": horizon,
                        "pUp": _sigmoid(expected / max(scale, 1e-6)),
                        "expectedReturn": expected,
                        "q10": expected - scale,
                        "q50": expected,
                        "q90": expected + scale,
                        "alphaScore": float(row["score"]),
                        "confidence": min(0.95, 0.55 + abs(float(row["score"])) / 5.0),
                        "regime": "risk-on" if float(row[f"pred_{horizons[-1]}"]) > 0 else "risk-off",
                        "riskFlags": ["ml-overlay", "cross-sectional"],
                        "modelVersion": STOCK_MODEL_VERSION,
                        "asOfDate": latest_ts.date().isoformat(),
                        **_frequency_provenance(signal_frequency),
                        **indicator_payload_from_row(row),
                    }
                )

    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _build_crypto_daily_rankings_and_forecasts(
    daily_latest: pd.DataFrame,
    hourly_latest: pd.DataFrame,
    universe_membership: pd.DataFrame,
    importance_map: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if daily_latest.empty:
        return pd.DataFrame(), pd.DataFrame()

    friendly_names = {
        "z_mom20": "momentum_20d",
        "z_mom5": "momentum_5d",
        "z_trend": "trend",
        "z_vol20": "volatility",
        "ret_1d": "recent_return",
        "macd_hist": "macd_hist",
        "rsi_bias": "rsi_bias",
        "atr_pct": "atr_pct",
        "bb_width": "bollinger_width",
        "bb_position_centered": "bollinger_position",
        "kdj_spread": "kdj_spread",
        "macd_state_code": "macd_state",
        "rsi_state_code": "rsi_state",
        "bb_state_code": "bollinger_state",
        "kdj_state_code": "kdj_state",
    }
    latest = daily_latest.merge(hourly_latest, on="symbol", how="left")
    universe_map = _market_universe_map(universe_membership)
    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []
    latest_ts = pd.Timestamp(daily_latest["timestamp"].max())

    for universe, (market, symbols) in universe_map.items():
        if market != "crypto":
            continue
        frame = latest.set_index("symbol").reindex(symbols).dropna(subset=["score"]).reset_index()
        if frame.empty:
            continue
        frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
        frame["rank"] = range(1, len(frame) + 1)
        volume_median = float(frame["volume"].median() or 0.0)
        long_weights = _normalize_weights(frame["score"], "long_only")
        hedged_weights = _normalize_weights(frame["score"], "hedged")

        for _, row in frame.iterrows():
            breakdown = _breakdown_from_row(row, importance_map, friendly_names)
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": "daily",
                    "strategyMode": "long_only",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["pred_1D"]),
                    "targetWeight": float(long_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_crypto_ts",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": CRYPTO_MODEL_VERSION,
                    **_frequency_provenance("daily"),
                }
            )
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": "daily",
                    "strategyMode": "hedged",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["pred_1D"]),
                    "targetWeight": float(hedged_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_crypto_ts",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": CRYPTO_MODEL_VERSION,
                    **_frequency_provenance("daily"),
                }
            )

            for horizon in ["1H", "4H", "1D"]:
                q50 = float(row.get(f"q50_{horizon}", row["pred_1D"]))
                q10 = float(row.get(f"q10_{horizon}", q50 - abs(q50)))
                q90 = float(row.get(f"q90_{horizon}", q50 + abs(q50)))
                p_up = float(row.get(f"pUp_{horizon}", _sigmoid(float(row["score"]))))
                forecast_rows.append(
                    {
                        "symbol": row["symbol"],
                        "market": "crypto",
                        "universe": universe,
                        "horizon": horizon,
                        "pUp": p_up,
                        "expectedReturn": q50,
                        "q10": q10,
                        "q50": q50,
                        "q90": q90,
                        "alphaScore": float(row["score"]),
                        "confidence": min(0.97, 0.55 + abs(float(row["score"])) / 4.5),
                        "regime": "risk-on" if float(row["score"]) > 0 else "risk-off",
                        "riskFlags": ["ml-overlay", "time-series", "quantile"],
                        "modelVersion": CRYPTO_MODEL_VERSION,
                        "asOfDate": latest_ts.date().isoformat(),
                        **_frequency_provenance("daily"),
                        **indicator_payload_from_row(row),
                    }
                )

    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _build_crypto_weekly_rankings_and_forecasts(
    weekly_latest: pd.DataFrame,
    universe_membership: pd.DataFrame,
    importance_map: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if weekly_latest.empty:
        return pd.DataFrame(), pd.DataFrame()

    friendly_names = {
        "z_mom20": "momentum_20w",
        "z_mom5": "momentum_5w",
        "z_trend": "trend",
        "z_vol20": "volatility",
        "ret_1d": "recent_return",
        "macd_hist": "macd_hist",
        "rsi_bias": "rsi_bias",
        "atr_pct": "atr_pct",
        "bb_width": "bollinger_width",
        "bb_position_centered": "bollinger_position",
        "kdj_spread": "kdj_spread",
        "macd_state_code": "macd_state",
        "rsi_state_code": "rsi_state",
        "bb_state_code": "bollinger_state",
        "kdj_state_code": "kdj_state",
    }
    universe_map = _market_universe_map(universe_membership)
    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []
    latest_ts = pd.Timestamp(weekly_latest["timestamp"].max())

    for universe, (market, symbols) in universe_map.items():
        if market != "crypto":
            continue
        frame = weekly_latest.set_index("symbol").reindex(symbols).dropna(subset=["score"]).reset_index()
        if frame.empty:
            continue
        frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
        frame["rank"] = range(1, len(frame) + 1)
        volume_median = float(frame["volume"].median() or 0.0)
        long_weights = _normalize_weights(frame["score"], "long_only")
        hedged_weights = _normalize_weights(frame["score"], "hedged")

        for _, row in frame.iterrows():
            breakdown = _breakdown_from_row(row, importance_map, friendly_names)
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": "weekly",
                    "strategyMode": "long_only",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["pred_1W"]),
                    "targetWeight": float(long_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_crypto_ts",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": CRYPTO_MODEL_VERSION,
                    **_frequency_provenance("weekly"),
                }
            )
            ranking_rows.append(
                {
                    "symbol": row["symbol"],
                    "universe": universe,
                    "rebalanceFreq": "weekly",
                    "strategyMode": "hedged",
                    "score": float(row["score"]),
                    "rank": int(row["rank"]),
                    "expectedReturn": float(row["pred_1W"]),
                    "targetWeight": float(hedged_weights.iloc[row["rank"] - 1]),
                    "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                    "factorExposures": breakdown,
                    "signalFamily": "ga_crypto_ts",
                    "signalBreakdown": breakdown,
                    "asOfDate": latest_ts.date().isoformat(),
                    "modelVersion": CRYPTO_MODEL_VERSION,
                    **_frequency_provenance("weekly"),
                }
            )
            q50 = float(row["pred_1W"])
            scale = float(abs(q50) + abs(row.get("ret_1d", 0.02) or 0.02) + 0.02)
            forecast_rows.append(
                {
                    "symbol": row["symbol"],
                    "market": "crypto",
                    "universe": universe,
                    "horizon": "1W",
                    "pUp": _sigmoid(float(row["score"])),
                    "expectedReturn": q50,
                    "q10": q50 - scale,
                    "q50": q50,
                    "q90": q50 + scale,
                    "alphaScore": float(row["score"]),
                    "confidence": min(0.97, 0.55 + abs(float(row["score"])) / 4.5),
                    "regime": "risk-on" if float(row["score"]) > 0 else "risk-off",
                    "riskFlags": ["ml-overlay", "weekly-time-series"],
                    "modelVersion": CRYPTO_MODEL_VERSION,
                    "asOfDate": latest_ts.date().isoformat(),
                    **_frequency_provenance("weekly"),
                    **indicator_payload_from_row(row),
                }
            )

    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _build_index_forecasts(index_latest: pd.DataFrame, universe_membership: pd.DataFrame, signal_frequency: str, horizon_periods: dict[str, int]) -> pd.DataFrame:
    if index_latest.empty:
        return pd.DataFrame()
    universe_map = _market_universe_map(universe_membership)
    latest_by_symbol = index_latest.set_index("symbol")
    forecast_rows: list[dict] = []
    horizons = list(horizon_periods)
    for universe, (market, symbols) in universe_map.items():
        if market != "index":
            continue
        for symbol in symbols:
            if symbol not in latest_by_symbol.index:
                continue
            row = latest_by_symbol.loc[symbol]
            payload = row["payload"]
            for horizon in horizons:
                horizon_payload = payload.get(horizon)
                if not horizon_payload:
                    continue
                forecast_rows.append(
                    {
                        "symbol": symbol,
                        "market": "index",
                        "universe": universe,
                        "horizon": horizon,
                        "pUp": float(horizon_payload["pUp"]),
                        "expectedReturn": float(horizon_payload["q50"]),
                        "q10": float(horizon_payload["q10"]),
                        "q50": float(horizon_payload["q50"]),
                        "q90": float(horizon_payload["q90"]),
                        "alphaScore": 0.0,
                        "confidence": min(0.95, 0.52 + abs(float(horizon_payload["q50"])) * 8.0),
                        "regime": str(horizon_payload["regime"]),
                        "riskFlags": [
                            "ml-overlay",
                            "regime-model",
                            *(
                                [str(horizon_payload["forecastConflictReason"])]
                                if horizon_payload.get("forecastConflictReason")
                                else []
                            ),
                        ],
                        "modelVersion": INDEX_MODEL_VERSION,
                        "asOfDate": pd.Timestamp(row["timestamp"]).date().isoformat(),
                        "forecastValidity": str(horizon_payload.get("forecastValidity") or "valid"),
                        "forecastConflictReason": horizon_payload.get("forecastConflictReason"),
                        "forecastAdjusted": bool(horizon_payload.get("forecastAdjusted", False)),
                        **_frequency_provenance(signal_frequency),
                        **indicator_payload_from_row(row),
                    }
                )
    return pd.DataFrame(forecast_rows)


def _failed_run(market: str, pipeline: str, message: str, model_version: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "market": market,
                "pipeline": pipeline,
                "horizon": "n/a",
                "modelVersion": model_version,
                "status": "failed",
                "trainedAt": _now_utc().isoformat(),
                "trainStart": None,
                "trainEnd": None,
                "artifactPath": None,
                "fallbackUsed": True,
                "metricSummary": json.dumps({}),
                "message": message,
            }
        ]
    )


def _baseline_breakdown(row: pd.Series, weights: dict[str, float]) -> dict[str, float]:
    if not weights:
        return {}
    contributions = {name: float(row.get(name, 0.0) or 0.0) * float(weight) for name, weight in weights.items()}
    normalizer = sum(abs(value) for value in contributions.values()) or 1.0
    return {key: float(value / normalizer) for key, value in contributions.items()}


def _capped_weights(raw_scores: pd.Series, top_n: int, max_position: float) -> pd.Series:
    if raw_scores.empty:
        return pd.Series(dtype="float64")
    selected = raw_scores.head(max(1, min(top_n, len(raw_scores)))).clip(lower=0.0)
    if float(selected.sum() or 0.0) <= 0.0:
        selected = pd.Series(1.0, index=selected.index, dtype="float64")
    weights = selected / float(selected.sum() or 1.0)
    if max_position > 0:
        weights = weights.clip(upper=max_position)
        weights = weights / float(weights.sum() or 1.0)
    out = pd.Series(0.0, index=raw_scores.index, dtype="float64")
    out.loc[selected.index] = weights
    return out


def _forecast_horizons_for_market(market: str, signal_frequency: str) -> tuple[list[str], dict[str, float]]:
    if market == "crypto":
        if signal_frequency == "daily":
            return ["1H", "4H", "1D"], {"1H": 0.25, "4H": 0.55, "1D": 1.0}
        return ["1W"], {"1W": 1.0}
    if signal_frequency == "daily":
        return ["1D", "5D", "20D"], {"1D": 1.0, "5D": math.sqrt(5), "20D": math.sqrt(20)}
    return ["1W", "5W", "20W"], {"1W": 1.0, "5W": math.sqrt(5), "20W": math.sqrt(20)}


def _optimize_baseline_specs(signal_panel: pd.DataFrame, ga_runs: pd.DataFrame | None = None, *, reuse_cached: bool = True) -> tuple[dict[tuple[str, str], dict[str, object]], pd.DataFrame]:
    specs: dict[tuple[str, str], dict[str, object]] = {}
    ga_rows: list[dict] = []
    for market in ["cn_equity", "us_equity", "crypto", "index"]:
        for signal_frequency in SIGNAL_FREQUENCIES:
            if market in {"cn_equity", "us_equity"}:
                frame = _prepare_stock_panel(signal_panel, market, signal_frequency)
            elif market == "crypto":
                frame = _prepare_crypto_panel(signal_panel, signal_frequency)
            else:
                frame = _prepare_index_panel(signal_panel, signal_frequency)
            if frame.empty:
                continue
            spec = _optimize_baseline_spec(frame, market, signal_frequency, ga_runs=ga_runs, reuse_cached=reuse_cached)
            specs[(market, signal_frequency)] = spec
            ga_rows.append(
                {
                    "market": market,
                    "pipeline": "baseline-ga",
                    "signalFrequency": signal_frequency,
                    "modelVersion": BASELINE_MODEL_VERSION,
                    "fitness": float(spec.get("fitness", 0.0)),
                    "selectedFactors": json.dumps(spec.get("selected", [])),
                    "config": json.dumps({"top_n": spec.get("top_n"), "max_position": spec.get("max_position"), "weights": spec.get("weights", {}), "selected": spec.get("selected", [])}),
                    "metricSummary": spec.get("summary", json.dumps({})),
                    "trainedAt": _now_utc().isoformat(),
                    "latestTimestamp": spec.get("latestTimestamp"),
                    "cacheHit": bool(spec.get("cacheHit", False)),
                }
            )
    return specs, pd.DataFrame(ga_rows)


def _apply_baseline_specs(signal_panel: pd.DataFrame, specs: dict[tuple[str, str], dict[str, object]]) -> pd.DataFrame:
    if signal_panel.empty:
        return signal_panel
    updated = signal_panel.copy()
    for (market, signal_frequency), spec in specs.items():
        mask = (updated["market"] == market) & (updated["signalFrequency"] == signal_frequency)
        if not mask.any():
            continue
        weights = dict(spec.get("weights", {}))
        if not weights:
            continue
        updated.loc[mask, "score"] = _apply_weighted_score(updated.loc[mask], weights)
    return updated


def _build_ga_baseline_outputs(
    signal_panel: pd.DataFrame,
    universe_membership: pd.DataFrame,
    specs: dict[tuple[str, str], dict[str, object]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    universe_map = _market_universe_map(universe_membership)
    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []

    for universe, (market, symbols) in universe_map.items():
        for signal_frequency in SIGNAL_FREQUENCIES:
            spec = specs.get((market, signal_frequency))
            if spec is None:
                continue
            scoped = signal_panel[
                (signal_panel["market"] == market)
                & (signal_panel["signalFrequency"] == signal_frequency)
                & (signal_panel["symbol"].isin(symbols))
            ].copy()
            if scoped.empty:
                continue
            latest_ts = scoped["timestamp"].max()
            latest = scoped[scoped["timestamp"] == latest_ts].copy().sort_values("score", ascending=False).reset_index(drop=True)
            if latest.empty:
                continue
            latest["rank"] = range(1, len(latest) + 1)
            weights = dict(spec.get("weights", {}))
            top_n = int(spec.get("top_n", 10))
            max_position = float(spec.get("max_position", 0.10))
            long_weights = _capped_weights(latest["score"], top_n, max_position)
            short_weights = _capped_weights((-latest["score"]).sort_values(ascending=False), top_n, max_position)
            short_weights = short_weights.reindex(latest.index, fill_value=0.0)
            horizons, horizon_scale = _forecast_horizons_for_market(market, signal_frequency)
            recent_return = pd.to_numeric(latest.get("ret_1d", 0.0), errors="coerce").fillna(0.0).abs()
            realized_vol = pd.to_numeric(latest.get("realized_vol20", latest.get("vol20", 0.02)), errors="coerce").fillna(0.02)
            base_return = latest["score"] * np.maximum(recent_return, 0.01)

            for idx, row in latest.iterrows():
                breakdown = _baseline_breakdown(row, weights)
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": signal_frequency,
                        "strategyMode": "long_only",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(base_return.iloc[idx]),
                        "targetWeight": float(long_weights.iloc[idx]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), float(latest["volume"].median() or 0.0)),
                        "factorExposures": breakdown,
                        "signalFamily": "ga_baseline_multifactor",
                        "signalBreakdown": breakdown,
                        "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                        "modelVersion": BASELINE_MODEL_VERSION,
                        **_frequency_provenance(signal_frequency),
                    }
                )
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": signal_frequency,
                        "strategyMode": "hedged",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(base_return.iloc[idx]),
                        "targetWeight": float(long_weights.iloc[idx] - short_weights.iloc[idx]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), float(latest["volume"].median() or 0.0)),
                        "factorExposures": breakdown,
                        "signalFamily": "ga_baseline_multifactor",
                        "signalBreakdown": breakdown,
                        "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                        "modelVersion": BASELINE_MODEL_VERSION,
                        **_frequency_provenance(signal_frequency),
                    }
                )
                for horizon in horizons:
                    scale = float(horizon_scale[horizon])
                    expected = float(base_return.iloc[idx] * scale)
                    sigma = float(abs(realized_vol.iloc[idx]) + abs(recent_return.iloc[idx]) + 0.01)
                    forecast_rows.append(
                        {
                            "symbol": row["symbol"],
                            "market": market,
                            "universe": universe,
                            "horizon": horizon,
                            "pUp": _sigmoid(float(row["score"])),
                            "expectedReturn": expected,
                            "q10": expected - sigma * scale,
                            "q50": expected,
                            "q90": expected + sigma * scale,
                            "alphaScore": float(row["score"]),
                            "confidence": min(0.95, 0.55 + abs(float(row["score"])) / 4.0),
                            "regime": "risk-on" if float(row["score"]) >= 0 else "risk-off",
                            "riskFlags": ["ga-baseline", "multifactor"],
                            "modelVersion": BASELINE_MODEL_VERSION,
                            "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                            **_frequency_provenance(signal_frequency),
                            **indicator_payload_from_row(row),
                        }
                    )
    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _attach_ga_summary(metric_rows: list[dict], ga_summary: str) -> list[dict]:
    enriched: list[dict] = []
    parsed_ga = json.loads(ga_summary or "{}")
    for row in metric_rows:
        current = json.loads(row.get("metricSummary") or "{}")
        enriched.append({**row, "metricSummary": json.dumps({**current, "ga": parsed_ga})})
    return enriched


def build_ml_overlay(paths: AppPaths, *, reuse_cached: bool = True) -> None:
    signal_panel = read_frame(paths, "signal_panel")
    bars_1h = read_frame(paths, "bars_1h")
    memberships = read_frame(paths, "universe_membership")
    baseline_rankings = read_frame(paths, "baseline_ranking_panel")
    baseline_forecasts = read_frame(paths, "baseline_forecast_panel")

    if signal_panel.empty:
        raise ValueError("Signal panel is empty; build baseline signals before ML overlay")

    final_rankings = baseline_rankings.copy()
    final_forecasts = baseline_forecasts.copy()
    history_frames: list[pd.DataFrame] = []
    run_frames: list[pd.DataFrame] = []
    ga_frames: list[pd.DataFrame] = []
    existing_ga_runs = read_frame(paths, "ga_run_panel")
    if not existing_ga_runs.empty:
        ga_frames.append(existing_ga_runs)

    for market in ["cn_equity", "us_equity"]:
        for signal_frequency in SIGNAL_FREQUENCIES:
            try:
                horizon_periods = _horizon_periods_for_frequency(signal_frequency, market)
                stock_panel = _prepare_stock_panel(signal_panel, market, signal_frequency)
                if stock_panel.empty:
                    continue
                selected_features, model_kwargs, ga_summary, ga_metrics, ga_latest_timestamp = _optimize_ml_feature_subset(
                    stock_panel,
                    market,
                    signal_frequency,
                    evaluator_builder=lambda optimization_frame, selected, params, horizon_periods=horizon_periods, market=market, signal_frequency=signal_frequency: score_candidate_history(
                        _walk_forward_ranker(
                            optimization_frame,
                            selected,
                            horizons=list(horizon_periods),
                            horizon_periods=horizon_periods,
                            market=market,
                            model_version=STOCK_MODEL_VERSION,
                            signal_frequency=signal_frequency,
                            model_kwargs=params,
                        ).merge(
                            optimization_frame[["timestamp", "symbol", "target_alpha"]],
                            on=["timestamp", "symbol"],
                            how="left",
                        ),
                        market=signal_frequency,
                        target_col="target_alpha",
                        top_n=30 if market == "cn_equity" else 25,
                        feature_count=len(selected),
                        total_features=max(len(_feature_candidates_for_market(market, signal_frequency)), 1),
                    ),
                    ga_runs=existing_ga_runs,
                    pipeline_name="ml-ga-ranker",
                    model_version=STOCK_MODEL_VERSION,
                    reuse_cached=reuse_cached,
                )
                history = _walk_forward_ranker(
                    stock_panel,
                    selected_features,
                    horizons=list(horizon_periods),
                    horizon_periods=horizon_periods,
                    market=market,
                    model_version=STOCK_MODEL_VERSION,
                    signal_frequency=signal_frequency,
                    model_kwargs=model_kwargs,
                )
                latest, importance_map, metrics = _fit_stock_latest_models(stock_panel, selected_features, market, paths, signal_frequency, horizon_periods, model_kwargs)
                rankings, forecasts = _build_equity_rankings_and_forecasts(latest, memberships, market, importance_map, signal_frequency, horizon_periods)
                final_rankings = replace_rows_by_keys(final_rankings, rankings, ["symbol", "universe", "rebalanceFreq", "strategyMode", "signalFrequency"])
                final_forecasts = replace_rows_by_keys(final_forecasts, forecasts, ["symbol", "universe", "horizon", "signalFrequency"])
                history_frames.append(history)
                run_frames.append(pd.DataFrame(_attach_ga_summary(metrics, ga_summary)))
                ga_frames.append(
                    pd.DataFrame(
                        [
                            {
                                "market": market,
                                "pipeline": "ml-ga-ranker",
                                "signalFrequency": signal_frequency,
                                "modelVersion": STOCK_MODEL_VERSION,
                                "fitness": float(ga_metrics.get("fitness", 0.0)),
                                "selectedFactors": json.dumps(selected_features),
                                "config": json.dumps(model_kwargs),
                                "metricSummary": ga_summary,
                                "trainedAt": _now_utc().isoformat(),
                                "latestTimestamp": ga_latest_timestamp,
                                "cacheHit": False,
                            }
                        ]
                    )
                )
            except Exception as exc:
                run_frames.append(_failed_run(market, f"equity-ranker-{signal_frequency}", f"{market} {signal_frequency} overlay failed: {exc}", STOCK_MODEL_VERSION))

    try:
        crypto_daily_panel = _prepare_crypto_panel(signal_panel, "daily")
        selected_features, model_kwargs, ga_summary, ga_metrics, ga_latest_timestamp = _optimize_ml_feature_subset(
            crypto_daily_panel,
            "crypto",
            "daily",
            evaluator_builder=lambda optimization_frame, selected, params: score_candidate_history(
                _walk_forward_regression(
                    optimization_frame,
                    selected,
                    "target_1D",
                    preferred_step=10,
                    market="crypto",
                    model_version=CRYPTO_MODEL_VERSION,
                    signal_frequency="daily",
                    model_kwargs=params,
                ).merge(
                    optimization_frame[["timestamp", "symbol", "target_1D"]],
                    on=["timestamp", "symbol"],
                    how="left",
                ),
                market="daily",
                target_col="target_1D",
                top_n=10,
                feature_count=len(selected),
                total_features=max(len(_feature_candidates_for_market("crypto", "daily")), 1),
            ),
            ga_runs=existing_ga_runs,
            pipeline_name="ml-ga-regressor",
            model_version=CRYPTO_MODEL_VERSION,
            reuse_cached=reuse_cached,
        )
        crypto_history = _walk_forward_regression(
            crypto_daily_panel,
            selected_features,
            "target_1D",
            preferred_step=10,
            market="crypto",
            model_version=CRYPTO_MODEL_VERSION,
            signal_frequency="daily",
            model_kwargs=model_kwargs,
        )
        crypto_daily_latest, crypto_importance, crypto_daily_runs = _fit_crypto_latest(crypto_daily_panel, selected_features, paths, "daily", "1D", model_kwargs)
        crypto_hourly_panel = _build_crypto_hourly_panel(bars_1h)
        write_frame(paths, "crypto_feature_panel", crypto_hourly_panel)
        hourly_features = [column for column in CRYPTO_HOURLY_FEATURES if column in crypto_hourly_panel.columns]
        selected_hourly_features, hourly_model_kwargs, hourly_ga_summary, hourly_ga_metrics, hourly_ga_latest_timestamp = _optimize_ml_feature_subset(
            crypto_hourly_panel,
            "crypto",
            "hourly",
            evaluator_builder=lambda optimization_frame, selected, params: score_candidate_history(
                optimization_frame.dropna(subset=[*selected, "target_1D"]).assign(
                    score=lambda item: pd.Series(
                        _make_regressor(**params)
                        .fit(item[selected].fillna(0.0), item["target_1D"].fillna(0.0))
                        .predict(item[selected].fillna(0.0)),
                        index=item.index,
                    )
                )[["timestamp", "symbol", "score", "target_1D"]],
                market="daily",
                target_col="target_1D",
                top_n=10,
                feature_count=len(selected),
                total_features=max(len(hourly_features), 1),
            ),
            ga_runs=existing_ga_runs,
            pipeline_name="ml-ga-hourly",
            model_version=CRYPTO_MODEL_VERSION,
            reuse_cached=reuse_cached,
        )
        crypto_hourly_latest, crypto_hourly_runs = _fit_crypto_hourly_latest(crypto_hourly_panel, paths, selected_hourly_features, hourly_model_kwargs)
        crypto_rankings, crypto_forecasts = _build_crypto_daily_rankings_and_forecasts(crypto_daily_latest, crypto_hourly_latest, memberships, crypto_importance)
        final_rankings = replace_rows_by_keys(final_rankings, crypto_rankings, ["symbol", "universe", "rebalanceFreq", "strategyMode", "signalFrequency"])
        final_forecasts = replace_rows_by_keys(final_forecasts, crypto_forecasts, ["symbol", "universe", "horizon", "signalFrequency"])
        history_frames.append(crypto_history)
        run_frames.append(pd.DataFrame(_attach_ga_summary(crypto_daily_runs, ga_summary)))
        run_frames.append(pd.DataFrame(_attach_ga_summary(crypto_hourly_runs, hourly_ga_summary)))
        ga_frames.append(pd.DataFrame([{"market": "crypto", "pipeline": "ml-ga-regressor", "signalFrequency": "daily", "modelVersion": CRYPTO_MODEL_VERSION, "fitness": float(ga_metrics.get("fitness", 0.0)), "selectedFactors": json.dumps(selected_features), "config": json.dumps(model_kwargs), "metricSummary": ga_summary, "trainedAt": _now_utc().isoformat(), "latestTimestamp": ga_latest_timestamp, "cacheHit": False}]))
        ga_frames.append(pd.DataFrame([{"market": "crypto", "pipeline": "ml-ga-hourly", "signalFrequency": "hourly", "modelVersion": CRYPTO_MODEL_VERSION, "fitness": float(hourly_ga_metrics.get("fitness", 0.0)), "selectedFactors": json.dumps(selected_hourly_features), "config": json.dumps(hourly_model_kwargs), "metricSummary": hourly_ga_summary, "trainedAt": _now_utc().isoformat(), "latestTimestamp": hourly_ga_latest_timestamp, "cacheHit": False}]))
    except Exception as exc:
        run_frames.append(_failed_run("crypto", "crypto-overlay", f"crypto overlay failed: {exc}", CRYPTO_MODEL_VERSION))

    try:
        crypto_weekly_panel = _prepare_crypto_panel(signal_panel, "weekly")
        selected_features, model_kwargs, ga_summary, ga_metrics, ga_latest_timestamp = _optimize_ml_feature_subset(
            crypto_weekly_panel,
            "crypto",
            "weekly",
            evaluator_builder=lambda optimization_frame, selected, params: score_candidate_history(
                _walk_forward_regression(
                    optimization_frame,
                    selected,
                    "target_1W",
                    preferred_step=4,
                    market="crypto",
                    model_version=CRYPTO_MODEL_VERSION,
                    signal_frequency="weekly",
                    model_kwargs=params,
                ).merge(
                    optimization_frame[["timestamp", "symbol", "target_1W"]],
                    on=["timestamp", "symbol"],
                    how="left",
                ),
                market="weekly",
                target_col="target_1W",
                top_n=10,
                feature_count=len(selected),
                total_features=max(len(_feature_candidates_for_market("crypto", "weekly")), 1),
            ),
            ga_runs=existing_ga_runs,
            pipeline_name="ml-ga-regressor",
            model_version=CRYPTO_MODEL_VERSION,
            reuse_cached=reuse_cached,
        )
        crypto_weekly_history = _walk_forward_regression(
            crypto_weekly_panel,
            selected_features,
            "target_1W",
            preferred_step=4,
            market="crypto",
            model_version=CRYPTO_MODEL_VERSION,
            signal_frequency="weekly",
            model_kwargs=model_kwargs,
        )
        crypto_weekly_latest, crypto_weekly_importance, crypto_weekly_runs = _fit_crypto_latest(
            crypto_weekly_panel, selected_features, paths, "weekly", "1W", model_kwargs
        )
        crypto_weekly_rankings, crypto_weekly_forecasts = _build_crypto_weekly_rankings_and_forecasts(
            crypto_weekly_latest, memberships, crypto_weekly_importance
        )
        final_rankings = replace_rows_by_keys(final_rankings, crypto_weekly_rankings, ["symbol", "universe", "rebalanceFreq", "strategyMode", "signalFrequency"])
        final_forecasts = replace_rows_by_keys(final_forecasts, crypto_weekly_forecasts, ["symbol", "universe", "horizon", "signalFrequency"])
        history_frames.append(crypto_weekly_history)
        run_frames.append(pd.DataFrame(_attach_ga_summary(crypto_weekly_runs, ga_summary)))
        ga_frames.append(pd.DataFrame([{"market": "crypto", "pipeline": "ml-ga-regressor", "signalFrequency": "weekly", "modelVersion": CRYPTO_MODEL_VERSION, "fitness": float(ga_metrics.get("fitness", 0.0)), "selectedFactors": json.dumps(selected_features), "config": json.dumps(model_kwargs), "metricSummary": ga_summary, "trainedAt": _now_utc().isoformat(), "latestTimestamp": ga_latest_timestamp, "cacheHit": False}]))
    except Exception as exc:
        run_frames.append(_failed_run("crypto", "crypto-weekly", f"crypto weekly overlay failed: {exc}", CRYPTO_MODEL_VERSION))

    for signal_frequency in SIGNAL_FREQUENCIES:
        try:
            horizon_periods = _horizon_periods_for_frequency(signal_frequency, "index")
            index_panel = _prepare_index_panel(signal_panel, signal_frequency)
            selected_features, model_kwargs, ga_summary, ga_metrics, ga_latest_timestamp = _optimize_ml_feature_subset(
                index_panel,
                "index",
                signal_frequency,
                evaluator_builder=lambda optimization_frame, selected, params, signal_frequency=signal_frequency: score_candidate_history(
                    _walk_forward_regression(
                        optimization_frame,
                        selected,
                        f"target_{list(_horizon_periods_for_frequency(signal_frequency, 'index'))[0]}",
                        preferred_step=4 if signal_frequency == "weekly" else 10,
                        market="index",
                        model_version=INDEX_MODEL_VERSION,
                        signal_frequency=signal_frequency,
                        model_kwargs=params,
                    ).merge(
                        optimization_frame[["timestamp", "symbol", f"target_{list(_horizon_periods_for_frequency(signal_frequency, 'index'))[0]}"]],
                        on=["timestamp", "symbol"],
                        how="left",
                    ),
                    market=signal_frequency,
                    target_col=f"target_{list(_horizon_periods_for_frequency(signal_frequency, 'index'))[0]}",
                    top_n=3,
                    feature_count=len(selected),
                    total_features=max(len(_feature_candidates_for_market("index", signal_frequency)), 1),
                ),
                ga_runs=existing_ga_runs,
                pipeline_name="ml-ga-index",
                model_version=INDEX_MODEL_VERSION,
                reuse_cached=reuse_cached,
            )
            index_latest, index_runs = _fit_index_latest(index_panel, paths, signal_frequency, horizon_periods, selected_features, model_kwargs)
            index_forecasts = _build_index_forecasts(index_latest, memberships, signal_frequency, horizon_periods)
            final_forecasts = replace_rows_by_keys(final_forecasts, index_forecasts, ["symbol", "universe", "horizon", "signalFrequency"])
            run_frames.append(pd.DataFrame(_attach_ga_summary(index_runs, ga_summary)))
            ga_frames.append(pd.DataFrame([{"market": "index", "pipeline": "ml-ga-index", "signalFrequency": signal_frequency, "modelVersion": INDEX_MODEL_VERSION, "fitness": float(ga_metrics.get("fitness", 0.0)), "selectedFactors": json.dumps(selected_features), "config": json.dumps(model_kwargs), "metricSummary": ga_summary, "trainedAt": _now_utc().isoformat(), "latestTimestamp": ga_latest_timestamp, "cacheHit": False}]))
        except Exception as exc:
            run_frames.append(_failed_run("index", f"index-regime-{signal_frequency}", f"index {signal_frequency} overlay failed: {exc}", INDEX_MODEL_VERSION))

    history_panel = pd.concat([frame for frame in history_frames if not frame.empty], ignore_index=True) if history_frames else pd.DataFrame()
    if history_panel.empty:
        history_panel = signal_panel[["market", "symbol", "timestamp", "score", "signalFrequency", "sourceFrequency", "isDerivedSignal"]].copy()
        history_panel["predictedReturn"] = history_panel["score"]
        history_panel["modelVersion"] = BASELINE_MODEL_VERSION

    model_runs = pd.concat([frame for frame in run_frames if not frame.empty], ignore_index=True) if run_frames else pd.DataFrame()

    write_frame(paths, "ranking_panel", _ensure_frame(final_rankings, list(final_rankings.columns)))
    write_frame(paths, "forecast_panel", _ensure_frame(final_forecasts, list(final_forecasts.columns)))
    write_frame(paths, "prediction_history_panel", history_panel)
    write_frame(paths, "model_run_panel", model_runs)
    ga_run_panel = pd.concat([frame for frame in ga_frames if not frame.empty], ignore_index=True) if ga_frames else pd.DataFrame()
    if not ga_run_panel.empty:
        ga_run_panel = ga_run_panel.drop_duplicates(subset=["market", "pipeline", "signalFrequency", "modelVersion"], keep="last")
        write_frame(paths, "ga_run_panel", ga_run_panel)
    sync_duckdb(paths)


def bootstrap_baseline_outputs(paths: AppPaths, *, reuse_cached: bool = True) -> None:
    signal_panel = read_frame(paths, "signal_panel")
    memberships = read_frame(paths, "universe_membership")
    existing_ga_runs = read_frame(paths, "ga_run_panel")
    specs, ga_runs = _optimize_baseline_specs(signal_panel, existing_ga_runs, reuse_cached=reuse_cached)
    scored_signal_panel = _apply_baseline_specs(signal_panel, specs)
    rankings, forecasts = _build_ga_baseline_outputs(scored_signal_panel, memberships, specs)
    write_frame(paths, "signal_panel", scored_signal_panel)
    write_frame(paths, "baseline_ranking_panel", rankings)
    write_frame(paths, "baseline_forecast_panel", forecasts)
    write_frame(paths, "ranking_panel", rankings)
    write_frame(paths, "forecast_panel", forecasts)
    write_frame(paths, "ga_run_panel", ga_runs)
    sync_duckdb(paths)
