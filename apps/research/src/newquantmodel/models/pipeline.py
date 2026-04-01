from __future__ import annotations

import json
import math
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

from newquantmodel.analytics.signals import build_rankings_and_forecasts
from newquantmodel.config.settings import AppPaths
from newquantmodel.storage.parquet_store import (
    read_frame,
    replace_rows_by_keys,
    sync_duckdb,
    write_frame,
)


STOCK_MODEL_VERSION = "equity-lgbm-ranker-v1"
CRYPTO_MODEL_VERSION = "crypto-ts-v1"
INDEX_MODEL_VERSION = "index-regime-v1"
BASELINE_MODEL_VERSION = "baseline-signals-v1"

STOCK_FEATURES = ["z_mom20", "z_mom60", "z_low_vol", "z_liquidity", "z_trend50", "ret_1d"]
CRYPTO_DAILY_FEATURES = ["z_mom20", "z_mom5", "z_trend", "z_vol20", "ret_1d"]
CRYPTO_HOURLY_FEATURES = ["ret_1h", "mom24", "mom96", "vol24", "trend24_72", "volume_z"]
INDEX_FEATURES = ["mom20", "trend60", "vol20", "drawdown20", "ret_1d"]

HORIZON_TO_PERIODS = {"1D": 1, "5D": 5, "20D": 20}
CRYPTO_HORIZON_TO_PERIODS = {"1H": 1, "4H": 4, "1D": 24}
REBALANCE_FREQUENCIES = ["daily", "weekly"]


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


def _build_crypto_hourly_panel(bars_1h: pd.DataFrame) -> pd.DataFrame:
    if bars_1h.empty:
        return pd.DataFrame()
    frame = bars_1h[bars_1h["market"] == "crypto"].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1h"] = grouped["close"].pct_change()
    frame["mom24"] = grouped["close"].pct_change(24)
    frame["mom96"] = grouped["close"].pct_change(96)
    frame["vol24"] = grouped["ret_1h"].rolling(24).std().reset_index(level=0, drop=True)
    frame["ema24"] = grouped["close"].transform(lambda series: series.ewm(span=24, adjust=False).mean())
    frame["ema72"] = grouped["close"].transform(lambda series: series.ewm(span=72, adjust=False).mean())
    frame["trend24_72"] = frame["ema24"] / frame["ema72"] - 1.0
    rolling_volume = grouped["volume"].rolling(24).mean().reset_index(level=0, drop=True)
    frame["volume_z"] = ((frame["volume"] / rolling_volume.replace(0, np.nan)) - 1.0).replace([np.inf, -np.inf], np.nan)
    for horizon, periods in CRYPTO_HORIZON_TO_PERIODS.items():
        frame[f"target_{horizon}"] = grouped["close"].transform(lambda series, p=periods: _series_future_return(series, p))
        frame[f"class_{horizon}"] = (frame[f"target_{horizon}"] > 0).astype(int)
    return frame


def _prepare_stock_panel(signal_panel: pd.DataFrame, market: str) -> pd.DataFrame:
    frame = signal_panel[signal_panel["market"] == market].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["target_1D"] = grouped["close"].transform(lambda series: _series_future_return(series, 1))
    frame["target_5D"] = grouped["close"].transform(lambda series: _series_future_return(series, 5))
    frame["target_20D"] = grouped["close"].transform(lambda series: _series_future_return(series, 20))
    frame["target_alpha"] = (
        0.20 * frame["target_1D"].fillna(0.0)
        + 0.30 * (frame["target_5D"].fillna(0.0) / 5.0)
        + 0.50 * (frame["target_20D"].fillna(0.0) / 20.0)
    )
    frame["rank_label"] = 0
    for timestamp, group in frame.groupby("timestamp"):
        order = group["target_alpha"].rank(method="first", ascending=True)
        frame.loc[group.index, "rank_label"] = order.astype(int)
    return frame


def _prepare_crypto_daily_panel(signal_panel: pd.DataFrame) -> pd.DataFrame:
    frame = signal_panel[signal_panel["market"] == "crypto"].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["target_1D"] = grouped["close"].transform(lambda series: _series_future_return(series, 1))
    return frame


def _prepare_index_panel(signal_panel: pd.DataFrame) -> pd.DataFrame:
    frame = signal_panel[signal_panel["market"] == "index"].sort_values(["symbol", "timestamp"]).copy()
    if frame.empty:
        return pd.DataFrame()
    grouped = frame.groupby("symbol", group_keys=False)
    for horizon, periods in HORIZON_TO_PERIODS.items():
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
) -> pd.DataFrame:
    clean = frame.dropna(subset=[*feature_cols, target_col]).copy()
    if clean.empty:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "score", "predictedReturn", "modelVersion"])

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
        model = _make_regressor()
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
    market: str,
    model_version: str,
    preferred_step: int = 20,
) -> pd.DataFrame:
    clean = frame.dropna(subset=[*feature_cols, "target_alpha", "rank_label"]).copy()
    if clean.empty:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "score", "predictedReturn", "pred1D", "pred5D", "pred20D", "modelVersion"])

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

        ranker = _make_ranker()
        if LGBMRanker is not None:
            ranker.fit(train[feature_cols].fillna(0.0), train["rank_label"], group=group_sizes)
        else:
            ranker.fit(train[feature_cols].fillna(0.0), train["target_alpha"].fillna(0.0))

        regressors = {}
        for horizon in ["1D", "5D", "20D"]:
            regressor = _make_regressor()
            regressor.fit(train[feature_cols].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
            regressors[horizon] = regressor

        scores = np.asarray(ranker.predict(test[feature_cols].fillna(0.0)), dtype="float64")
        preds_1d = np.asarray(regressors["1D"].predict(test[feature_cols].fillna(0.0)), dtype="float64")
        preds_5d = np.asarray(regressors["5D"].predict(test[feature_cols].fillna(0.0)), dtype="float64")
        preds_20d = np.asarray(regressors["20D"].predict(test[feature_cols].fillna(0.0)), dtype="float64")

        for (_, row), score, pred_1d, pred_5d, pred_20d in zip(
            test.iterrows(), scores, preds_1d, preds_5d, preds_20d, strict=False
        ):
            rows.append(
                {
                    "market": market,
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"],
                    "score": float(score),
                    "predictedReturn": float(0.20 * pred_1d + 0.30 * (pred_5d / 5.0) + 0.50 * (pred_20d / 20.0)),
                    "pred1D": float(pred_1d),
                    "pred5D": float(pred_5d),
                    "pred20D": float(pred_20d),
                    "modelVersion": model_version,
                }
            )

    history = pd.DataFrame(rows)
    if history.empty:
        return history
    history["score"] = _zscore_by_date(history, "score")
    return history


def _fit_stock_latest_models(frame: pd.DataFrame, feature_cols: list[str], market: str, paths: AppPaths) -> tuple[pd.DataFrame, dict[str, float], list[dict]]:
    clean = frame.dropna(subset=[*feature_cols, "target_alpha", "rank_label"]).copy()
    if clean.empty:
        raise ValueError(f"No clean stock training rows for {market}")

    group_sizes = clean.groupby("timestamp").size().tolist()
    ranker = _make_ranker()
    if LGBMRanker is not None:
        ranker.fit(clean[feature_cols].fillna(0.0), clean["rank_label"], group=group_sizes)
    else:
        ranker.fit(clean[feature_cols].fillna(0.0), clean["target_alpha"].fillna(0.0))

    regressors = {}
    for horizon in ["1D", "5D", "20D"]:
        regressor = _make_regressor()
        regressor.fit(clean[feature_cols].fillna(0.0), clean[f"target_{horizon}"].fillna(0.0))
        regressors[horizon] = regressor

    latest_ts = clean["timestamp"].max()
    latest = frame[frame["timestamp"] == latest_ts].dropna(subset=feature_cols).copy()
    if latest.empty:
        raise ValueError(f"No latest stock rows for {market}")

    latest["score"] = np.asarray(ranker.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["score"] = ((latest["score"] - latest["score"].mean()) / (latest["score"].std() or 1.0)).fillna(0.0)
    latest["pred1D"] = np.asarray(regressors["1D"].predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["pred5D"] = np.asarray(regressors["5D"].predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["pred20D"] = np.asarray(regressors["20D"].predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["predictedReturn"] = 0.20 * latest["pred1D"] + 0.30 * (latest["pred5D"] / 5.0) + 0.50 * (latest["pred20D"] / 20.0)

    importance_map = _feature_importance_map(ranker, feature_cols)
    artifact = _artifact_path(paths, market, "ranker.joblib")
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
            "horizon": "1D/5D/20D",
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
                    "meanPred1D": float(latest["pred1D"].mean()),
                    "meanPred5D": float(latest["pred5D"].mean()),
                    "meanPred20D": float(latest["pred20D"].mean()),
                }
            ),
            "message": "LightGBM ranker overlay completed",
        }
    ]
    return latest, importance_map, metric_rows


def _fit_crypto_daily_latest(frame: pd.DataFrame, feature_cols: list[str], paths: AppPaths) -> tuple[pd.DataFrame, dict[str, float], list[dict]]:
    clean = frame.dropna(subset=[*feature_cols, "target_1D"]).copy()
    if clean.empty:
        raise ValueError("No clean crypto daily rows")
    regressor = _make_regressor()
    regressor.fit(clean[feature_cols].fillna(0.0), clean["target_1D"].fillna(0.0))
    latest_ts = clean["timestamp"].max()
    latest = frame[frame["timestamp"] == latest_ts].dropna(subset=feature_cols).copy()
    if latest.empty:
        raise ValueError("No latest crypto daily rows")
    latest["score"] = np.asarray(regressor.predict(latest[feature_cols].fillna(0.0)), dtype="float64")
    latest["score"] = ((latest["score"] - latest["score"].mean()) / (latest["score"].std() or 1.0)).fillna(0.0)
    latest["pred1D"] = latest["score"] * latest["ret_1d"].abs().replace(0.0, latest["ret_1d"].abs().median() or 0.01)
    importance_map = _feature_importance_map(regressor, feature_cols)
    artifact = _artifact_path(paths, "crypto", "daily-regressor.joblib")
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
            "pipeline": "crypto-daily",
            "horizon": "1D",
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
                    "meanPred1D": float(latest["pred1D"].mean()),
                }
            ),
            "message": "Crypto daily overlay completed",
        }
    ]
    return latest, importance_map, metric_rows


def _fit_calibrated_classifier(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
) -> tuple[object, IsotonicRegression | None, float | None]:
    classifier = _make_classifier()
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


def _fit_crypto_hourly_latest(panel: pd.DataFrame, paths: AppPaths) -> tuple[pd.DataFrame, list[dict]]:
    if panel.empty:
        raise ValueError("No crypto hourly panel")
    latest_rows: list[pd.DataFrame] = []
    run_rows: list[dict] = []

    for horizon in CRYPTO_HORIZON_TO_PERIODS:
        clean = panel.dropna(subset=[*CRYPTO_HOURLY_FEATURES, f"target_{horizon}"]).copy()
        if clean.empty:
            continue
        split_index = max(int(len(clean) * 0.8), 1)
        train = clean.iloc[:split_index].copy()
        validation = clean.iloc[split_index:].copy()
        if train.empty:
            train = clean.copy()
            validation = clean.iloc[0:0].copy()

        classifier, calibrator, auc = _fit_calibrated_classifier(train, validation, CRYPTO_HOURLY_FEATURES, f"class_{horizon}")
        mean_regressor = _make_regressor()
        mean_regressor.fit(train[CRYPTO_HOURLY_FEATURES].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))

        lower_regressor = _make_regressor(objective="quantile", alpha=0.10) if LGBMRegressor is not None else _make_regressor()
        upper_regressor = _make_regressor(objective="quantile", alpha=0.90) if LGBMRegressor is not None else _make_regressor()
        lower_regressor.fit(train[CRYPTO_HOURLY_FEATURES].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
        upper_regressor.fit(train[CRYPTO_HOURLY_FEATURES].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))

        latest = panel.sort_values("timestamp").groupby("symbol", as_index=False).tail(1).copy()
        if latest.empty:
            continue
        latest[f"pUp_{horizon}"] = _predict_probability(classifier, calibrator, latest[CRYPTO_HOURLY_FEATURES])
        latest[f"q50_{horizon}"] = np.asarray(mean_regressor.predict(latest[CRYPTO_HOURLY_FEATURES].fillna(0.0)), dtype="float64")
        latest[f"q10_{horizon}"] = np.asarray(lower_regressor.predict(latest[CRYPTO_HOURLY_FEATURES].fillna(0.0)), dtype="float64")
        latest[f"q90_{horizon}"] = np.asarray(upper_regressor.predict(latest[CRYPTO_HOURLY_FEATURES].fillna(0.0)), dtype="float64")

        artifact = _artifact_path(paths, "crypto", f"hourly-{horizon.lower()}.joblib")
        joblib.dump(
            {
                "classifier": classifier,
                "calibrator": calibrator,
                "regressor": mean_regressor,
                "lower": lower_regressor,
                "upper": upper_regressor,
                "feature_cols": CRYPTO_HOURLY_FEATURES,
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
                            np.asarray(lower_regressor.predict(validation[CRYPTO_HOURLY_FEATURES].fillna(0.0)), dtype="float64"),
                            0.10,
                        )
                        if not validation.empty
                        else None,
                        "q90Pinball": _safe_pinball(
                            validation[f"target_{horizon}"].to_numpy(dtype="float64"),
                            np.asarray(upper_regressor.predict(validation[CRYPTO_HOURLY_FEATURES].fillna(0.0)), dtype="float64"),
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


def _fit_index_latest(panel: pd.DataFrame, paths: AppPaths) -> tuple[pd.DataFrame, list[dict]]:
    if panel.empty:
        raise ValueError("No index panel")
    forecast_rows: list[dict] = []
    run_rows: list[dict] = []

    for symbol, symbol_frame in panel.groupby("symbol"):
        latest_row = symbol_frame.sort_values("timestamp").tail(1)
        if latest_row.empty:
            continue
        latest_payload = latest_row.iloc[0]
        summary_by_horizon: dict[str, dict[str, float | str]] = {}
        for horizon in ["1D", "5D", "20D"]:
            clean = symbol_frame.dropna(subset=[*INDEX_FEATURES, f"target_{horizon}", f"class_{horizon}"]).copy()
            if clean.empty:
                continue
            split_index = max(int(len(clean) * 0.8), 1)
            train = clean.iloc[:split_index].copy()
            validation = clean.iloc[split_index:].copy()
            if train.empty:
                train = clean.copy()
                validation = clean.iloc[0:0].copy()
            classifier, calibrator, auc = _fit_calibrated_classifier(train, validation, INDEX_FEATURES, f"class_{horizon}")
            regressor = _make_regressor()
            regressor.fit(train[INDEX_FEATURES].fillna(0.0), train[f"target_{horizon}"].fillna(0.0))
            latest_features = latest_row[INDEX_FEATURES].fillna(0.0)
            probability = float(_predict_probability(classifier, calibrator, latest_features)[0])
            expected = float(np.asarray(regressor.predict(latest_features), dtype="float64")[0])
            residual_scale = float(train[f"target_{horizon}"].std() or 0.02) * math.sqrt(max(HORIZON_TO_PERIODS[horizon], 1))
            summary_by_horizon[horizon] = {
                "pUp": probability,
                "q10": expected - residual_scale,
                "q50": expected,
                "q90": expected + residual_scale,
                "regime": "risk-on" if expected > 0.01 else ("risk-off" if expected < -0.01 else "neutral"),
                "auc": auc if auc is not None else 0.0,
            }

            artifact = _artifact_path(paths, "index", f"{symbol.replace('^', 'IDX_')}-{horizon.lower()}.joblib")
            joblib.dump(
                {
                    "classifier": classifier,
                    "calibrator": calibrator,
                    "regressor": regressor,
                    "feature_cols": INDEX_FEATURES,
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
                    "metricSummary": json.dumps({"rows": int(len(clean)), "validationRows": int(len(validation)), "auc": auc}),
                    "message": f"Index regime model completed for {symbol} {horizon}",
                }
            )

        if summary_by_horizon:
            forecast_rows.append(
                {
                    "symbol": symbol,
                    "timestamp": latest_payload["timestamp"],
                    "payload": summary_by_horizon,
                    "score": float(latest_payload.get("score", 0.0)),
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
    }
    universe_map = _market_universe_map(universe_membership)
    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []

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
            for rebalance_freq in REBALANCE_FREQUENCIES:
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": "long_only",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(row["predictedReturn"]),
                        "targetWeight": float(long_weights.iloc[row["rank"] - 1]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                        "factorExposures": breakdown,
                        "signalFamily": "lgbm_cross_sectional_ranker",
                        "signalBreakdown": breakdown,
                        "asOfDate": latest_ts.date().isoformat(),
                        "modelVersion": STOCK_MODEL_VERSION,
                    }
                )
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": "hedged",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(row["predictedReturn"]),
                        "targetWeight": float(hedged_weights.iloc[row["rank"] - 1]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                        "factorExposures": breakdown,
                        "signalFamily": "lgbm_cross_sectional_ranker",
                        "signalBreakdown": breakdown,
                        "asOfDate": latest_ts.date().isoformat(),
                        "modelVersion": STOCK_MODEL_VERSION,
                    }
                )

            for horizon in ["1D", "5D", "20D"]:
                expected = float(row[f"pred{horizon}"])
                scale = float(abs(expected) + abs(row.get("ret_1d", 0.02) or 0.02) + 0.02)
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
                        "regime": "risk-on" if float(row["pred20D"]) > 0 else "risk-off",
                        "riskFlags": ["ml-overlay", "cross-sectional"],
                        "modelVersion": STOCK_MODEL_VERSION,
                        "asOfDate": latest_ts.date().isoformat(),
                    }
                )

    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _build_crypto_rankings_and_forecasts(
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
            for rebalance_freq in REBALANCE_FREQUENCIES:
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": "long_only",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(row["pred1D"]),
                        "targetWeight": float(long_weights.iloc[row["rank"] - 1]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                        "factorExposures": breakdown,
                        "signalFamily": "crypto_time_series",
                        "signalBreakdown": breakdown,
                        "asOfDate": latest_ts.date().isoformat(),
                        "modelVersion": CRYPTO_MODEL_VERSION,
                    }
                )
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": "hedged",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": float(row["pred1D"]),
                        "targetWeight": float(hedged_weights.iloc[row["rank"] - 1]),
                        "liquidityBucket": _liquidity_bucket(float(row.get("volume", 0.0) or 0.0), volume_median),
                        "factorExposures": breakdown,
                        "signalFamily": "crypto_time_series",
                        "signalBreakdown": breakdown,
                        "asOfDate": latest_ts.date().isoformat(),
                        "modelVersion": CRYPTO_MODEL_VERSION,
                    }
                )

            for horizon in ["1H", "4H", "1D"]:
                q50 = float(row.get(f"q50_{horizon}", row["pred1D"]))
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
                    }
                )

    return pd.DataFrame(ranking_rows), pd.DataFrame(forecast_rows)


def _build_index_forecasts(index_latest: pd.DataFrame, universe_membership: pd.DataFrame) -> pd.DataFrame:
    if index_latest.empty:
        return pd.DataFrame()
    universe_map = _market_universe_map(universe_membership)
    latest_by_symbol = index_latest.set_index("symbol")
    forecast_rows: list[dict] = []
    for universe, (market, symbols) in universe_map.items():
        if market != "index":
            continue
        for symbol in symbols:
            if symbol not in latest_by_symbol.index:
                continue
            row = latest_by_symbol.loc[symbol]
            payload = row["payload"]
            for horizon in ["1D", "5D", "20D"]:
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
                        "riskFlags": ["ml-overlay", "regime-model"],
                        "modelVersion": INDEX_MODEL_VERSION,
                        "asOfDate": pd.Timestamp(row["timestamp"]).date().isoformat(),
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


def build_ml_overlay(paths: AppPaths) -> None:
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

    for market in ["cn_equity", "us_equity"]:
        try:
            stock_panel = _prepare_stock_panel(signal_panel, market)
            history = _walk_forward_ranker(stock_panel, STOCK_FEATURES, market=market, model_version=STOCK_MODEL_VERSION)
            latest, importance_map, metrics = _fit_stock_latest_models(stock_panel, STOCK_FEATURES, market, paths)
            rankings, forecasts = _build_equity_rankings_and_forecasts(latest, memberships, market, importance_map)
            final_rankings = replace_rows_by_keys(final_rankings, rankings, ["symbol", "universe", "rebalanceFreq", "strategyMode"])
            final_forecasts = replace_rows_by_keys(final_forecasts, forecasts, ["symbol", "universe", "horizon"])
            history_frames.append(history)
            run_frames.append(pd.DataFrame(metrics))
        except Exception as exc:
            run_frames.append(_failed_run(market, "equity-ranker", f"{market} overlay failed: {exc}", STOCK_MODEL_VERSION))

    try:
        crypto_daily_panel = _prepare_crypto_daily_panel(signal_panel)
        crypto_history = _walk_forward_regression(
            crypto_daily_panel,
            CRYPTO_DAILY_FEATURES,
            "target_1D",
            preferred_step=10,
            market="crypto",
            model_version=CRYPTO_MODEL_VERSION,
        )
        crypto_daily_latest, crypto_importance, crypto_daily_runs = _fit_crypto_daily_latest(crypto_daily_panel, CRYPTO_DAILY_FEATURES, paths)
        crypto_hourly_panel = _build_crypto_hourly_panel(bars_1h)
        write_frame(paths, "crypto_feature_panel", crypto_hourly_panel)
        crypto_hourly_latest, crypto_hourly_runs = _fit_crypto_hourly_latest(crypto_hourly_panel, paths)
        crypto_rankings, crypto_forecasts = _build_crypto_rankings_and_forecasts(crypto_daily_latest, crypto_hourly_latest, memberships, crypto_importance)
        final_rankings = replace_rows_by_keys(final_rankings, crypto_rankings, ["symbol", "universe", "rebalanceFreq", "strategyMode"])
        final_forecasts = replace_rows_by_keys(final_forecasts, crypto_forecasts, ["symbol", "universe", "horizon"])
        history_frames.append(crypto_history)
        run_frames.append(pd.DataFrame(crypto_daily_runs))
        run_frames.append(pd.DataFrame(crypto_hourly_runs))
    except Exception as exc:
        run_frames.append(_failed_run("crypto", "crypto-overlay", f"crypto overlay failed: {exc}", CRYPTO_MODEL_VERSION))

    try:
        index_panel = _prepare_index_panel(signal_panel)
        index_latest, index_runs = _fit_index_latest(index_panel, paths)
        index_forecasts = _build_index_forecasts(index_latest, memberships)
        final_forecasts = replace_rows_by_keys(final_forecasts, index_forecasts, ["symbol", "universe", "horizon"])
        run_frames.append(pd.DataFrame(index_runs))
    except Exception as exc:
        run_frames.append(_failed_run("index", "index-regime", f"index overlay failed: {exc}", INDEX_MODEL_VERSION))

    history_panel = pd.concat([frame for frame in history_frames if not frame.empty], ignore_index=True) if history_frames else pd.DataFrame()
    if history_panel.empty:
        history_panel = signal_panel[["market", "symbol", "timestamp", "score"]].copy()
        history_panel["predictedReturn"] = history_panel["score"]
        history_panel["modelVersion"] = BASELINE_MODEL_VERSION

    model_runs = pd.concat([frame for frame in run_frames if not frame.empty], ignore_index=True) if run_frames else pd.DataFrame()

    write_frame(paths, "ranking_panel", _ensure_frame(final_rankings, list(final_rankings.columns)))
    write_frame(paths, "forecast_panel", _ensure_frame(final_forecasts, list(final_forecasts.columns)))
    write_frame(paths, "prediction_history_panel", history_panel)
    write_frame(paths, "model_run_panel", model_runs)
    sync_duckdb(paths)


def bootstrap_baseline_outputs(paths: AppPaths) -> None:
    signal_panel = read_frame(paths, "signal_panel")
    asset_master = read_frame(paths, "asset_master")
    memberships = read_frame(paths, "universe_membership")
    rankings, forecasts = build_rankings_and_forecasts(signal_panel, asset_master, memberships)
    write_frame(paths, "baseline_ranking_panel", rankings)
    write_frame(paths, "baseline_forecast_panel", forecasts)
    write_frame(paths, "ranking_panel", rankings)
    write_frame(paths, "forecast_panel", forecasts)
    sync_duckdb(paths)
