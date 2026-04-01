from __future__ import annotations

import math

import numpy as np
import pandas as pd


def _zscore_by_date(frame: pd.DataFrame, column: str) -> pd.Series:
    grouped = frame.groupby("timestamp")[column]
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, np.nan)
    return ((frame[column] - mean) / std).fillna(0.0)


def _build_equity_panel(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["mom60"] = grouped["close"].pct_change(60)
    frame["low_vol"] = -grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["liquidity"] = grouped["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    frame["trend50"] = grouped["close"].transform(lambda s: s / s.rolling(50).mean() - 1.0)
    for column in ["mom20", "mom60", "low_vol", "liquidity", "trend50"]:
        frame[f"z_{column}"] = _zscore_by_date(frame, column)
    frame["score"] = (
        0.25 * frame["z_mom20"]
        + 0.30 * frame["z_mom60"]
        + 0.20 * frame["z_low_vol"]
        + 0.10 * frame["z_liquidity"]
        + 0.15 * frame["z_trend50"]
    )
    return frame


def _build_crypto_daily_panel(bars_1d: pd.DataFrame) -> pd.DataFrame:
    frame = bars_1d.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["mom5"] = grouped["close"].pct_change(5)
    frame["vol20"] = grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["ema20"] = grouped["close"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    frame["ema50"] = grouped["close"].transform(lambda s: s.ewm(span=50, adjust=False).mean())
    frame["trend"] = frame["ema20"] / frame["ema50"] - 1.0
    for column in ["mom20", "mom5", "trend"]:
        frame[f"z_{column}"] = _zscore_by_date(frame, column)
    frame["z_vol20"] = _zscore_by_date(frame, "vol20")
    frame["score"] = 0.35 * frame["z_mom20"] + 0.20 * frame["z_mom5"] + 0.20 * frame["z_trend"] - 0.25 * frame["z_vol20"]
    return frame


def _build_index_panel(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["trend60"] = grouped["close"].transform(lambda s: s / s.rolling(60).mean() - 1.0)
    frame["vol20"] = grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["drawdown20"] = grouped["close"].transform(lambda s: s / s.rolling(20).max() - 1.0)
    frame["score"] = (
        0.35 * frame["mom20"].fillna(0.0)
        + 0.25 * frame["trend60"].fillna(0.0)
        - 0.20 * frame["vol20"].fillna(0.0)
        + 0.20 * frame["drawdown20"].fillna(0.0)
    )
    return frame


def build_signal_panel(bars_1d: pd.DataFrame) -> pd.DataFrame:
    if bars_1d.empty:
        return pd.DataFrame()
    crypto = _build_crypto_daily_panel(bars_1d[bars_1d["market"] == "crypto"])
    cn_us = _build_equity_panel(bars_1d[bars_1d["market"].isin(["cn_equity", "us_equity"])])
    indices = _build_index_panel(bars_1d[bars_1d["market"] == "index"])
    panel = pd.concat([crypto, cn_us, indices], ignore_index=True)
    return panel.sort_values(["market", "symbol", "timestamp"]).reset_index(drop=True)


def build_rankings_and_forecasts(
    signal_panel: pd.DataFrame,
    asset_master: pd.DataFrame,
    universe_membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if signal_panel.empty:
        return pd.DataFrame(), pd.DataFrame()

    latest_by_market = signal_panel.groupby("market")["timestamp"].max().to_dict()
    current_membership = universe_membership.sort_values("effective_from").drop_duplicates(subset=["symbol", "universe"], keep="last")

    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []

    universe_map = current_membership.groupby("universe")["symbol"].apply(list).to_dict()
    market_map = current_membership.groupby("universe")["market"].first().to_dict()

    for universe, symbols in universe_map.items():
        market = market_map[universe]
        latest_ts = latest_by_market.get(market)
        if latest_ts is None:
            continue
        frame = signal_panel[(signal_panel["market"] == market) & (signal_panel["timestamp"] == latest_ts) & (signal_panel["symbol"].isin(symbols))].copy()
        if frame.empty:
            continue
        frame = frame.sort_values("score", ascending=False)
        frame["rank"] = range(1, len(frame) + 1)
        positive_scores = frame["score"].clip(lower=0)
        long_weight_den = positive_scores.sum() or 1.0
        long_weights = positive_scores / long_weight_den
        short_scores = frame["score"].clip(upper=0).abs()
        short_weight_den = short_scores.sum() or 1.0
        hedged_weights = long_weights - (short_scores / short_weight_den)

        volume_median = float(frame["volume"].median()) if "volume" in frame.columns and not frame["volume"].empty else 0.0

        for _, row in frame.iterrows():
            breakdown = {}
            if market == "crypto":
                breakdown = {
                    "momentum_20d": float(row.get("z_mom20", 0.0)),
                    "momentum_5d": float(row.get("z_mom5", 0.0)),
                    "trend": float(row.get("z_trend", 0.0)),
                    "volatility": float(-row.get("z_vol20", 0.0)),
                }
            elif market in {"cn_equity", "us_equity"}:
                breakdown = {
                    "momentum_20d": float(row.get("z_mom20", 0.0)),
                    "momentum_60d": float(row.get("z_mom60", 0.0)),
                    "low_vol": float(row.get("z_low_vol", 0.0)),
                    "liquidity": float(row.get("z_liquidity", 0.0)),
                    "trend": float(row.get("z_trend50", 0.0)),
                }
            else:
                breakdown = {
                    "momentum_20d": float(row.get("mom20", 0.0)),
                    "trend_60d": float(row.get("trend60", 0.0)),
                    "volatility": float(-row.get("vol20", 0.0)),
                    "drawdown": float(row.get("drawdown20", 0.0)),
                }

            expected_base = float(row["score"]) * max(float(row.get("ret_1d", 0.0) or 0.01), 0.01)
            sigma = float(abs(row.get("ret_1d", 0.02)) + abs(row.get("vol20", 0.02) or 0.02))
            horizons = ["1H", "4H", "1D"] if market == "crypto" else ["1D", "5D", "20D"]
            horizon_multiplier = {"1H": 0.2, "4H": 0.4, "1D": 1.0, "5D": math.sqrt(5), "20D": math.sqrt(20)}

            for rebalance_freq in ["daily", "weekly"]:
                ranking_rows.append(
                    {
                        "symbol": row["symbol"],
                        "universe": universe,
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": "long_only",
                        "score": float(row["score"]),
                        "rank": int(row["rank"]),
                        "expectedReturn": expected_base,
                        "targetWeight": float(long_weights.loc[row.name]),
                        "liquidityBucket": "high" if float(row.get("volume", 0.0)) > volume_median else "medium",
                        "factorExposures": breakdown,
                        "signalFamily": "baseline_momentum_volatility",
                        "signalBreakdown": breakdown,
                        "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                        "modelVersion": "baseline-signals-v1",
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
                        "expectedReturn": expected_base,
                        "targetWeight": float(hedged_weights.loc[row.name]),
                        "liquidityBucket": "high" if float(row.get("volume", 0.0)) > volume_median else "medium",
                        "factorExposures": breakdown,
                        "signalFamily": "baseline_momentum_volatility",
                        "signalBreakdown": breakdown,
                        "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                        "modelVersion": "baseline-signals-v1",
                    }
                )

            for horizon in horizons:
                mult = horizon_multiplier[horizon]
                expected = expected_base * mult
                p_up = 1.0 / (1.0 + math.exp(-float(row["score"])))
                q50 = expected
                band = sigma * mult
                regime = "risk-on" if float(row["score"]) > 0 else "risk-off"
                forecast_rows.append(
                    {
                        "symbol": row["symbol"],
                        "market": market,
                        "universe": universe,
                        "horizon": horizon,
                        "pUp": p_up,
                        "expectedReturn": expected,
                        "q10": q50 - band,
                        "q50": q50,
                        "q90": q50 + band,
                        "alphaScore": float(row["score"]) if market != "index" else 0.0,
                        "confidence": min(0.95, 0.5 + abs(float(row["score"])) / 4.0),
                        "regime": regime,
                        "riskFlags": ["baseline-signal", "data-driven"],
                        "modelVersion": "baseline-signals-v1",
                        "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                    }
                )

    ranking_frame = pd.DataFrame(ranking_rows)
    forecast_frame = pd.DataFrame(forecast_rows)
    return ranking_frame, forecast_frame
