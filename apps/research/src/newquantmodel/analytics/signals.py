from __future__ import annotations

import math

import numpy as np
import pandas as pd


WEEKLY_RULES = {
    "crypto": "W-SUN",
    "cn_equity": "W-FRI",
    "us_equity": "W-FRI",
    "index": "W-FRI",
}

INDICATOR_PAYLOAD_COLUMNS = [
    "indicatorUnavailable",
    "macdLine",
    "macdSignal",
    "macdHist",
    "macdState",
    "rsi14",
    "rsiState",
    "atr14",
    "atrPct",
    "bbUpper",
    "bbMid",
    "bbLower",
    "bbWidth",
    "bbPosition",
    "bbState",
    "kValue",
    "dValue",
    "jValue",
    "kdjState",
]


def _zscore_by_date(frame: pd.DataFrame, column: str) -> pd.Series:
    grouped = frame.groupby("timestamp")[column]
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, np.nan)
    return ((frame[column] - mean) / std).fillna(0.0)


def _safe_indicator_value(value: object, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if not np.isfinite(parsed):
        return fallback
    return float(parsed)


def _state_series(index: pd.Index, default: str = "unavailable") -> pd.Series:
    return pd.Series(default, index=index, dtype="object")


def enrich_with_technical_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    enriched_frames: list[pd.DataFrame] = []
    for _, symbol_frame in frame.groupby("symbol", sort=False):
        symbol_frame = symbol_frame.sort_values("timestamp").copy()
        close = symbol_frame["close"].astype("float64")
        high = symbol_frame["high"].astype("float64")
        low = symbol_frame["low"].astype("float64")
        prev_close = close.shift(1)

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal
        macd_state = _state_series(symbol_frame.index)
        bullish_cross = (macd_line > macd_signal) & (macd_line.shift(1) <= macd_signal.shift(1))
        bearish_cross = (macd_line < macd_signal) & (macd_line.shift(1) >= macd_signal.shift(1))
        macd_state.loc[bullish_cross] = "bullish_cross"
        macd_state.loc[bearish_cross] = "bearish_cross"
        macd_state.loc[(macd_line > macd_signal) & ~bullish_cross] = "above_signal"
        macd_state.loc[(macd_line < macd_signal) & ~bearish_cross] = "below_signal"
        macd_state_code = np.select(
            [macd_state.eq("bullish_cross"), macd_state.eq("bearish_cross"), macd_state.eq("above_signal"), macd_state.eq("below_signal")],
            [1.0, -1.0, 0.5, -0.5],
            default=0.0,
        )

        delta = close.diff()
        gains = delta.clip(lower=0.0)
        losses = (-delta).clip(lower=0.0)
        avg_gain = gains.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        avg_loss = losses.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
        rs = avg_gain / avg_loss.replace(0.0, np.nan)
        rsi14 = 100.0 - (100.0 / (1.0 + rs))
        rsi14 = rsi14.replace([np.inf, -np.inf], np.nan).fillna(50.0)
        rsi_state = _state_series(symbol_frame.index, default="neutral")
        rsi_state.loc[rsi14 <= 30.0] = "oversold"
        rsi_state.loc[rsi14 >= 70.0] = "overbought"
        rsi_state_code = np.select([rsi_state.eq("oversold"), rsi_state.eq("overbought")], [1.0, -1.0], default=0.0)

        true_range = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr14 = true_range.rolling(14, min_periods=14).mean()
        atr_pct = (atr14 / close.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

        bb_mid = close.rolling(20, min_periods=20).mean()
        bb_std = close.rolling(20, min_periods=20).std()
        bb_upper = bb_mid + 2.0 * bb_std
        bb_lower = bb_mid - 2.0 * bb_std
        bb_width = ((bb_upper - bb_lower) / bb_mid.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        bb_range = (bb_upper - bb_lower).replace(0.0, np.nan)
        bb_position = ((close - bb_lower) / bb_range).replace([np.inf, -np.inf], np.nan).fillna(0.5)
        bb_state = _state_series(symbol_frame.index, default="inside_band")
        bb_state.loc[close >= bb_upper.fillna(np.inf)] = "above_upper"
        bb_state.loc[close <= bb_lower.fillna(-np.inf)] = "below_lower"
        bb_state.loc[(bb_state == "inside_band") & (bb_position >= 0.65)] = "upper_half"
        bb_state.loc[(bb_state == "inside_band") & (bb_position <= 0.35)] = "lower_half"
        bb_state_code = np.select(
            [bb_state.eq("above_upper"), bb_state.eq("below_lower"), bb_state.eq("upper_half"), bb_state.eq("lower_half")],
            [1.0, -1.0, 0.5, -0.5],
            default=0.0,
        )

        lowest_low = low.rolling(14, min_periods=14).min()
        highest_high = high.rolling(14, min_periods=14).max()
        kdj_range = (highest_high - lowest_low).replace(0.0, np.nan)
        rsv = (((close - lowest_low) / kdj_range) * 100.0).replace([np.inf, -np.inf], np.nan).fillna(50.0)
        k_value = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        d_value = k_value.ewm(alpha=1 / 3, adjust=False).mean()
        j_value = 3.0 * k_value - 2.0 * d_value
        kdj_state = _state_series(symbol_frame.index)
        kdj_bullish = (k_value > d_value) & (k_value.shift(1) <= d_value.shift(1))
        kdj_bearish = (k_value < d_value) & (k_value.shift(1) >= d_value.shift(1))
        kdj_state.loc[kdj_bullish] = "bullish_cross"
        kdj_state.loc[kdj_bearish] = "bearish_cross"
        kdj_state.loc[(k_value > d_value) & ~kdj_bullish] = "above_signal"
        kdj_state.loc[(k_value < d_value) & ~kdj_bearish] = "below_signal"
        kdj_state_code = np.select(
            [kdj_state.eq("bullish_cross"), kdj_state.eq("bearish_cross"), kdj_state.eq("above_signal"), kdj_state.eq("below_signal")],
            [1.0, -1.0, 0.5, -0.5],
            default=0.0,
        )

        symbol_frame["macd_line"] = macd_line.fillna(0.0)
        symbol_frame["macd_signal"] = macd_signal.fillna(0.0)
        symbol_frame["macd_hist"] = macd_hist.fillna(0.0)
        symbol_frame["macd_cross_state"] = macd_state
        symbol_frame["macd_state_code"] = macd_state_code
        symbol_frame["rsi14"] = rsi14
        symbol_frame["rsi_state"] = rsi_state
        symbol_frame["rsi_state_code"] = rsi_state_code
        symbol_frame["atr14"] = atr14.fillna(0.0)
        symbol_frame["atr_pct"] = atr_pct
        symbol_frame["bb_mid"] = bb_mid.fillna(close)
        symbol_frame["bb_upper"] = bb_upper.fillna(close)
        symbol_frame["bb_lower"] = bb_lower.fillna(close)
        symbol_frame["bb_width"] = bb_width
        symbol_frame["bb_position"] = bb_position.clip(0.0, 1.0)
        symbol_frame["bb_break_state"] = bb_state
        symbol_frame["bb_state_code"] = bb_state_code
        symbol_frame["k_value"] = k_value.fillna(50.0)
        symbol_frame["d_value"] = d_value.fillna(50.0)
        symbol_frame["j_value"] = j_value.fillna(50.0)
        symbol_frame["kdj_cross_state"] = kdj_state
        symbol_frame["kdj_state_code"] = kdj_state_code
        symbol_frame["rsi_bias"] = ((symbol_frame["rsi14"] - 50.0) / 50.0).clip(-1.0, 1.0)
        symbol_frame["bb_position_centered"] = ((symbol_frame["bb_position"] - 0.5) * 2.0).clip(-1.0, 1.0)
        symbol_frame["kdj_spread"] = ((symbol_frame["k_value"] - symbol_frame["d_value"]) / 100.0).clip(-2.0, 2.0)
        indicator_nan = (
            atr14.isna()
            | bb_mid.isna()
            | highest_high.isna()
            | lowest_low.isna()
        )
        symbol_frame["indicator_unavailable"] = indicator_nan.fillna(True)
        enriched_frames.append(symbol_frame)

    if not enriched_frames:
        return frame
    return pd.concat(enriched_frames, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def indicator_payload_from_row(row: pd.Series | dict) -> dict[str, object]:
    indicator_unavailable = bool(row.get("indicator_unavailable", False) or row.get("indicatorUnavailable", False))
    return {
        "indicatorUnavailable": indicator_unavailable,
        "macdLine": _safe_indicator_value(row.get("macd_line", row.get("macdLine"))),
        "macdSignal": _safe_indicator_value(row.get("macd_signal", row.get("macdSignal"))),
        "macdHist": _safe_indicator_value(row.get("macd_hist", row.get("macdHist"))),
        "macdState": str(row.get("macd_cross_state", row.get("macdState")) or "unavailable"),
        "rsi14": _safe_indicator_value(row.get("rsi14")),
        "rsiState": str(row.get("rsi_state", row.get("rsiState")) or "unavailable"),
        "atr14": _safe_indicator_value(row.get("atr14")),
        "atrPct": _safe_indicator_value(row.get("atr_pct", row.get("atrPct"))),
        "bbUpper": _safe_indicator_value(row.get("bb_upper", row.get("bbUpper"))),
        "bbMid": _safe_indicator_value(row.get("bb_mid", row.get("bbMid"))),
        "bbLower": _safe_indicator_value(row.get("bb_lower", row.get("bbLower"))),
        "bbWidth": _safe_indicator_value(row.get("bb_width", row.get("bbWidth"))),
        "bbPosition": _safe_indicator_value(row.get("bb_position", row.get("bbPosition")), fallback=0.5),
        "bbState": str(row.get("bb_break_state", row.get("bbState")) or "unavailable"),
        "kValue": _safe_indicator_value(row.get("k_value", row.get("kValue")), fallback=50.0),
        "dValue": _safe_indicator_value(row.get("d_value", row.get("dValue")), fallback=50.0),
        "jValue": _safe_indicator_value(row.get("j_value", row.get("jValue")), fallback=50.0),
        "kdjState": str(row.get("kdj_cross_state", row.get("kdjState")) or "unavailable"),
    }


def _build_equity_panel(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["mom60"] = grouped["close"].pct_change(60)
    frame["low_vol"] = -grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["liquidity"] = grouped["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    frame["trend50"] = grouped["close"].transform(lambda series: series / series.rolling(50).mean() - 1.0)
    frame = enrich_with_technical_indicators(frame)
    for column in ["mom20", "mom60", "low_vol", "liquidity", "trend50", "macd_hist", "rsi_bias", "bb_position_centered", "kdj_spread", "atr_pct"]:
        frame[f"z_{column}"] = _zscore_by_date(frame, column)
    frame["score"] = (
        0.25 * frame["z_mom20"]
        + 0.30 * frame["z_mom60"]
        + 0.20 * frame["z_low_vol"]
        + 0.10 * frame["z_liquidity"]
        + 0.15 * frame["z_trend50"]
        + 0.08 * frame["z_macd_hist"]
        + 0.05 * frame["z_rsi_bias"]
        + 0.03 * frame["z_bb_position_centered"]
        + 0.03 * frame["z_kdj_spread"]
        - 0.04 * frame["z_atr_pct"]
    )
    return frame


def _build_crypto_panel(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["mom5"] = grouped["close"].pct_change(5)
    frame["vol20"] = grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["ema20"] = grouped["close"].transform(lambda series: series.ewm(span=20, adjust=False).mean())
    frame["ema50"] = grouped["close"].transform(lambda series: series.ewm(span=50, adjust=False).mean())
    frame["trend"] = frame["ema20"] / frame["ema50"] - 1.0
    frame = enrich_with_technical_indicators(frame)
    for column in ["mom20", "mom5", "trend", "macd_hist", "rsi_bias", "bb_position_centered", "kdj_spread"]:
        frame[f"z_{column}"] = _zscore_by_date(frame, column)
    frame["z_vol20"] = _zscore_by_date(frame, "vol20")
    frame["z_atr_pct"] = _zscore_by_date(frame, "atr_pct")
    frame["score"] = (
        0.30 * frame["z_mom20"]
        + 0.17 * frame["z_mom5"]
        + 0.18 * frame["z_trend"]
        + 0.10 * frame["z_macd_hist"]
        + 0.07 * frame["z_rsi_bias"]
        + 0.05 * frame["z_bb_position_centered"]
        + 0.05 * frame["z_kdj_spread"]
        - 0.20 * frame["z_vol20"]
        - 0.05 * frame["z_atr_pct"]
    )
    return frame


def _build_index_panel(bars: pd.DataFrame) -> pd.DataFrame:
    frame = bars.sort_values(["symbol", "timestamp"]).copy()
    grouped = frame.groupby("symbol", group_keys=False)
    frame["ret_1d"] = grouped["close"].pct_change()
    frame["mom20"] = grouped["close"].pct_change(20)
    frame["trend60"] = grouped["close"].transform(lambda series: series / series.rolling(60).mean() - 1.0)
    frame["vol20"] = grouped["ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    frame["drawdown20"] = grouped["close"].transform(lambda series: series / series.rolling(20).max() - 1.0)
    frame = enrich_with_technical_indicators(frame)
    frame["score"] = (
        0.35 * frame["mom20"].fillna(0.0)
        + 0.25 * frame["trend60"].fillna(0.0)
        - 0.20 * frame["vol20"].fillna(0.0)
        + 0.20 * frame["drawdown20"].fillna(0.0)
        + 0.08 * frame["macd_hist"].fillna(0.0)
        + 0.05 * frame["rsi_bias"].fillna(0.0)
        + 0.04 * frame["bb_position_centered"].fillna(0.0)
        + 0.03 * frame["kdj_spread"].fillna(0.0)
        - 0.04 * frame["atr_pct"].fillna(0.0)
    )
    return frame


def _stamp_provenance(
    frame: pd.DataFrame,
    *,
    signal_frequency: str,
    source_frequency: str,
    is_derived_signal: bool,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    stamped = frame.copy()
    stamped["signalFrequency"] = signal_frequency
    stamped["sourceFrequency"] = source_frequency
    stamped["isDerivedSignal"] = is_derived_signal
    return stamped


def _resample_weekly_bars(bars_1d: pd.DataFrame) -> pd.DataFrame:
    if bars_1d.empty:
        return pd.DataFrame(columns=bars_1d.columns)

    weekly_frames: list[pd.DataFrame] = []
    base_columns = ["timestamp", "open", "high", "low", "close", "volume", "symbol", "market"]

    for market, rule in WEEKLY_RULES.items():
        market_frame = bars_1d[bars_1d["market"] == market].copy()
        if market_frame.empty:
            continue
        for symbol, symbol_frame in market_frame.groupby("symbol"):
            resampled = (
                symbol_frame.sort_values("timestamp")
                .set_index("timestamp")
                .resample(rule, label="right", closed="right")
                .agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                    }
                )
                .dropna(subset=["open", "high", "low", "close"])
                .reset_index()
            )
            if resampled.empty:
                continue
            resampled["symbol"] = symbol
            resampled["market"] = market
            weekly_frames.append(resampled[base_columns])

    if not weekly_frames:
        return pd.DataFrame(columns=bars_1d.columns)

    return pd.concat(weekly_frames, ignore_index=True).sort_values(["market", "symbol", "timestamp"]).reset_index(drop=True)


def build_signal_panel(
    bars_1d: pd.DataFrame,
    bars_30m: pd.DataFrame | None = None,
    external_factor_panel: pd.DataFrame | None = None,
) -> pd.DataFrame:
    from newquantmodel.analytics.factor_library import build_multifrequency_signal_panel

    return build_multifrequency_signal_panel(bars_1d, bars_30m, external_factor_panel)


def _horizons_for_market(market: str, signal_frequency: str) -> tuple[list[str], dict[str, float]]:
    if signal_frequency == "intraday":
        return ["30m", "1H", "4H"], {"30m": 1.0, "1H": math.sqrt(2), "4H": math.sqrt(8)}
    if market == "crypto":
        if signal_frequency == "daily":
            return ["1H", "4H", "1D"], {"1H": 0.2, "4H": 0.4, "1D": 1.0}
        return ["1W"], {"1W": 1.0}
    if signal_frequency == "daily":
        return ["1D", "5D", "20D"], {"1D": 1.0, "5D": math.sqrt(5), "20D": math.sqrt(20)}
    return ["1W", "5W", "20W"], {"1W": 1.0, "5W": math.sqrt(5), "20W": math.sqrt(20)}


def build_rankings_and_forecasts(
    signal_panel: pd.DataFrame,
    asset_master: pd.DataFrame,
    universe_membership: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _ = asset_master
    if signal_panel.empty:
        return pd.DataFrame(), pd.DataFrame()

    latest_by_market_frequency = (
        signal_panel.groupby(["market", "signalFrequency"])["timestamp"].max().to_dict()
    )
    current_membership = universe_membership.sort_values("effective_from").drop_duplicates(subset=["symbol", "universe"], keep="last")

    ranking_rows: list[dict] = []
    forecast_rows: list[dict] = []

    universe_map = current_membership.groupby("universe")["symbol"].apply(list).to_dict()
    market_map = current_membership.groupby("universe")["market"].first().to_dict()

    for universe, symbols in universe_map.items():
        market = market_map[universe]
        for signal_frequency in ["intraday", "daily", "weekly"]:
            latest_ts = latest_by_market_frequency.get((market, signal_frequency))
            if latest_ts is None:
                continue
            frame = signal_panel[
                (signal_panel["market"] == market)
                & (signal_panel["signalFrequency"] == signal_frequency)
                & (signal_panel["timestamp"] == latest_ts)
                & (signal_panel["symbol"].isin(symbols))
            ].copy()
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
            horizons, horizon_multiplier = _horizons_for_market(market, signal_frequency)

            for _, row in frame.iterrows():
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

                recent_return = row.get("ret_1d", 0.01)
                if recent_return is None or not np.isfinite(float(recent_return)):
                    recent_return = 0.01
                vol20 = row.get("vol20", 0.02)
                if vol20 is None or not np.isfinite(float(vol20)):
                    vol20 = 0.02
                expected_base = float(row["score"]) * max(abs(float(recent_return)), 0.01)
                sigma = float(abs(float(recent_return)) + abs(float(vol20)))

                for strategy_mode, target_weight in [("long_only", float(long_weights.loc[row.name])), ("hedged", float(hedged_weights.loc[row.name]))]:
                    ranking_rows.append(
                        {
                            "symbol": row["symbol"],
                            "universe": universe,
                            "rebalanceFreq": signal_frequency,
                            "strategyMode": strategy_mode,
                            "score": float(row["score"]),
                            "rank": int(row["rank"]),
                            "expectedReturn": expected_base,
                            "targetWeight": target_weight,
                            "liquidityBucket": "high" if float(row.get("volume", 0.0)) > volume_median else "medium",
                            "factorExposures": breakdown,
                            "signalFamily": "baseline_momentum_volatility",
                            "signalBreakdown": breakdown,
                            "asOfDate": pd.Timestamp(latest_ts).date().isoformat(),
                            "modelVersion": "baseline-signals-v1",
                            "signalFrequency": signal_frequency,
                            "sourceFrequency": signal_frequency,
                            "isDerivedSignal": False,
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
                            "signalFrequency": signal_frequency,
                            "sourceFrequency": signal_frequency,
                            "isDerivedSignal": False,
                            **indicator_payload_from_row(row),
                        }
                    )

    ranking_frame = pd.DataFrame(ranking_rows)
    forecast_frame = pd.DataFrame(forecast_rows)
    return ranking_frame, forecast_frame
