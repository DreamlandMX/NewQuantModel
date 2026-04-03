from __future__ import annotations

import math

import numpy as np
import pandas as pd

from newquantmodel.analytics.signals import WEEKLY_RULES, enrich_with_technical_indicators


COMMON_FACTOR_COLUMNS = [
    "ret_1d",
    "ret_3",
    "ret_5",
    "ret_10",
    "ret_20",
    "ret_60",
    "ret_120",
    "mom_term_20_60",
    "mom_accel_5_20",
    "rev_1",
    "rev_3",
    "rev_5",
    "gap_reversal",
    "trend_10_50",
    "price_to_ma20",
    "price_to_ma50",
    "breakout_20",
    "drawdown_20",
    "adx14",
    "aroon_osc",
    "cci20",
    "cmo20",
    "bop20",
    "realized_vol10",
    "realized_vol20",
    "downside_vol20",
    "parkinson20",
    "gk_vol20",
    "atr_pct",
    "atr_term",
    "bb_width",
    "bb_position_centered",
    "bb_squeeze",
    "dollar_volume",
    "volume_shock",
    "amihud20",
    "obv_slope20",
    "cmf20",
    "mfi14",
    "ad_line",
    "time_since_high_20",
    "time_since_low_20",
    "skew20",
    "kurt20",
    "price_delay_5",
    "macd_hist",
    "rsi_bias",
    "kdj_spread",
    "macd_state_code",
    "rsi_state_code",
    "bb_state_code",
    "kdj_state_code",
]

EXTERNAL_FACTOR_COLUMNS = [
    "macro_vix_level",
    "macro_vix_ret_5",
    "macro_dxy_ret_20",
    "macro_tnx_change_20",
    "macro_gold_ret_20",
    "macro_oil_ret_20",
    "macro_equity_rs_20",
    "macro_regime_score",
    "funding_rate",
    "open_interest_change",
    "basis_rate",
    "taker_buy_imbalance",
]

EQUITY_FACTOR_COLUMNS = [
    "z_ret_20",
    "z_ret_60",
    "z_ret_120",
    "z_mom_term_20_60",
    "z_mom_accel_5_20",
    "z_rev_5",
    "z_gap_reversal",
    "z_trend_10_50",
    "z_price_to_ma20",
    "z_price_to_ma50",
    "z_breakout_20",
    "z_drawdown_20",
    "z_realized_vol20",
    "z_downside_vol20",
    "z_parkinson20",
    "z_gk_vol20",
    "z_atr_pct",
    "z_bb_position_centered",
    "z_bb_squeeze",
    "z_dollar_volume",
    "z_volume_shock",
    "z_amihud20",
    "z_obv_slope20",
    "z_cmf20",
    "z_mfi14",
    "z_ad_line",
    "z_time_since_high_20",
    "z_time_since_low_20",
    "z_skew20",
    "z_kurt20",
    "z_price_delay_5",
    "z_macd_hist",
    "z_rsi_bias",
    "z_kdj_spread",
    "adx14",
    "aroon_osc",
    "cci20",
    "cmo20",
    "bop20",
    "macro_regime_score",
]

CRYPTO_FACTOR_COLUMNS = [
    "z_ret_5",
    "z_ret_20",
    "z_ret_60",
    "z_mom_term_20_60",
    "z_mom_accel_5_20",
    "z_rev_3",
    "z_rev_5",
    "z_gap_reversal",
    "z_trend_10_50",
    "z_price_to_ma20",
    "z_breakout_20",
    "z_drawdown_20",
    "z_realized_vol20",
    "z_downside_vol20",
    "z_atr_pct",
    "z_bb_position_centered",
    "z_bb_squeeze",
    "z_dollar_volume",
    "z_volume_shock",
    "z_amihud20",
    "z_obv_slope20",
    "z_cmf20",
    "z_mfi14",
    "z_price_delay_5",
    "z_macd_hist",
    "z_rsi_bias",
    "z_kdj_spread",
    "macro_regime_score",
    "funding_rate",
    "open_interest_change",
    "basis_rate",
    "taker_buy_imbalance",
]

CRYPTO_HOURLY_FACTOR_COLUMNS = [
    "ret_1d",
    "ret_3",
    "ret_5",
    "ret_10",
    "ret_20",
    "mom_accel_5_20",
    "rev_1",
    "rev_3",
    "gap_reversal",
    "trend_10_50",
    "price_to_ma20",
    "breakout_20",
    "realized_vol10",
    "realized_vol20",
    "downside_vol20",
    "atr_pct",
    "bb_width",
    "bb_position_centered",
    "dollar_volume",
    "volume_shock",
    "amihud20",
    "obv_slope20",
    "cmf20",
    "mfi14",
    "price_delay_5",
    "macd_hist",
    "rsi_bias",
    "kdj_spread",
    "funding_rate",
    "open_interest_change",
    "basis_rate",
    "taker_buy_imbalance",
]

INDEX_FACTOR_COLUMNS = [
    "ret_5",
    "ret_20",
    "ret_60",
    "ret_120",
    "mom_term_20_60",
    "mom_accel_5_20",
    "rev_5",
    "trend_10_50",
    "price_to_ma20",
    "price_to_ma50",
    "breakout_20",
    "drawdown_20",
    "adx14",
    "aroon_osc",
    "cci20",
    "cmo20",
    "bop20",
    "realized_vol20",
    "downside_vol20",
    "parkinson20",
    "gk_vol20",
    "atr_pct",
    "atr_term",
    "bb_width",
    "bb_position_centered",
    "bb_squeeze",
    "volume_shock",
    "time_since_high_20",
    "time_since_low_20",
    "skew20",
    "kurt20",
    "price_delay_5",
    "macd_hist",
    "rsi_bias",
    "kdj_spread",
    "macro_vix_level",
    "macro_vix_ret_5",
    "macro_dxy_ret_20",
    "macro_tnx_change_20",
    "macro_gold_ret_20",
    "macro_oil_ret_20",
    "macro_equity_rs_20",
    "macro_regime_score",
]


def _safe_numeric(series: pd.Series, fill: float = 0.0) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.replace([np.inf, -np.inf], np.nan).fillna(fill)


def _zscore_by_date(frame: pd.DataFrame, column: str) -> pd.Series:
    grouped = frame.groupby("timestamp")[column]
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, np.nan)
    return ((frame[column] - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _rolling_autocorr(values: pd.Series, window: int, lag: int = 1) -> pd.Series:
    return values.rolling(window, min_periods=max(window // 2, lag + 2)).apply(
        lambda arr: float(pd.Series(arr).autocorr(lag=lag) or 0.0),
        raw=False,
    )


def _time_since_extreme(series: pd.Series, window: int, kind: str) -> pd.Series:
    def _calc(arr: np.ndarray) -> float:
        if len(arr) == 0:
            return 0.0
        idx = int(np.argmax(arr) if kind == "high" else np.argmin(arr))
        return float(len(arr) - idx - 1)

    return series.rolling(window, min_periods=max(2, window // 2)).apply(_calc, raw=True).fillna(float(window))


def _resample_weekly_bars(bars_1d: pd.DataFrame) -> pd.DataFrame:
    if bars_1d.empty:
        return pd.DataFrame(columns=bars_1d.columns)

    weekly_frames: list[pd.DataFrame] = []
    for market, rule in WEEKLY_RULES.items():
        market_frame = bars_1d[bars_1d["market"] == market].copy()
        if market_frame.empty:
            continue
        for symbol, symbol_frame in market_frame.groupby("symbol", sort=False):
            resampled = (
                symbol_frame.sort_values("timestamp")
                .set_index("timestamp")
                .resample(rule, label="right", closed="right")
                .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                .dropna(subset=["open", "high", "low", "close"])
                .reset_index()
            )
            if resampled.empty:
                continue
            resampled["market"] = market
            resampled["symbol"] = symbol
            weekly_frames.append(resampled[["symbol", "market", "timestamp", "open", "high", "low", "close", "volume"]])
    if not weekly_frames:
        return pd.DataFrame(columns=bars_1d.columns)
    return pd.concat(weekly_frames, ignore_index=True).sort_values(["market", "symbol", "timestamp"]).reset_index(drop=True)


def build_external_factor_panel_view(signal_panel: pd.DataFrame, external_factor_panel: pd.DataFrame | None) -> pd.DataFrame:
    if signal_panel.empty or external_factor_panel is None or external_factor_panel.empty:
        return signal_panel
    merge_keys = ["market", "symbol", "timestamp", "signalFrequency"]
    available = [key for key in merge_keys if key in signal_panel.columns and key in external_factor_panel.columns]
    if len(available) != len(merge_keys):
        return signal_panel
    merged = signal_panel.merge(external_factor_panel, on=merge_keys, how="left", suffixes=("", "_ext"))
    return merged


def candidate_factor_columns(market: str, signal_frequency: str) -> list[str]:
    if market in {"cn_equity", "us_equity"}:
        return EQUITY_FACTOR_COLUMNS
    if market == "crypto":
        return CRYPTO_HOURLY_FACTOR_COLUMNS if signal_frequency == "hourly" else CRYPTO_FACTOR_COLUMNS
    return INDEX_FACTOR_COLUMNS


def _base_symbol_factors(symbol_frame: pd.DataFrame) -> pd.DataFrame:
    frame = symbol_frame.sort_values("timestamp").copy()
    close = frame["close"].astype("float64")
    open_ = frame["open"].astype("float64")
    high = frame["high"].astype("float64")
    low = frame["low"].astype("float64")
    volume = frame["volume"].astype("float64")
    prev_close = close.shift(1)

    ret_1d = close.pct_change()
    ret_3 = close.pct_change(3)
    ret_5 = close.pct_change(5)
    ret_10 = close.pct_change(10)
    ret_20 = close.pct_change(20)
    ret_60 = close.pct_change(60)
    ret_120 = close.pct_change(120)

    ma20 = close.rolling(20, min_periods=10).mean()
    ma50 = close.rolling(50, min_periods=20).mean()
    ema10 = close.ewm(span=10, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    highest20 = high.rolling(20, min_periods=10).max()
    lowest20 = low.rolling(20, min_periods=10).min()

    up_move = high.diff()
    down_move = low.shift(1) - low
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=14).mean()
    atr28 = tr.rolling(28, min_periods=14).mean()
    plus_di = 100.0 * plus_dm.rolling(14, min_periods=14).sum() / tr.rolling(14, min_periods=14).sum().replace(0.0, np.nan)
    minus_di = 100.0 * minus_dm.rolling(14, min_periods=14).sum() / tr.rolling(14, min_periods=14).sum().replace(0.0, np.nan)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)) * 100.0
    adx14 = dx.rolling(14, min_periods=14).mean().fillna(0.0)

    periods = 20
    aroon_up = high.rolling(periods + 1, min_periods=max(10, periods // 2)).apply(
        lambda arr: 100.0 * (periods - (len(arr) - 1 - int(np.argmax(arr)))) / periods,
        raw=True,
    )
    aroon_down = low.rolling(periods + 1, min_periods=max(10, periods // 2)).apply(
        lambda arr: 100.0 * (periods - (len(arr) - 1 - int(np.argmin(arr)))) / periods,
        raw=True,
    )
    aroon_osc = (aroon_up - aroon_down).fillna(0.0)

    typical_price = (high + low + close) / 3.0
    tp_sma = typical_price.rolling(20, min_periods=10).mean()
    mean_dev = typical_price.rolling(20, min_periods=10).apply(lambda arr: float(np.mean(np.abs(arr - np.mean(arr)))), raw=True)
    cci20 = ((typical_price - tp_sma) / (0.015 * mean_dev.replace(0.0, np.nan))).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    sum_up = up.rolling(20, min_periods=10).sum()
    sum_down = down.rolling(20, min_periods=10).sum()
    cmo20 = ((sum_up - sum_down) / (sum_up + sum_down).replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    bop = ((close - open_) / (high - low).replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    bop20 = bop.rolling(20, min_periods=10).mean().fillna(0.0)

    realized_vol10 = ret_1d.rolling(10, min_periods=5).std().fillna(0.0)
    realized_vol20 = ret_1d.rolling(20, min_periods=10).std().fillna(0.0)
    downside_vol20 = ret_1d.where(ret_1d < 0.0, 0.0).rolling(20, min_periods=10).std().fillna(0.0)

    parkinson = np.sqrt(((np.log(high / low.replace(0.0, np.nan)) ** 2) / (4.0 * math.log(2.0))).rolling(20, min_periods=10).mean()).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    gk_var = (
        0.5 * (np.log(high / low.replace(0.0, np.nan)) ** 2)
        - (2.0 * math.log(2.0) - 1.0) * (np.log(close / open_.replace(0.0, np.nan)) ** 2)
    )
    gk_vol20 = np.sqrt(gk_var.clip(lower=0.0).rolling(20, min_periods=10).mean()).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    dollar_volume = (close * volume).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    volume_ma20 = volume.rolling(20, min_periods=10).mean()
    volume_shock = (volume / volume_ma20.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    amihud20 = (ret_1d.abs() / dollar_volume.replace(0.0, np.nan)).rolling(20, min_periods=10).mean().replace([np.inf, -np.inf], np.nan).fillna(0.0)

    obv = (np.sign(delta.fillna(0.0)) * volume).cumsum()
    obv_slope20 = (obv - obv.shift(20)) / volume.rolling(20, min_periods=10).sum().replace(0.0, np.nan)
    obv_slope20 = obv_slope20.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    money_flow_multiplier = (((close - low) - (high - close)) / (high - low).replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    money_flow_volume = money_flow_multiplier * volume
    cmf20 = (money_flow_volume.rolling(20, min_periods=10).sum() / volume.rolling(20, min_periods=10).sum().replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    raw_money_flow = typical_price * volume
    direction = typical_price.diff()
    positive_flow = raw_money_flow.where(direction > 0, 0.0).rolling(14, min_periods=7).sum()
    negative_flow = raw_money_flow.where(direction < 0, 0.0).rolling(14, min_periods=7).sum().abs()
    money_ratio = positive_flow / negative_flow.replace(0.0, np.nan)
    mfi14 = (100.0 - (100.0 / (1.0 + money_ratio))).replace([np.inf, -np.inf], np.nan).fillna(50.0)

    ad_line = money_flow_volume.cumsum().fillna(0.0)

    frame["ret_1d"] = ret_1d.fillna(0.0)
    frame["ret_3"] = ret_3.fillna(0.0)
    frame["ret_5"] = ret_5.fillna(0.0)
    frame["ret_10"] = ret_10.fillna(0.0)
    frame["ret_20"] = ret_20.fillna(0.0)
    frame["ret_60"] = ret_60.fillna(0.0)
    frame["ret_120"] = ret_120.fillna(0.0)
    frame["mom_term_20_60"] = (ret_20 - ret_60).fillna(0.0)
    frame["mom_accel_5_20"] = (ret_5 - ret_20).fillna(0.0)
    frame["rev_1"] = (-ret_1d).fillna(0.0)
    frame["rev_3"] = (-ret_3).fillna(0.0)
    frame["rev_5"] = (-ret_5).fillna(0.0)
    frame["gap_reversal"] = ((close / open_.replace(0.0, np.nan) - 1.0) - (open_ / prev_close.replace(0.0, np.nan) - 1.0)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["trend_10_50"] = (ema10 / ema50.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["price_to_ma20"] = (close / ma20.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["price_to_ma50"] = (close / ma50.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["breakout_20"] = (close / highest20.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["drawdown_20"] = (close / highest20.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["adx14"] = adx14
    frame["aroon_osc"] = aroon_osc
    frame["cci20"] = cci20
    frame["cmo20"] = cmo20
    frame["bop20"] = bop20
    frame["realized_vol10"] = realized_vol10
    frame["realized_vol20"] = realized_vol20
    frame["downside_vol20"] = downside_vol20
    frame["parkinson20"] = parkinson
    frame["gk_vol20"] = gk_vol20
    frame["dollar_volume"] = dollar_volume
    frame["volume_shock"] = volume_shock
    frame["amihud20"] = amihud20
    frame["obv_slope20"] = obv_slope20
    frame["cmf20"] = cmf20
    frame["mfi14"] = mfi14
    frame["ad_line"] = ad_line
    frame["time_since_high_20"] = _time_since_extreme(close, 20, "high")
    frame["time_since_low_20"] = _time_since_extreme(close, 20, "low")
    frame["skew20"] = ret_1d.rolling(20, min_periods=10).skew().fillna(0.0)
    frame["kurt20"] = ret_1d.rolling(20, min_periods=10).kurt().fillna(0.0)
    frame["price_delay_5"] = _rolling_autocorr(ret_1d.fillna(0.0), 5).fillna(0.0)
    frame["atr_term"] = (atr14 / atr28.replace(0.0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return frame


def _apply_cross_sectional_zscores(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for column in columns:
        out[f"z_{column}"] = _zscore_by_date(out, column)
    return out


def _heuristic_score(frame: pd.DataFrame, market: str) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="float64")
    if market in {"cn_equity", "us_equity"}:
        terms = {
            "z_ret_20": 0.12,
            "z_ret_60": 0.16,
            "z_ret_120": 0.12,
            "z_mom_term_20_60": 0.05,
            "z_mom_accel_5_20": 0.04,
            "z_rev_5": 0.04,
            "z_trend_10_50": 0.10,
            "z_price_to_ma20": 0.05,
            "z_breakout_20": 0.06,
            "z_macd_hist": 0.05,
            "z_rsi_bias": 0.04,
            "z_kdj_spread": 0.03,
            "z_realized_vol20": -0.08,
            "z_downside_vol20": -0.08,
            "z_atr_pct": -0.05,
            "z_amihud20": -0.04,
            "z_dollar_volume": 0.03,
            "macro_regime_score": 0.02,
        }
    elif market == "crypto":
        terms = {
            "z_ret_5": 0.12,
            "z_ret_20": 0.16,
            "z_ret_60": 0.10,
            "z_mom_term_20_60": 0.05,
            "z_mom_accel_5_20": 0.06,
            "z_rev_3": 0.04,
            "z_gap_reversal": 0.03,
            "z_trend_10_50": 0.10,
            "z_breakout_20": 0.08,
            "z_macd_hist": 0.06,
            "z_rsi_bias": 0.04,
            "z_kdj_spread": 0.04,
            "z_realized_vol20": -0.10,
            "z_atr_pct": -0.05,
            "z_volume_shock": 0.03,
            "z_amihud20": -0.04,
            "funding_rate": -0.03,
            "basis_rate": 0.03,
            "taker_buy_imbalance": 0.03,
            "macro_regime_score": 0.02,
        }
    else:
        terms = {
            "ret_20": 0.20,
            "ret_60": 0.16,
            "mom_term_20_60": 0.05,
            "trend_10_50": 0.12,
            "price_to_ma20": 0.08,
            "drawdown_20": 0.12,
            "macd_hist": 0.06,
            "rsi_bias": 0.04,
            "kdj_spread": 0.03,
            "realized_vol20": -0.10,
            "downside_vol20": -0.06,
            "atr_pct": -0.04,
            "macro_vix_ret_5": -0.06,
            "macro_dxy_ret_20": -0.04,
            "macro_equity_rs_20": 0.08,
            "macro_regime_score": 0.10,
        }
    score = pd.Series(0.0, index=frame.index, dtype="float64")
    for column, weight in terms.items():
        if column in frame.columns:
            score = score + _safe_numeric(frame[column]) * weight
    return score


def build_market_factor_panel(
    bars: pd.DataFrame,
    *,
    market: str,
    signal_frequency: str,
    source_frequency: str,
    external_factor_panel: pd.DataFrame | None = None,
) -> pd.DataFrame:
    scoped = bars[bars["market"] == market].sort_values(["symbol", "timestamp"]).copy()
    if scoped.empty:
        return pd.DataFrame()

    frames = [_base_symbol_factors(group) for _, group in scoped.groupby("symbol", sort=False)]
    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames, ignore_index=True)
    panel = enrich_with_technical_indicators(panel)
    panel["bb_squeeze"] = (-_safe_numeric(panel["bb_width"])).clip(-5.0, 5.0)
    panel["mom20"] = panel["ret_20"]
    panel["mom60"] = panel["ret_60"]
    panel["mom5"] = panel["ret_5"]
    panel["vol20"] = panel["realized_vol20"]
    panel["low_vol"] = -panel["realized_vol20"]
    panel["liquidity"] = panel["dollar_volume"]
    panel["trend50"] = panel["price_to_ma50"]
    panel["trend60"] = panel["price_to_ma50"]
    panel["trend"] = panel["trend_10_50"]
    panel["drawdown20"] = panel["drawdown_20"]

    zscore_columns = [
        column
        for column in COMMON_FACTOR_COLUMNS
        if column in panel.columns and column not in {"macd_state_code", "rsi_state_code", "bb_state_code", "kdj_state_code"}
    ]
    if market in {"cn_equity", "us_equity", "crypto"}:
        panel = _apply_cross_sectional_zscores(panel, zscore_columns)
        legacy_aliases = {
            "z_mom20": "z_ret_20",
            "z_mom60": "z_ret_60",
            "z_mom5": "z_ret_5",
            "z_low_vol": "z_low_vol",
            "z_liquidity": "z_dollar_volume",
            "z_trend50": "z_price_to_ma50",
            "z_trend": "z_trend_10_50",
            "z_vol20": "z_realized_vol20",
            "z_atr_pct": "z_atr_pct",
        }
        if "z_low_vol" not in panel.columns:
            panel["z_low_vol"] = _zscore_by_date(panel, "low_vol")
        for alias, source in legacy_aliases.items():
            if alias == source:
                continue
            if source in panel.columns:
                panel[alias] = panel[source]

    panel["signalFrequency"] = signal_frequency
    panel["sourceFrequency"] = source_frequency
    panel["isDerivedSignal"] = False

    if external_factor_panel is not None and not external_factor_panel.empty:
        panel = build_external_factor_panel_view(panel, external_factor_panel)
        for column in EXTERNAL_FACTOR_COLUMNS:
            if column not in panel.columns:
                panel[column] = 0.0
            panel[column] = _safe_numeric(panel[column], fill=0.0)
    else:
        for column in EXTERNAL_FACTOR_COLUMNS:
            panel[column] = 0.0

    panel["score"] = _heuristic_score(panel, market)
    return panel.sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def build_multifrequency_signal_panel(
    bars_1d: pd.DataFrame,
    bars_30m: pd.DataFrame | None = None,
    external_factor_panel: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if bars_1d.empty and (bars_30m is None or bars_30m.empty):
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if bars_30m is not None and not bars_30m.empty:
        for market in ["crypto", "cn_equity", "us_equity", "index"]:
            built = build_market_factor_panel(
                bars_30m,
                market=market,
                signal_frequency="intraday",
                source_frequency="intraday",
                external_factor_panel=external_factor_panel[external_factor_panel["signalFrequency"] == "intraday"] if external_factor_panel is not None and not external_factor_panel.empty else None,
            )
            if not built.empty:
                frames.append(built)

    for market in ["crypto", "cn_equity", "us_equity", "index"]:
        built = build_market_factor_panel(
            bars_1d,
            market=market,
            signal_frequency="daily",
            source_frequency="daily",
            external_factor_panel=external_factor_panel[external_factor_panel["signalFrequency"] == "daily"] if external_factor_panel is not None and not external_factor_panel.empty else None,
        )
        if not built.empty:
            frames.append(built)

    weekly_bars = _resample_weekly_bars(bars_1d)
    if not weekly_bars.empty:
        for market in ["crypto", "cn_equity", "us_equity", "index"]:
            built = build_market_factor_panel(
                weekly_bars,
                market=market,
                signal_frequency="weekly",
                source_frequency="weekly",
                external_factor_panel=external_factor_panel[external_factor_panel["signalFrequency"] == "weekly"] if external_factor_panel is not None and not external_factor_panel.empty else None,
            )
            if not built.empty:
                frames.append(built)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["market", "signalFrequency", "symbol", "timestamp"]).reset_index(drop=True)
