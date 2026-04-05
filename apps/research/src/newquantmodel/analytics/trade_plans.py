from __future__ import annotations

from datetime import timedelta

import pandas as pd


MIN_DIRECTION_PROBABILITY = 0.55
MIN_TRADE_CONFIDENCE = 0.60
MIN_INDICATOR_ALIGNMENT = 0.45
MIN_RISK_REWARD = 1.5
DEFAULT_CONTEXTS = [
    ("long_only", "intraday"),
    ("long_only", "daily"),
    ("long_only", "weekly"),
]

TRADE_PLAN_COLUMNS = [
    "symbol",
    "market",
    "universe",
    "horizon",
    "strategyMode",
    "rebalanceFreq",
    "side",
    "entryBasis",
    "entryPriceMode",
    "entryPrice",
    "stopLossPrice",
    "takeProfitPrice",
    "riskPct",
    "rewardPct",
    "riskRewardRatio",
    "expectedReturn",
    "pUp",
    "confidence",
    "directionProbability",
    "tradeConfidence",
    "srUnavailable",
    "setupType",
    "levelRegime",
    "nearestSupport",
    "nearestResistance",
    "supportDistancePct",
    "resistanceDistancePct",
    "levelStrengthSupport",
    "levelStrengthResistance",
    "entrySource",
    "stopSource",
    "targetSource",
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
    "indicatorAlignmentScore",
    "indicatorNotes",
    "actionable",
    "rejectionReason",
    "selectionRank",
    "selectionReason",
    "conflictGroupKey",
    "executionSymbol",
    "executionMode",
    "validFrom",
    "validUntil",
    "validityMode",
    "nextBarAt",
    "expiresAt",
    "modelVersion",
    "asOfDate",
    "signalFrequency",
    "sourceFrequency",
    "isDerivedSignal",
    "forecastValidity",
    "forecastConflictReason",
    "forecastAdjusted",
    "priceBasis",
    "executionBasis",
]


def _safe_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _latest_price_lookup(
    bars_30m: pd.DataFrame,
    bars_1d: pd.DataFrame,
    bars_1h: pd.DataFrame,
) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], float], dict[tuple[str, str], float]]:
    intraday_lookup: dict[tuple[str, str], float] = {}
    daily_lookup: dict[tuple[str, str], float] = {}
    hourly_lookup: dict[tuple[str, str], float] = {}
    if not bars_30m.empty:
        latest_intraday = bars_30m.sort_values(["market", "symbol", "timestamp"]).drop_duplicates(["market", "symbol"], keep="last")
        intraday_lookup = {
            (str(row["market"]), str(row["symbol"])): float(row["close"])
            for row in latest_intraday.to_dict(orient="records")
            if _safe_float(row.get("close")) is not None
        }
    if not bars_1d.empty:
        latest_daily = bars_1d.sort_values(["market", "symbol", "timestamp"]).drop_duplicates(["market", "symbol"], keep="last")
        daily_lookup = {
            (str(row["market"]), str(row["symbol"])): float(row["close"])
            for row in latest_daily.to_dict(orient="records")
            if _safe_float(row.get("close")) is not None
        }
    if not bars_1h.empty:
        latest_hourly = bars_1h.sort_values(["market", "symbol", "timestamp"]).drop_duplicates(["market", "symbol"], keep="last")
        hourly_lookup = {
            (str(row["market"]), str(row["symbol"])): float(row["close"])
            for row in latest_hourly.to_dict(orient="records")
            if _safe_float(row.get("close")) is not None
        }
    return intraday_lookup, daily_lookup, hourly_lookup


def _resample_weekly_bars(bars_1d: pd.DataFrame) -> pd.DataFrame:
    if bars_1d.empty:
        return pd.DataFrame(columns=bars_1d.columns)

    rules = {
        "crypto": "W-SUN",
        "cn_equity": "W-FRI",
        "us_equity": "W-FRI",
        "index": "W-FRI",
    }
    weekly_frames: list[pd.DataFrame] = []
    for market, rule in rules.items():
        market_frame = bars_1d[bars_1d["market"] == market].copy()
        if market_frame.empty:
            continue
        for symbol, symbol_frame in market_frame.groupby("symbol"):
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
            weekly_frames.append(resampled[["market", "symbol", "timestamp", "open", "high", "low", "close", "volume"]])
    if not weekly_frames:
        return pd.DataFrame(columns=bars_1d.columns)
    return pd.concat(weekly_frames, ignore_index=True).sort_values(["market", "symbol", "timestamp"]).reset_index(drop=True)


def _true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame["close"].shift(1)
    return pd.concat(
        [
            (frame["high"] - frame["low"]).abs(),
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _level_bucket(market: str, horizon: str) -> str:
    horizon = horizon.upper()
    if horizon in {"30M", "1H", "4H"}:
        return "intraday"
    if horizon.endswith("W"):
        return "weekly"
    return "daily"


def _is_crypto_intraday_horizon(market: str, horizon: str) -> bool:
    return str(market) == "crypto" and str(horizon).upper() in {"30M", "1H", "4H"}


def _build_level_lookup(
    bars_30m: pd.DataFrame,
    bars_1d: pd.DataFrame,
    bars_1h: pd.DataFrame,
) -> dict[tuple[str, str, str], dict[str, float | str | bool]]:
    weekly_bars = _resample_weekly_bars(bars_1d)
    sources = {
        "daily": bars_1d[["market", "symbol", "timestamp", "open", "high", "low", "close", "volume"]].copy() if not bars_1d.empty else pd.DataFrame(),
        "intraday": bars_30m[["market", "symbol", "timestamp", "open", "high", "low", "close", "volume"]].copy() if not bars_30m.empty else (
            bars_1h[["market", "symbol", "timestamp", "open", "high", "low", "close", "volume"]].copy() if not bars_1h.empty else pd.DataFrame()
        ),
        "weekly": weekly_bars[["market", "symbol", "timestamp", "open", "high", "low", "close", "volume"]].copy() if not weekly_bars.empty else pd.DataFrame(),
    }

    lookup: dict[tuple[str, str, str], dict[str, float | str | bool]] = {}
    for bucket, source in sources.items():
        if source.empty:
            continue
        for (market, symbol), frame in source.groupby(["market", "symbol"], sort=False):
            frame = frame.sort_values("timestamp").copy()
            if len(frame) < 6:
                lookup[(str(market), str(symbol), bucket)] = {
                    "srUnavailable": True,
                    "setupType": "none",
                    "levelRegime": "unavailable",
                    "nearestSupport": 0.0,
                    "nearestResistance": 0.0,
                    "supportDistancePct": 0.0,
                    "resistanceDistancePct": 0.0,
                    "levelStrengthSupport": 0.0,
                    "levelStrengthResistance": 0.0,
                    "atrValue": 0.0,
                    "nearSupport": False,
                    "nearResistance": False,
                    "breakAboveResistance": False,
                    "breakBelowSupport": False,
                }
                continue

            current = frame.iloc[-1]
            history = frame.iloc[:-1].tail(20 if bucket != "weekly" else 13).copy()
            if history.empty:
                history = frame.iloc[:-1].copy()
            current_close = _safe_float(current.get("close")) or 0.0
            current_open = _safe_float(current.get("open")) or current_close
            support = _safe_float(history["low"].min()) or current_close
            resistance = _safe_float(history["high"].max()) or current_close
            tr = _true_range(frame.tail(20 if bucket != "weekly" else 13))
            atr_value = float(tr.tail(14).mean() or 0.0)
            buffer_value = max(atr_value * 0.6, current_close * 0.003, 1e-6)
            support_distance_pct = abs(current_close - support) / max(current_close, 1e-6)
            resistance_distance_pct = abs(resistance - current_close) / max(current_close, 1e-6)
            near_support = support_distance_pct <= max(buffer_value / max(current_close, 1e-6), 0.012)
            near_resistance = resistance_distance_pct <= max(buffer_value / max(current_close, 1e-6), 0.012)
            break_above = current_close > resistance + buffer_value * 0.35
            break_below = current_close < support - buffer_value * 0.35
            support_touches = int(((history["low"] - support).abs() <= buffer_value).sum())
            resistance_touches = int(((history["high"] - resistance).abs() <= buffer_value).sum())
            support_strength = _clamp(support_touches / 3.0)
            resistance_strength = _clamp(resistance_touches / 3.0)

            if break_above:
                level_regime = "uptrend_breakout"
            elif break_below:
                level_regime = "downtrend_breakdown"
            elif near_support or near_resistance:
                level_regime = "range"
            else:
                level_regime = "neutral"

            lookup[(str(market), str(symbol), bucket)] = {
                "srUnavailable": False,
                "setupType": "none",
                "levelRegime": level_regime,
                "nearestSupport": float(support),
                "nearestResistance": float(resistance),
                "supportDistancePct": float(support_distance_pct),
                "resistanceDistancePct": float(resistance_distance_pct),
                "levelStrengthSupport": float(support_strength),
                "levelStrengthResistance": float(resistance_strength),
                "atrValue": float(atr_value),
                "nearSupport": bool(near_support),
                "nearResistance": bool(near_resistance),
                "breakAboveResistance": bool(break_above),
                "breakBelowSupport": bool(break_below),
                "currentOpen": float(current_open),
                "currentClose": float(current_close),
            }
    return lookup


def _entry_price(
    market: str,
    symbol: str,
    horizon: str,
    intraday_lookup: dict[tuple[str, str], float],
    daily_lookup: dict[tuple[str, str], float],
    hourly_lookup: dict[tuple[str, str], float],
) -> float | None:
    if horizon == "30m":
        return intraday_lookup.get((market, symbol)) or hourly_lookup.get((market, symbol)) or daily_lookup.get((market, symbol))
    if horizon in {"1H", "4H"}:
        return hourly_lookup.get((market, symbol)) or intraday_lookup.get((market, symbol)) or daily_lookup.get((market, symbol))
    return daily_lookup.get((market, symbol)) or hourly_lookup.get((market, symbol)) or intraday_lookup.get((market, symbol))


def _latest_bar_timestamp(
    market: str,
    symbol: str,
    horizon: str,
    bars_30m: pd.DataFrame,
    bars_1d: pd.DataFrame,
    bars_1h: pd.DataFrame,
) -> pd.Timestamp:
    def _to_utc(value: object) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    normalized = str(horizon).upper()
    if normalized == "30M" and not bars_30m.empty:
        scoped = bars_30m[(bars_30m["market"] == market) & (bars_30m["symbol"] == symbol)]
        if not scoped.empty:
            return _to_utc(scoped["timestamp"].max())
    if normalized in {"1H", "4H"} and not bars_1h.empty:
        scoped = bars_1h[(bars_1h["market"] == market) & (bars_1h["symbol"] == symbol)]
        if not scoped.empty:
            return _to_utc(scoped["timestamp"].max())
    if normalized.endswith("W") and not bars_1d.empty:
        weekly = _resample_weekly_bars(bars_1d)
        scoped = weekly[(weekly["market"] == market) & (weekly["symbol"] == symbol)]
        if not scoped.empty:
            return _to_utc(scoped["timestamp"].max())
    if not bars_1d.empty:
        scoped = bars_1d[(bars_1d["market"] == market) & (bars_1d["symbol"] == symbol)]
        if not scoped.empty:
            return _to_utc(scoped["timestamp"].max())
    return pd.Timestamp.utcnow().tz_localize("UTC")


def _universe_lookup(universes: list[dict] | None) -> dict[str, dict]:
    universes = universes or []
    return {str(item.get("universe")): item for item in universes if item.get("universe")}


def _parse_proxy_pair(value: object) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    text = str(value).strip()
    if not text:
        return None, None
    parts = [part.strip() for part in text.split("/") if part.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], parts[1]


def _execution_details(asset: dict, market: str, side: str, universe_meta: dict) -> tuple[str | None, str, str | None]:
    tradable_symbol = asset.get("tradableSymbol") or asset.get("symbol")
    hedge_proxy = asset.get("hedgeProxy") or universe_meta.get("tradableProxy")
    is_tradable = bool(asset.get("isTradable"))
    has_perpetual = bool(asset.get("hasPerpetualProxy"))

    if market == "crypto":
        if is_tradable and has_perpetual and tradable_symbol:
            return str(tradable_symbol), "perpetual", None
        return None, "perpetual", "non_tradable_or_missing_perpetual_proxy"

    if market == "cn_equity":
        if side == "short":
            return None, "if_proxy", "cn_equity_short_disabled"
        if is_tradable and tradable_symbol:
            return str(tradable_symbol), "cash_equity", None
        return None, "cash_equity", "non_tradable_cash_equity"

    if market == "us_equity":
        if tradable_symbol:
            return str(tradable_symbol), "cash_equity" if side == "long" else "research_short", None
        return None, "cash_equity", "missing_execution_symbol"

    long_proxy, short_proxy = _parse_proxy_pair(hedge_proxy)
    if side == "long":
        execution_symbol = long_proxy or tradable_symbol
        execution_mode = "proxy_long" if long_proxy else "index_reference"
        if execution_symbol:
            return str(execution_symbol), execution_mode, None
        return None, execution_mode, "missing_long_proxy"

    execution_symbol = short_proxy
    if execution_symbol:
        return str(execution_symbol), "proxy_inverse", None
    return None, "proxy_inverse", "missing_inverse_proxy"


def _price_and_execution_basis(market: str, execution_mode: str) -> tuple[str, str]:
    if market == "index":
        if execution_mode == "proxy_inverse" or execution_mode == "proxy_long":
            return "index_spot", "proxy_etf"
        if execution_mode == "if_proxy":
            return "index_spot", "proxy_future"
        return "index_spot", "index_reference"
    if market == "crypto":
        return "asset_spot", "perpetual"
    if market in {"cn_equity", "us_equity"}:
        return "asset_spot", execution_mode
    return "asset_spot", execution_mode


def _geometry_is_valid(side: str, entry_price: float, stop_loss_price: float, take_profit_price: float) -> bool:
    if entry_price <= 0 or stop_loss_price <= 0 or take_profit_price <= 0:
        return False
    if side == "long":
        return take_profit_price > entry_price > stop_loss_price
    return stop_loss_price > entry_price > take_profit_price


def _index_side_is_consistent(side: str, q10: float | None, q50: float | None, q90: float | None) -> bool:
    if q10 is None or q90 is None:
        return False
    if side == "long":
        if q90 <= 0:
            return False
        if q50 is not None and q50 <= 0:
            return False
        return True
    if q10 >= 0:
        return False
    if q50 is not None and q50 >= 0:
        return False
    return True


def _next_bar_boundary(base: pd.Timestamp, horizon: str) -> pd.Timestamp:
    normalized = str(horizon).upper()
    ts = pd.Timestamp(base)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    if normalized == "30M":
        floored = ts.floor("30min")
        return floored + timedelta(minutes=30)
    if normalized == "1H":
        floored = ts.floor("1h")
        return floored + timedelta(hours=1)
    if normalized == "4H":
        floored = ts.floor("1h")
        bucket_hour = (floored.hour // 4) * 4
        bucket_start = floored.replace(hour=bucket_hour, minute=0, second=0, microsecond=0)
        return bucket_start + timedelta(hours=4)
    if normalized == "1D":
        floored = ts.floor("1d")
        return floored + timedelta(days=1)
    if normalized.endswith("W"):
        floored = ts.floor("1d")
        return floored + timedelta(days=7)
    floored = ts.floor("1d")
    return floored + timedelta(days=1)


def _rejection_reason(reasons: list[str]) -> str | None:
    filtered = [reason for reason in reasons if reason]
    if not filtered:
        return None
    return ", ".join(dict.fromkeys(filtered))


def _append_reason(reason: str | None, extra: str) -> str:
    reasons = []
    if reason:
        reasons.extend(part.strip() for part in str(reason).split(",") if part.strip())
    if extra and extra not in reasons:
        reasons.append(extra)
    return ", ".join(reasons)


def _ranking_contexts(rankings: pd.DataFrame, symbol: str, universe: str, model_version: str, signal_frequency: str) -> list[dict]:
    if not rankings.empty:
        subset = rankings[
            (rankings["symbol"] == symbol)
            & (rankings["universe"] == universe)
            & (rankings["strategyMode"] == "long_only")
            & (rankings["rebalanceFreq"] == signal_frequency)
        ]
        if not subset.empty:
            records: list[dict] = []
            for row in subset.to_dict(orient="records"):
                records.append(
                    {
                        "strategyMode": "long_only",
                        "rebalanceFreq": str(row["rebalanceFreq"]),
                        "modelVersion": str(row.get("modelVersion") or model_version),
                        "rank": int(row.get("rank", 999999) or 999999),
                        "score": _safe_float(row.get("score")) or 0.0,
                        "targetWeight": _safe_float(row.get("targetWeight")) or 0.0,
                        "signalFrequency": str(row.get("signalFrequency") or signal_frequency),
                        "sourceFrequency": str(row.get("sourceFrequency") or signal_frequency),
                        "isDerivedSignal": bool(row.get("isDerivedSignal", False)),
                    }
                )
            return records
    return [
        {
            "strategyMode": "long_only",
            "rebalanceFreq": signal_frequency,
            "modelVersion": model_version,
            "rank": 999999,
            "score": 0.0,
            "targetWeight": 0.0,
            "signalFrequency": signal_frequency,
            "sourceFrequency": signal_frequency,
            "isDerivedSignal": False,
        }
    ]


def _selection_reason(actionable: bool, selection_rank: int) -> str:
    if actionable and selection_rank == 1:
        return "selected_active_trade"
    if selection_rank > 1:
        return "lost_conflict_resolution"
    return "inactive_candidate"


def _freshness_score(as_of_date: object, horizon: str, rebalance_freq: str) -> float:
    if not as_of_date:
        return 0.4
    parsed = pd.to_datetime(as_of_date, utc=True, errors="coerce")
    if pd.isna(parsed):
        return 0.4
    age_days = max((pd.Timestamp.utcnow() - parsed).total_seconds() / 86_400.0, 0.0)
    if horizon in {"30m", "1H", "4H"}:
        tolerance = 0.25 if rebalance_freq == "intraday" else 0.5 if rebalance_freq == "daily" else 1.0
    else:
        tolerance = 2.0 if rebalance_freq == "daily" else 7.0
    return _clamp(1.0 - (age_days / max(tolerance, 1e-6)))


def _trade_confidence(
    *,
    side: str,
    p_up: float,
    expected_return: float,
    risk_pct: float,
    reward_pct: float,
    risk_reward_ratio: float,
    rank_score: float,
    target_weight: float,
    as_of_date: object,
    horizon: str,
    rebalance_freq: str,
) -> tuple[float, float]:
    direction_probability = p_up if side == "long" else (1.0 - p_up)
    return_edge = max(expected_return, 0.0) if side == "long" else abs(min(expected_return, 0.0))
    directional_edge = _clamp((direction_probability - 0.50) / 0.35)
    rr_quality = _clamp((risk_reward_ratio - 1.0) / 2.0) if risk_reward_ratio > 0 else 0.0
    asymmetry = _clamp(reward_pct / max(reward_pct + risk_pct, 1e-6)) if reward_pct > 0 else 0.0
    return_quality = _clamp(return_edge / max(risk_pct + 0.005, 0.005))
    rank_quality = _clamp(abs(rank_score) / 2.5)
    target_quality = _clamp(abs(target_weight) / 0.10)
    freshness = _freshness_score(as_of_date, horizon, rebalance_freq)
    trade_confidence = (
        0.34 * directional_edge
        + 0.20 * rr_quality
        + 0.16 * asymmetry
        + 0.12 * return_quality
        + 0.10 * rank_quality
        + 0.04 * target_quality
        + 0.04 * freshness
    )
    return direction_probability, _clamp(trade_confidence, 0.0, 0.99)


def _indicator_payload(forecast: dict) -> dict[str, object]:
    return {
        "indicatorUnavailable": bool(forecast.get("indicatorUnavailable", False)),
        "macdLine": _safe_float(forecast.get("macdLine")) or 0.0,
        "macdSignal": _safe_float(forecast.get("macdSignal")) or 0.0,
        "macdHist": _safe_float(forecast.get("macdHist")) or 0.0,
        "macdState": str(forecast.get("macdState") or "unavailable"),
        "rsi14": _safe_float(forecast.get("rsi14")) or 0.0,
        "rsiState": str(forecast.get("rsiState") or "unavailable"),
        "atr14": _safe_float(forecast.get("atr14")) or 0.0,
        "atrPct": _safe_float(forecast.get("atrPct")) or 0.0,
        "bbUpper": _safe_float(forecast.get("bbUpper")) or 0.0,
        "bbMid": _safe_float(forecast.get("bbMid")) or 0.0,
        "bbLower": _safe_float(forecast.get("bbLower")) or 0.0,
        "bbWidth": _safe_float(forecast.get("bbWidth")) or 0.0,
        "bbPosition": _safe_float(forecast.get("bbPosition")) or 0.5,
        "bbState": str(forecast.get("bbState") or "unavailable"),
        "kValue": _safe_float(forecast.get("kValue")) or 50.0,
        "dValue": _safe_float(forecast.get("dValue")) or 50.0,
        "jValue": _safe_float(forecast.get("jValue")) or 50.0,
        "kdjState": str(forecast.get("kdjState") or "unavailable"),
    }


def _indicator_alignment(
    *,
    side: str,
    expected_return: float,
    indicators: dict[str, object],
) -> tuple[float, str]:
    if bool(indicators.get("indicatorUnavailable", False)):
        return 0.50, "Indicator warmup incomplete"

    macd_hist = _safe_float(indicators.get("macdHist")) or 0.0
    macd_state = str(indicators.get("macdState") or "unavailable")
    rsi14 = _safe_float(indicators.get("rsi14")) or 50.0
    rsi_state = str(indicators.get("rsiState") or "neutral")
    atr_pct = abs(_safe_float(indicators.get("atrPct")) or 0.0)
    bb_position = _safe_float(indicators.get("bbPosition")) or 0.5
    bb_state = str(indicators.get("bbState") or "inside_band")
    k_value = _safe_float(indicators.get("kValue")) or 50.0
    d_value = _safe_float(indicators.get("dValue")) or 50.0
    kdj_state = str(indicators.get("kdjState") or "unavailable")

    if side == "long":
        macd_score = 1.0 if macd_state in {"bullish_cross", "above_signal"} or macd_hist > 0 else 0.20
        rsi_score = 1.0 if 40.0 <= rsi14 <= 68.0 else 0.75 if rsi_state == "oversold" else 0.25 if rsi_state == "overbought" else 0.55
        kdj_score = 1.0 if kdj_state in {"bullish_cross", "above_signal"} or k_value >= d_value else 0.25
        bb_score = 1.0 if bb_state in {"inside_band", "lower_half"} else 0.75 if bb_state == "above_upper" and expected_return > 0 else 0.25
    else:
        macd_score = 1.0 if macd_state in {"bearish_cross", "below_signal"} or macd_hist < 0 else 0.20
        rsi_score = 1.0 if 32.0 <= rsi14 <= 60.0 else 0.75 if rsi_state == "overbought" else 0.25 if rsi_state == "oversold" else 0.55
        kdj_score = 1.0 if kdj_state in {"bearish_cross", "below_signal"} or k_value <= d_value else 0.25
        bb_score = 1.0 if bb_state in {"inside_band", "upper_half"} else 0.75 if bb_state == "below_lower" and expected_return < 0 else 0.25

    atr_score = _clamp(1.0 - (atr_pct / max(abs(expected_return) * 4.0 + 0.03, 0.03)))
    alignment = _clamp(0.28 * macd_score + 0.22 * rsi_score + 0.18 * kdj_score + 0.18 * bb_score + 0.14 * atr_score)

    macd_note = "MACD bull" if macd_state in {"bullish_cross", "above_signal"} else "MACD bear" if macd_state in {"bearish_cross", "below_signal"} else "MACD flat"
    rsi_note = f"RSI {rsi_state}" if rsi_state != "neutral" else f"RSI {int(round(rsi14))}"
    bb_note = f"BB {bb_state}"
    kdj_note = "KDJ bull" if kdj_state in {"bullish_cross", "above_signal"} else "KDJ bear" if kdj_state in {"bearish_cross", "below_signal"} else "KDJ mixed"
    atr_note = f"ATR {atr_pct * 100:.1f}%"
    return alignment, " | ".join([macd_note, rsi_note, bb_note, kdj_note, atr_note])


def _setup_type(
    *,
    side: str,
    direction_probability: float,
    expected_return: float,
    indicators: dict[str, object],
    level: dict[str, float | str | bool],
) -> str:
    if bool(level.get("srUnavailable", True)):
        return "none"

    near_support = bool(level.get("nearSupport", False))
    near_resistance = bool(level.get("nearResistance", False))
    break_above = bool(level.get("breakAboveResistance", False))
    break_below = bool(level.get("breakBelowSupport", False))
    macd_state = str(indicators.get("macdState") or "unavailable")
    kdj_state = str(indicators.get("kdjState") or "unavailable")
    bb_state = str(indicators.get("bbState") or "unavailable")
    bullish_indicator = macd_state in {"bullish_cross", "above_signal"} or kdj_state in {"bullish_cross", "above_signal"}
    bearish_indicator = macd_state in {"bearish_cross", "below_signal"} or kdj_state in {"bearish_cross", "below_signal"}

    if side == "long":
        if break_above and direction_probability >= MIN_DIRECTION_PROBABILITY and (bullish_indicator or bb_state == "above_upper" or expected_return > 0):
            return "breakout_long"
        if near_support and direction_probability >= MIN_DIRECTION_PROBABILITY and not break_below and (bullish_indicator or expected_return > 0):
            return "bounce_long"
        return "none"

    if break_below and direction_probability >= MIN_DIRECTION_PROBABILITY and (bearish_indicator or bb_state == "below_lower" or expected_return < 0):
        return "breakout_short"
    if near_resistance and direction_probability >= MIN_DIRECTION_PROBABILITY and not break_above and (bearish_indicator or expected_return < 0):
        return "bounce_short"
    return "none"


def _apply_level_targets(
    *,
    side: str,
    setup_type: str,
    entry_price: float,
    stop_loss_price: float,
    take_profit_price: float,
    level: dict[str, float | str | bool],
) -> tuple[float, float, float, str, str, str]:
    if bool(level.get("srUnavailable", True)) or setup_type == "none" or entry_price <= 0:
        return entry_price, stop_loss_price, take_profit_price, "quantile_fallback", "quantile_fallback", "quantile_fallback"

    support = _safe_float(level.get("nearestSupport")) or entry_price
    resistance = _safe_float(level.get("nearestResistance")) or entry_price
    atr_value = abs(_safe_float(level.get("atrValue")) or 0.0)
    atr_buffer = max(atr_value * 0.75, entry_price * 0.0025)
    if side == "long":
        if setup_type == "bounce_long":
            next_entry = max(entry_price, support + 0.25 * atr_buffer)
            next_stop = max(support - atr_buffer, 0.0)
            next_target = max(take_profit_price, resistance)
        else:
            next_entry = max(entry_price, resistance + 0.10 * atr_buffer)
            next_stop = max(resistance - atr_buffer, support - 0.5 * atr_buffer, 0.0)
            next_target = max(take_profit_price, next_entry + max(next_entry - next_stop, atr_buffer) * 2.0)
    else:
        if setup_type == "bounce_short":
            next_entry = min(entry_price, resistance - 0.25 * atr_buffer)
            next_stop = resistance + atr_buffer
            next_target = min(take_profit_price, support)
        else:
            next_entry = min(entry_price, support - 0.10 * atr_buffer)
            next_stop = support + atr_buffer
            next_target = min(take_profit_price, next_entry - max(next_stop - next_entry, atr_buffer) * 2.0)
    return float(next_entry), float(next_stop), float(next_target), "support_resistance", "support_resistance", "support_resistance"


def _has_valid_crypto_intraday_structure(
    *,
    side: str,
    setup_type: str,
    level: dict[str, float | str | bool],
    entry_source: str,
    stop_source: str,
    target_source: str,
) -> bool:
    allowed_setups = {"bounce_long", "breakout_long", "bounce_short", "breakout_short"}
    if bool(level.get("srUnavailable", True)):
        return False
    if setup_type not in allowed_setups:
        return False
    if entry_source != "support_resistance" or stop_source != "support_resistance" or target_source != "support_resistance":
        return False
    if side == "long":
        if setup_type == "bounce_long":
            return bool(level.get("nearSupport", False))
        if setup_type == "breakout_long":
            return bool(level.get("breakAboveResistance", False))
        return False
    if setup_type == "bounce_short":
        return bool(level.get("nearResistance", False))
    if setup_type == "breakout_short":
        return bool(level.get("breakBelowSupport", False))
    return False


def build_trade_plan_panel(
    asset_master: pd.DataFrame,
    forecasts: pd.DataFrame,
    rankings: pd.DataFrame,
    bars_1d: pd.DataFrame,
    bars_1h: pd.DataFrame,
    bars_30m: pd.DataFrame | None = None,
    universes: list[dict] | None = None,
) -> pd.DataFrame:
    if forecasts.empty or asset_master.empty:
        return pd.DataFrame(columns=TRADE_PLAN_COLUMNS)
    bars_30m = bars_30m if bars_30m is not None else pd.DataFrame()

    assets = {
        str(row["symbol"]): row
        for row in asset_master.to_dict(orient="records")
    }
    universe_lookup = _universe_lookup(universes)
    intraday_lookup, daily_lookup, hourly_lookup = _latest_price_lookup(bars_30m, bars_1d, bars_1h)
    level_lookup = _build_level_lookup(bars_30m, bars_1d, bars_1h)

    rows: list[dict] = []
    for forecast in forecasts.to_dict(orient="records"):
        symbol = str(forecast["symbol"])
        asset = assets.get(symbol)
        if not asset:
            continue

        market = str(forecast["market"])
        universe = str(forecast["universe"])
        universe_meta = universe_lookup.get(universe, {})
        signal_frequency = str(forecast.get("signalFrequency") or ("weekly" if str(forecast.get("horizon", "")).endswith("W") else "daily"))
        contexts = _ranking_contexts(rankings, symbol, universe, str(forecast.get("modelVersion") or "baseline-signals-v1"), signal_frequency)

        entry_price = _entry_price(
            market,
            symbol,
            str(forecast["horizon"]),
            intraday_lookup,
            daily_lookup,
            hourly_lookup,
        )
        q10 = _safe_float(forecast.get("q10"))
        q90 = _safe_float(forecast.get("q90"))
        q50 = _safe_float(forecast.get("q50"))
        expected_return = _safe_float(forecast.get("expectedReturn")) or 0.0
        p_up = _safe_float(forecast.get("pUp")) or 0.0
        indicators = _indicator_payload(forecast)
        forecast_validity = str(forecast.get("forecastValidity") or "valid")
        forecast_conflict_reason = str(forecast.get("forecastConflictReason") or "") or None
        forecast_adjusted = bool(forecast.get("forecastAdjusted", False))
        latest_bar_at = _latest_bar_timestamp(market, symbol, str(forecast["horizon"]), bars_30m, bars_1d, bars_1h)
        next_bar_at = _next_bar_boundary(latest_bar_at, str(forecast["horizon"]))
        valid_from = latest_bar_at.isoformat()
        valid_until = next_bar_at.isoformat()

        for context in contexts:
            strategy_mode = str(context["strategyMode"])
            rebalance_freq = "intraday" if _is_crypto_intraday_horizon(market, str(forecast["horizon"])) else str(context["rebalanceFreq"])
            rank_score = _safe_float(context.get("score")) or 0.0
            target_weight = _safe_float(context.get("targetWeight")) or 0.0
            candidate_sides = ["long"] if strategy_mode == "long_only" else ["long", "short"]

            for side in candidate_sides:
                if market == "cn_equity" and side == "short":
                    continue
                if market == "index" and not _index_side_is_consistent(side, q10, q50, q90):
                    continue

                execution_symbol, execution_mode, execution_rejection = _execution_details(asset, market, side, universe_meta)
                price_basis, execution_basis = _price_and_execution_basis(market, execution_mode)
                reasons: list[str] = []
                if entry_price is None or entry_price <= 0:
                    reasons.append("missing_entry_reference_price")
                if forecast_validity == "conflict":
                    reasons.append("forecast_conflict")

                risk_pct = 0.0
                reward_pct = 0.0
                stop_loss_price = float(entry_price or 0.0)
                take_profit_price = float(entry_price or 0.0)

                if q10 is None or q90 is None:
                    reasons.append("missing_quantile_forecast")
                else:
                    if side == "long":
                        if p_up < MIN_DIRECTION_PROBABILITY:
                            reasons.append("direction_not_bullish_enough")
                        if not (q90 > 0 and q10 < 0 and (q50 is None or q50 > 0)):
                            reasons.append("invalid_long_quantile_geometry")
                        risk_pct = abs(min(q10, 0.0))
                        reward_pct = max(q90, 0.0)
                        if entry_price:
                            stop_loss_price = float(entry_price * (1.0 + q10))
                            take_profit_price = float(entry_price * (1.0 + q90))
                    else:
                        if (1.0 - p_up) < MIN_DIRECTION_PROBABILITY:
                            reasons.append("direction_not_bearish_enough")
                        if not (q10 < 0 and q90 > 0 and (q50 is None or q50 < 0)):
                            reasons.append("invalid_short_quantile_geometry")
                        risk_pct = abs(max(q90, 0.0))
                        reward_pct = abs(min(q10, 0.0))
                        if entry_price:
                            stop_loss_price = float(entry_price * (1.0 + q90))
                            take_profit_price = float(entry_price * (1.0 + q10))

                risk_reward_ratio = float(reward_pct / risk_pct) if risk_pct > 0 else 0.0
                if risk_pct <= 0:
                    reasons.append("zero_risk_distance")
                if risk_reward_ratio < MIN_RISK_REWARD:
                    reasons.append("risk_reward_below_1_5x")
                if execution_rejection:
                    reasons.append(execution_rejection)

                direction_probability, trade_confidence = _trade_confidence(
                    side=side,
                    p_up=float(p_up),
                    expected_return=float(expected_return),
                    risk_pct=float(risk_pct),
                    reward_pct=float(reward_pct),
                    risk_reward_ratio=float(risk_reward_ratio),
                    rank_score=float(rank_score),
                    target_weight=float(target_weight),
                    as_of_date=forecast.get("asOfDate"),
                    horizon=str(forecast["horizon"]),
                    rebalance_freq=rebalance_freq,
                )
                level = level_lookup.get((market, symbol, _level_bucket(market, str(forecast["horizon"]))), {
                    "srUnavailable": True,
                    "setupType": "none",
                    "levelRegime": "unavailable",
                    "nearestSupport": 0.0,
                    "nearestResistance": 0.0,
                    "supportDistancePct": 0.0,
                    "resistanceDistancePct": 0.0,
                    "levelStrengthSupport": 0.0,
                    "levelStrengthResistance": 0.0,
                })
                setup_type = _setup_type(
                    side=side,
                    direction_probability=float(direction_probability),
                    expected_return=float(expected_return),
                    indicators=indicators,
                    level=level,
                )
                if setup_type == "none" and not bool(level.get("srUnavailable", True)):
                    reasons.append("no_valid_sr_setup")
                if side == "long" and bool(level.get("nearResistance", False)) and setup_type != "breakout_long":
                    reasons.append("strong_resistance_overhead")
                if side == "short" and bool(level.get("nearSupport", False)) and setup_type != "breakout_short":
                    reasons.append("strong_support_below")

                entry_price_effective, stop_loss_price, take_profit_price, entry_source, stop_source, target_source = _apply_level_targets(
                    side=side,
                    setup_type=setup_type,
                    entry_price=float(entry_price or 0.0),
                    stop_loss_price=float(stop_loss_price),
                    take_profit_price=float(take_profit_price),
                    level=level,
                )
                entry_price = entry_price_effective
                if _is_crypto_intraday_horizon(market, str(forecast["horizon"])) and not _has_valid_crypto_intraday_structure(
                    side=side,
                    setup_type=setup_type,
                    level=level,
                    entry_source=entry_source,
                    stop_source=stop_source,
                    target_source=target_source,
                ):
                    reasons.append("no_valid_sr_setup")
                if side == "long":
                    risk_pct = max((entry_price - stop_loss_price) / max(entry_price, 1e-6), 0.0)
                    reward_pct = max((take_profit_price - entry_price) / max(entry_price, 1e-6), 0.0)
                else:
                    risk_pct = max((stop_loss_price - entry_price) / max(entry_price, 1e-6), 0.0)
                    reward_pct = max((entry_price - take_profit_price) / max(entry_price, 1e-6), 0.0)
                risk_reward_ratio = float(reward_pct / risk_pct) if risk_pct > 0 else 0.0
                if risk_pct <= 0:
                    reasons.append("zero_risk_distance")
                if risk_reward_ratio < MIN_RISK_REWARD:
                    reasons.append("risk_reward_below_1_5x")
                geometry_valid = _geometry_is_valid(side, float(entry_price or 0.0), float(stop_loss_price), float(take_profit_price))
                if not geometry_valid:
                    reasons.append("invalid_long_trade_geometry" if side == "long" else "invalid_short_trade_geometry")
                if market == "index" and (forecast_validity == "conflict" or not geometry_valid):
                    continue

                indicator_alignment_score, indicator_notes = _indicator_alignment(
                    side=side,
                    expected_return=float(expected_return),
                    indicators=indicators,
                )
                trade_confidence = _clamp(0.72 * trade_confidence + 0.28 * indicator_alignment_score, 0.0, 0.99)
                if trade_confidence < MIN_TRADE_CONFIDENCE:
                    reasons.append("trade_confidence_below_threshold")
                if indicator_alignment_score < MIN_INDICATOR_ALIGNMENT:
                    reasons.append("indicator_alignment_below_threshold")

                rows.append(
                    {
                        "symbol": symbol,
                        "market": market,
                        "universe": universe,
                        "horizon": str(forecast["horizon"]),
                        "strategyMode": strategy_mode,
                        "rebalanceFreq": rebalance_freq,
                        "side": side,
                        "entryBasis": "next_bar_open",
                        "entryPriceMode": "planned_last_close_proxy",
                        "entryPrice": float(entry_price or 0.0),
                        "stopLossPrice": stop_loss_price,
                        "takeProfitPrice": take_profit_price,
                        "riskPct": float(risk_pct),
                        "rewardPct": float(reward_pct),
                        "riskRewardRatio": float(risk_reward_ratio),
                        "expectedReturn": float(expected_return),
                        "pUp": float(p_up),
                        "confidence": float(trade_confidence),
                        "directionProbability": float(direction_probability),
                        "tradeConfidence": float(trade_confidence),
                        "srUnavailable": bool(level.get("srUnavailable", True)),
                        "setupType": setup_type,
                        "levelRegime": str(level.get("levelRegime", "unavailable")),
                        "nearestSupport": float(_safe_float(level.get("nearestSupport")) or 0.0),
                        "nearestResistance": float(_safe_float(level.get("nearestResistance")) or 0.0),
                        "supportDistancePct": float(_safe_float(level.get("supportDistancePct")) or 0.0),
                        "resistanceDistancePct": float(_safe_float(level.get("resistanceDistancePct")) or 0.0),
                        "levelStrengthSupport": float(_safe_float(level.get("levelStrengthSupport")) or 0.0),
                        "levelStrengthResistance": float(_safe_float(level.get("levelStrengthResistance")) or 0.0),
                        "entrySource": entry_source,
                        "stopSource": stop_source,
                        "targetSource": target_source,
                        **indicators,
                        "indicatorAlignmentScore": float(indicator_alignment_score),
                        "indicatorNotes": indicator_notes,
                        "actionable": len(reasons) == 0,
                        "rejectionReason": _rejection_reason(reasons),
                        "selectionRank": 0,
                        "selectionReason": "pending_selection",
                        "conflictGroupKey": f"{market}:{symbol}:{str(forecast['horizon'])}:{rebalance_freq}",
                        "executionSymbol": execution_symbol,
                        "executionMode": execution_mode,
                        "validFrom": valid_from,
                        "validUntil": valid_until,
                        "validityMode": "bar_boundary",
                        "nextBarAt": next_bar_at.isoformat(),
                        "expiresAt": valid_until,
                        "modelVersion": str(context["modelVersion"]),
                        "asOfDate": str(forecast["asOfDate"]),
                        "signalFrequency": signal_frequency,
                        "sourceFrequency": str(forecast.get("sourceFrequency") or context.get("sourceFrequency") or signal_frequency),
                        "isDerivedSignal": bool(forecast.get("isDerivedSignal", context.get("isDerivedSignal", False))),
                        "forecastValidity": forecast_validity,
                        "forecastConflictReason": forecast_conflict_reason,
                        "forecastAdjusted": forecast_adjusted,
                        "priceBasis": price_basis,
                        "executionBasis": execution_basis,
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=TRADE_PLAN_COLUMNS)

    selected_rows: list[pd.DataFrame] = []
    for _, group in frame.groupby("conflictGroupKey", dropna=False, sort=False):
        ordered = group.sort_values(
            ["actionable", "tradeConfidence", "riskRewardRatio", "directionProbability", "rewardPct", "expectedReturn"],
            ascending=[False, False, False, False, False, False],
        ).reset_index(drop=True)
        ordered["selectionRank"] = ordered.index + 1
        ordered["selectionReason"] = ordered["selectionRank"].apply(lambda rank: _selection_reason(False, int(rank)))
        winner_actionable = bool(ordered.loc[0, "actionable"])
        if winner_actionable:
            ordered.loc[0, "selectionReason"] = "selected_active_trade"
        for idx in range(1, len(ordered)):
            ordered.loc[idx, "rejectionReason"] = _append_reason(ordered.loc[idx, "rejectionReason"], "lost_conflict_resolution")
            ordered.loc[idx, "actionable"] = False
            ordered.loc[idx, "selectionReason"] = "lost_conflict_resolution"
        if not winner_actionable:
            ordered.loc[0, "selectionReason"] = "inactive_candidate"
        selected_rows.append(ordered)

    final = pd.concat(selected_rows, ignore_index=True)
    final["status_order"] = final["actionable"].map({True: 0, False: 1})
    final = final.sort_values(
        ["market", "universe", "rebalanceFreq", "status_order", "tradeConfidence", "riskRewardRatio", "directionProbability", "expectedReturn"],
        ascending=[True, True, True, True, False, False, False, False],
    ).drop(columns=["status_order"]).reset_index(drop=True)
    return final.reindex(columns=TRADE_PLAN_COLUMNS)
