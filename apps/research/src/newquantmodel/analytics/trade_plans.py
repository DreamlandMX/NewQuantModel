from __future__ import annotations

from datetime import timedelta

import pandas as pd


MIN_CONFIDENCE = 0.55
MIN_RISK_REWARD = 1.5
DEFAULT_CONTEXTS = [
    ("long_only", "daily"),
    ("long_only", "weekly"),
    ("hedged", "daily"),
    ("hedged", "weekly"),
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
    "actionable",
    "rejectionReason",
    "executionSymbol",
    "executionMode",
    "expiresAt",
    "modelVersion",
    "asOfDate",
]


def _safe_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _latest_price_lookup(bars_1d: pd.DataFrame, bars_1h: pd.DataFrame) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], float]]:
    daily_lookup: dict[tuple[str, str], float] = {}
    hourly_lookup: dict[tuple[str, str], float] = {}
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
    return daily_lookup, hourly_lookup


def _entry_price(market: str, symbol: str, horizon: str, daily_lookup: dict[tuple[str, str], float], hourly_lookup: dict[tuple[str, str], float]) -> float | None:
    if market == "crypto" and horizon in {"1H", "4H"}:
        return hourly_lookup.get((market, symbol)) or daily_lookup.get((market, symbol))
    return daily_lookup.get((market, symbol)) or hourly_lookup.get((market, symbol))


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


def _expiry(as_of_date: object, rebalance_freq: str) -> str:
    _ = as_of_date
    base = pd.Timestamp.utcnow()
    delta = timedelta(days=1 if rebalance_freq == "daily" else 7)
    return (base + delta).isoformat()


def _rejection_reason(reasons: list[str]) -> str | None:
    filtered = [reason for reason in reasons if reason]
    if not filtered:
        return None
    return ", ".join(dict.fromkeys(filtered))


def _ranking_contexts(rankings: pd.DataFrame, symbol: str, universe: str, model_version: str) -> list[dict]:
    if not rankings.empty:
        subset = rankings[(rankings["symbol"] == symbol) & (rankings["universe"] == universe)]
        if not subset.empty:
            records: list[dict] = []
            for row in subset.to_dict(orient="records"):
                records.append(
                    {
                        "strategyMode": str(row["strategyMode"]),
                        "rebalanceFreq": str(row["rebalanceFreq"]),
                        "modelVersion": str(row.get("modelVersion") or model_version),
                    }
                )
            return records
    return [
        {
            "strategyMode": strategy_mode,
            "rebalanceFreq": rebalance_freq,
            "modelVersion": model_version,
        }
        for strategy_mode, rebalance_freq in DEFAULT_CONTEXTS
    ]


def build_trade_plan_panel(
    asset_master: pd.DataFrame,
    forecasts: pd.DataFrame,
    rankings: pd.DataFrame,
    bars_1d: pd.DataFrame,
    bars_1h: pd.DataFrame,
    universes: list[dict] | None = None,
) -> pd.DataFrame:
    if forecasts.empty or asset_master.empty:
        return pd.DataFrame(columns=TRADE_PLAN_COLUMNS)

    assets = {
        str(row["symbol"]): row
        for row in asset_master.to_dict(orient="records")
    }
    universe_lookup = _universe_lookup(universes)
    daily_lookup, hourly_lookup = _latest_price_lookup(bars_1d, bars_1h)

    rows: list[dict] = []
    for forecast in forecasts.to_dict(orient="records"):
        symbol = str(forecast["symbol"])
        asset = assets.get(symbol)
        if not asset:
            continue

        market = str(forecast["market"])
        universe = str(forecast["universe"])
        universe_meta = universe_lookup.get(universe, {})
        contexts = _ranking_contexts(rankings, symbol, universe, str(forecast.get("modelVersion") or "baseline-signals-v1"))

        entry_price = _entry_price(
            market,
            symbol,
            str(forecast["horizon"]),
            daily_lookup,
            hourly_lookup,
        )
        q10 = _safe_float(forecast.get("q10"))
        q90 = _safe_float(forecast.get("q90"))
        expected_return = _safe_float(forecast.get("expectedReturn")) or 0.0
        p_up = _safe_float(forecast.get("pUp")) or 0.0
        confidence = _safe_float(forecast.get("confidence")) or 0.0

        for context in contexts:
            strategy_mode = str(context["strategyMode"])
            rebalance_freq = str(context["rebalanceFreq"])
            candidate_sides = ["long"] if strategy_mode == "long_only" else ["long", "short"]

            for side in candidate_sides:
                if market == "cn_equity" and side == "short":
                    continue

                execution_symbol, execution_mode, execution_rejection = _execution_details(asset, market, side, universe_meta)
                reasons: list[str] = []
                if entry_price is None or entry_price <= 0:
                    reasons.append("missing_entry_reference_price")
                if confidence < MIN_CONFIDENCE:
                    reasons.append("confidence_below_55pct")

                risk_pct = 0.0
                reward_pct = 0.0
                stop_loss_price = float(entry_price or 0.0)
                take_profit_price = float(entry_price or 0.0)

                if q10 is None or q90 is None:
                    reasons.append("missing_quantile_forecast")
                else:
                    if side == "long":
                        if p_up < MIN_CONFIDENCE:
                            reasons.append("direction_not_bullish_enough")
                        if not (q90 > 0 and q10 < 0):
                            reasons.append("invalid_long_quantile_geometry")
                        risk_pct = abs(min(q10, 0.0))
                        reward_pct = max(q90, 0.0)
                        if entry_price:
                            stop_loss_price = float(entry_price * (1.0 + q10))
                            take_profit_price = float(entry_price * (1.0 + q90))
                    else:
                        if p_up > (1.0 - MIN_CONFIDENCE):
                            reasons.append("direction_not_bearish_enough")
                        if not (q10 < 0 and q90 > 0):
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
                        "confidence": float(confidence),
                        "actionable": len(reasons) == 0,
                        "rejectionReason": _rejection_reason(reasons),
                        "executionSymbol": execution_symbol,
                        "executionMode": execution_mode,
                        "expiresAt": _expiry(forecast.get("asOfDate"), rebalance_freq),
                        "modelVersion": str(context["modelVersion"]),
                        "asOfDate": str(forecast["asOfDate"]),
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=TRADE_PLAN_COLUMNS)
    frame = frame.sort_values(
        ["market", "universe", "strategyMode", "rebalanceFreq", "actionable", "riskRewardRatio", "confidence"],
        ascending=[True, True, True, True, False, False, False],
    ).reset_index(drop=True)
    return frame.reindex(columns=TRADE_PLAN_COLUMNS)
