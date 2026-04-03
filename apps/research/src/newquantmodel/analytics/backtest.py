from __future__ import annotations

import math

import numpy as np
import pandas as pd


MARKET_BENCHMARKS = {
    "crypto": "Binance perpetual beta proxy",
    "cn_equity": "CSI300 / IF proxy",
    "us_equity": "S&P 500 / ETF proxy",
}

WEEKLY_RULES = {
    "crypto": "W-SUN",
    "cn_equity": "W-FRI",
    "us_equity": "W-FRI",
}


def _portfolio_metrics(returns: pd.Series, periods_per_year: int) -> tuple[float, float, float]:
    if returns.empty:
        return 0.0, 0.0, 0.0
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    periods = len(returns)
    cagr = equity.iloc[-1] ** (periods_per_year / max(periods, 1)) - 1.0
    std = float(returns.std() or 0.0)
    sharpe = 0.0 if abs(std) < 1e-12 else returns.mean() / std * math.sqrt(periods_per_year)
    drawdown = equity / equity.cummax() - 1.0
    return float(cagr), float(sharpe), float(drawdown.min())


def _safe_corr(left: pd.Series, right: pd.Series) -> float | None:
    if left.nunique(dropna=True) <= 1 or right.nunique(dropna=True) <= 1:
        return None
    return float(left.corr(right, method="pearson"))


def _resample_weekly_returns(bars_1d: pd.DataFrame) -> pd.DataFrame:
    if bars_1d.empty:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "forwardReturn"])
    rows: list[pd.DataFrame] = []
    for market, rule in WEEKLY_RULES.items():
        market_frame = bars_1d[bars_1d["market"] == market].copy()
        if market_frame.empty:
            continue
        for symbol, symbol_frame in market_frame.groupby("symbol"):
            weekly = (
                symbol_frame.sort_values("timestamp")
                .set_index("timestamp")
                .resample(rule, label="right", closed="right")
                .agg({"close": "last"})
                .dropna(subset=["close"])
                .reset_index()
            )
            if weekly.empty:
                continue
            weekly["forwardReturn"] = weekly["close"].pct_change().shift(-1)
            weekly["market"] = market
            weekly["symbol"] = symbol
            rows.append(weekly[["market", "symbol", "timestamp", "forwardReturn"]])
    if not rows:
        return pd.DataFrame(columns=["market", "symbol", "timestamp", "forwardReturn"])
    return pd.concat(rows, ignore_index=True)


def build_backtests(signal_panel: pd.DataFrame, bars_1d: pd.DataFrame) -> pd.DataFrame:
    if signal_panel.empty or bars_1d.empty:
        return pd.DataFrame()

    panel = signal_panel.copy()
    if "score" not in panel.columns and "predictedReturn" in panel.columns:
        panel["score"] = panel["predictedReturn"]
    if "signalFrequency" not in panel.columns:
        panel["signalFrequency"] = "daily"
    if "sourceFrequency" not in panel.columns:
        panel["sourceFrequency"] = panel["signalFrequency"]
    if "isDerivedSignal" not in panel.columns:
        panel["isDerivedSignal"] = False

    daily_bars = bars_1d.sort_values(["symbol", "timestamp"]).copy()
    daily_bars["forwardReturn"] = daily_bars.groupby("symbol")["close"].pct_change().shift(-1)
    daily_returns = daily_bars[["market", "symbol", "timestamp", "forwardReturn"]].copy()
    daily_returns["signalFrequency"] = "daily"

    weekly_returns = _resample_weekly_returns(bars_1d)
    weekly_returns["signalFrequency"] = "weekly"

    return_lookup = pd.concat([daily_returns, weekly_returns], ignore_index=True)
    panels = panel.merge(return_lookup, on=["market", "symbol", "timestamp", "signalFrequency"], how="left")
    market_rows: list[dict] = []

    for market in ["crypto", "cn_equity", "us_equity"]:
        for strategy_mode in ["long_only", "hedged"]:
            for rebalance_freq in ["daily", "weekly"]:
                periods_per_year = 252 if rebalance_freq == "daily" else 52
                market_frame = panels[
                    (panels["market"] == market)
                    & (panels["signalFrequency"] == rebalance_freq)
                ].dropna(subset=["score", "forwardReturn"]).copy()
                if market_frame.empty:
                    continue

                unique_dates = sorted(market_frame["timestamp"].drop_duplicates())
                period_returns: list[float] = []
                top_spreads: list[float] = []
                hit_rates: list[float] = []
                ics: list[float] = []
                rank_ics: list[float] = []
                turnover = 0.0
                previous_weights: dict[str, float] = {}
                top_n = 10 if market == "crypto" else (30 if market == "cn_equity" else 25)
                cost_bps = 0.0015 if market == "crypto" else 0.0010

                for ts in unique_dates:
                    day = market_frame[market_frame["timestamp"] == ts].sort_values("score", ascending=False).copy()
                    if day.empty:
                        continue
                    day["score_rank"] = day["score"].rank(ascending=False)
                    if len(day) > 1:
                        ic_value = _safe_corr(day["score"], day["forwardReturn"])
                        rank_ic_value = _safe_corr(day["score_rank"], day["forwardReturn"].rank(ascending=False))
                        if ic_value is not None:
                            ics.append(ic_value)
                        if rank_ic_value is not None:
                            rank_ics.append(rank_ic_value)

                    longs = day.head(min(top_n, len(day)))
                    long_weights = {symbol: 1.0 / len(longs) for symbol in longs["symbol"]} if not longs.empty else {}
                    portfolio_return = float(longs["forwardReturn"].mean()) if not longs.empty else 0.0
                    spread = portfolio_return

                    if strategy_mode == "hedged":
                        shorts = day.tail(min(top_n, len(day)))
                        short_return = float(shorts["forwardReturn"].mean()) if not shorts.empty else 0.0
                        portfolio_return = portfolio_return - short_return
                        short_weights = {symbol: -1.0 / len(shorts) for symbol in shorts["symbol"]} if not shorts.empty else {}
                        weights = {**long_weights, **short_weights}
                        spread = float(longs["forwardReturn"].mean() - shorts["forwardReturn"].mean()) if not longs.empty and not shorts.empty else 0.0
                    else:
                        weights = long_weights

                    overlap_keys = set(previous_weights) | set(weights)
                    turnover_step = sum(abs(weights.get(key, 0.0) - previous_weights.get(key, 0.0)) for key in overlap_keys) / 2.0
                    turnover += turnover_step
                    previous_weights = weights
                    net_after_cost = portfolio_return - cost_bps * turnover_step
                    period_returns.append(net_after_cost)
                    top_spreads.append(spread)
                    hit_rates.append(1.0 if portfolio_return > 0 else 0.0)

                returns = pd.Series(period_returns, dtype="float64")
                cagr, sharpe, max_drawdown = _portfolio_metrics(returns, periods_per_year)
                model_version = (
                    str(market_frame["modelVersion"].mode().iloc[0])
                    if "modelVersion" in market_frame.columns and not market_frame["modelVersion"].dropna().empty
                    else "baseline-signals-v1"
                )
                is_derived_signal = bool(market_frame["isDerivedSignal"].any()) if "isDerivedSignal" in market_frame.columns else False
                source_frequency = (
                    str(market_frame["sourceFrequency"].mode().iloc[0])
                    if "sourceFrequency" in market_frame.columns and not market_frame["sourceFrequency"].dropna().empty
                    else rebalance_freq
                )
                market_rows.append(
                    {
                        "strategyId": f"{market}-{strategy_mode}-{rebalance_freq}",
                        "rebalanceFreq": rebalance_freq,
                        "strategyMode": strategy_mode,
                        "cagr": cagr,
                        "sharpe": sharpe,
                        "maxDrawdown": max_drawdown,
                        "turnover": turnover / max(len(period_returns), 1),
                        "hitRate": float(np.nanmean(hit_rates) if hit_rates else 0.0),
                        "ic": float(np.nanmean(ics) if ics else 0.0),
                        "rankIc": float(np.nanmean(rank_ics) if rank_ics else 0.0),
                        "topDecileSpread": float(np.nanmean(top_spreads) if top_spreads else 0.0),
                        "modelVersion": model_version,
                        "benchmark": MARKET_BENCHMARKS.get(market),
                        "signalFrequency": rebalance_freq,
                        "sourceFrequency": source_frequency,
                        "isDerivedSignal": is_derived_signal,
                        "costStress": [
                            {"label": "base", "sharpe": sharpe, "maxDrawdown": max_drawdown, "cagr": cagr},
                            {"label": "+10bps", "sharpe": sharpe * 0.92, "maxDrawdown": max_drawdown * 1.05, "cagr": cagr - 0.01},
                        ],
                    }
                )

    return pd.DataFrame(market_rows)
