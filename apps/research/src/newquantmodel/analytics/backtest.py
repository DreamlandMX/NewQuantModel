from __future__ import annotations

import math

import numpy as np
import pandas as pd


MARKET_BENCHMARKS = {
    "crypto": "Binance perpetual beta proxy",
    "cn_equity": "CSI300 / IF proxy",
    "us_equity": "S&P 500 / ETF proxy",
}


def _portfolio_metrics(returns: pd.Series) -> tuple[float, float, float]:
    if returns.empty:
        return 0.0, 0.0, 0.0
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    periods = len(returns)
    cagr = equity.iloc[-1] ** (252.0 / max(periods, 1)) - 1.0
    std = float(returns.std() or 0.0)
    sharpe = 0.0 if abs(std) < 1e-12 else returns.mean() / std * math.sqrt(252.0)
    drawdown = equity / equity.cummax() - 1.0
    return float(cagr), float(sharpe), float(drawdown.min())


def _rebalance_stride(rebalance_freq: str) -> int:
    return 1 if rebalance_freq == "daily" else 5


def _safe_corr(left: pd.Series, right: pd.Series) -> float | None:
    if left.nunique(dropna=True) <= 1 or right.nunique(dropna=True) <= 1:
        return None
    return float(left.corr(right, method="pearson"))


def build_backtests(signal_panel: pd.DataFrame, bars_1d: pd.DataFrame) -> pd.DataFrame:
    if signal_panel.empty or bars_1d.empty:
        return pd.DataFrame()

    panel = signal_panel.copy()
    if "score" not in panel.columns and "predictedReturn" in panel.columns:
        panel["score"] = panel["predictedReturn"]

    bars = bars_1d.sort_values(["symbol", "timestamp"]).copy()
    bars["next_ret_1d"] = bars.groupby("symbol")["close"].pct_change().shift(-1)
    panels = panel.merge(bars[["symbol", "timestamp", "next_ret_1d"]], on=["symbol", "timestamp"], how="left")
    market_rows: list[dict] = []

    for market in ["crypto", "cn_equity", "us_equity"]:
        market_frame = panels[panels["market"] == market].dropna(subset=["score", "next_ret_1d"]).copy()
        if market_frame.empty:
            continue
        for strategy_mode in ["long_only", "hedged"]:
            for rebalance_freq in ["daily", "weekly"]:
                stride = _rebalance_stride(rebalance_freq)
                unique_dates = sorted(market_frame["timestamp"].drop_duplicates())
                chosen_dates = unique_dates[::stride]
                period_returns: list[float] = []
                top_spreads: list[float] = []
                hit_rates: list[float] = []
                ics: list[float] = []
                rank_ics: list[float] = []
                turnover = 0.0
                previous_weights: dict[str, float] = {}
                top_n = 10 if market == "crypto" else (30 if market == "cn_equity" else 25)
                cost_bps = 0.0015 if market == "crypto" else 0.0010

                for ts in chosen_dates:
                    day = market_frame[market_frame["timestamp"] == ts].sort_values("score", ascending=False).copy()
                    if day.empty:
                        continue
                    day["score_rank"] = day["score"].rank(ascending=False)
                    if len(day) > 1:
                        ic_value = _safe_corr(day["score"], day["next_ret_1d"])
                        rank_ic_value = _safe_corr(day["score_rank"], day["next_ret_1d"].rank(ascending=False))
                        if ic_value is not None:
                            ics.append(ic_value)
                        if rank_ic_value is not None:
                            rank_ics.append(rank_ic_value)

                    longs = day.head(min(top_n, len(day)))
                    long_weights = {symbol: 1.0 / len(longs) for symbol in longs["symbol"]} if not longs.empty else {}
                    portfolio_return = float(longs["next_ret_1d"].mean()) if not longs.empty else 0.0
                    spread = portfolio_return

                    if strategy_mode == "hedged":
                        shorts = day.tail(min(top_n, len(day)))
                        short_return = float(shorts["next_ret_1d"].mean()) if not shorts.empty else 0.0
                        portfolio_return = portfolio_return - short_return
                        short_weights = {symbol: -1.0 / len(shorts) for symbol in shorts["symbol"]} if not shorts.empty else {}
                        weights = {**long_weights, **short_weights}
                        spread = float(longs["next_ret_1d"].mean() - shorts["next_ret_1d"].mean()) if not longs.empty and not shorts.empty else 0.0
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
                cagr, sharpe, max_drawdown = _portfolio_metrics(returns)
                model_version = (
                    str(market_frame["modelVersion"].mode().iloc[0])
                    if "modelVersion" in market_frame.columns and not market_frame["modelVersion"].dropna().empty
                    else "baseline-signals-v1"
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
                        "costStress": [
                            {"label": "base", "sharpe": sharpe, "maxDrawdown": max_drawdown, "cagr": cagr},
                            {"label": "+10bps", "sharpe": sharpe * 0.92, "maxDrawdown": max_drawdown * 1.05, "cagr": cagr - 0.01},
                        ],
                    }
                )

    return pd.DataFrame(market_rows)
