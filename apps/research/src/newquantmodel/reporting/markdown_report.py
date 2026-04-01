from __future__ import annotations

from pathlib import Path

def render_markdown_report(
    path: Path,
    *,
    forecasts: list[dict] | None = None,
    rankings: list[dict] | None = None,
    trade_plans: list[dict] | None = None,
    health: list[dict] | None = None,
    universes: list[dict] | None = None,
    backtests: list[dict] | None = None,
) -> Path:
    forecasts = forecasts or []
    rankings = rankings or []
    trade_plans = trade_plans or []
    health = health or []
    universes = universes or []
    backtests = backtests or []
    top_rankings = sorted(rankings, key=lambda item: float(item.get("score", 0.0)), reverse=True)[:8]
    top_trades = sorted(
        [item for item in trade_plans if bool(item.get("actionable"))],
        key=lambda item: (
            float(item.get("riskRewardRatio", 0.0)),
            float(item.get("confidence", 0.0)),
            float(item.get("rewardPct", 0.0)),
        ),
        reverse=True,
    )[:8]
    top_backtests = sorted(backtests, key=lambda item: float(item.get("sharpe", 0.0)), reverse=True)[:6]
    model_versions = sorted({str(item.get("modelVersion")) for item in forecasts + rankings + trade_plans + backtests if item.get("modelVersion")})
    lines = [
        "# newquantmodel Weekly Research Report",
        "",
        f"- Published date: `{Path(path).parent.name}`",
        "- Style: investment research",
        "- Scope: crypto, China A-shares, US equities, and major indices",
        "",
        "## Market Highlights",
        "",
        f"- Forecast rows published: `{len(forecasts)}`",
        f"- Ranking rows published: `{len(rankings)}`",
        f"- Trade plan rows published: `{len(trade_plans)}`",
        f"- Data health rows published: `{len(health)}`",
        f"- Universe rows published: `{len(universes)}`",
        f"- Model versions: `{', '.join(model_versions) if model_versions else 'baseline-signals-v1'}`",
        "",
        "## Top Forecasts",
        "",
    ]
    for item in forecasts[:5]:
        lines.append(
                f"- `{item['symbol']}` `{item['horizon']}`: expected return `{item['expectedReturn']:.2%}`, median `{item['q50']:.2%}`, confidence `{item['confidence']:.0%}`"
        )
    lines.extend(
        [
            "",
            "## Top Actionable Trades",
            "",
        ]
    )
    for item in top_trades:
        lines.append(
            f"- `{item['symbol']}` `{item['side']}` `{item['horizon']}`: entry `{item['entryPrice']:.4f}`, SL `{item['stopLossPrice']:.4f}`, TP `{item['takeProfitPrice']:.4f}`, RR `{item['riskRewardRatio']:.2f}x`, confidence `{item['confidence']:.0%}`"
        )
    lines.extend(
        [
            "",
            "## Top Signals",
            "",
        ]
    )
    for item in top_rankings:
        lines.append(
            f"- `{item['symbol']}` in `{item['universe']}`: score `{item['score']:.3f}`, target weight `{item['targetWeight']:.2%}`, model `{item['modelVersion']}`"
        )
    lines.extend(
        [
            "",
            "## Portfolio Notes",
            "",
            "- Long-only output should be interpreted as baseline ranking plus medium-risk target weights.",
            "- Hedged output uses market proxies instead of direct shorting for China and US equity research.",
            "",
            "## Strategy Snapshot",
            "",
        ]
    )
    for item in top_backtests:
        lines.append(
            f"- `{item['strategyId']}`: Sharpe `{item['sharpe']:.2f}`, CAGR `{item['cagr']:.2%}`, Max DD `{item['maxDrawdown']:.2%}`, model `{item['modelVersion']}`"
        )
    lines.extend(
        [
            "",
            "## Validation Snapshot",
            "",
            "- Stock research tracks IC, rank IC, top-decile spread, turnover, and cost-adjusted Sharpe.",
            "- Crypto and index research publish calibrated direction probabilities and interval forecasts with baseline fallback preserved.",
        ]
    )
    if health:
        lines.extend(["", "## Data Health", ""])
        for item in health[:5]:
            lines.append(
                f"- `{item['market']}` coverage `{item['coveragePct']:.2f}%`, tradable `{item['tradableCoveragePct']:.2f}%`, stale `{item['stale']}`, history start `{item['historyStartDate']}`"
            )
    if universes:
        lines.extend(["", "## Universe Coverage", ""])
        for item in universes[:8]:
            lines.append(
                f"- `{item['universe']}` `{item['coverageMode']}` coverage `{item['coveragePct']:.2f}%`, members `{item['memberCount']}`, refresh `{item['refreshSchedule']}`"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
