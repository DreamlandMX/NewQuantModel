from __future__ import annotations

from datetime import datetime, timezone
import html
from pathlib import Path
from typing import Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, StyleSheet1, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _text(value: object, *, fallback: str = "N/A") -> str:
    if value is None:
        return fallback
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return " | ".join(parts) if parts else fallback
    text = str(value).strip()
    return text or fallback


def _bool_text(value: object) -> str:
    return "Yes" if bool(value) else "No"


def _percent(value: object, digits: int = 2) -> str:
    return f"{_to_float(value):.{digits}%}"


def _number(value: object, digits: int = 2) -> str:
    return f"{_to_float(value):,.{digits}f}"


def _price(value: object) -> str:
    numeric = _to_float(value)
    if abs(numeric) >= 1000:
        return f"{numeric:,.2f}"
    if abs(numeric) >= 1:
        return f"{numeric:,.4f}"
    return f"{numeric:,.6f}"


def _ratio_percent(value: object) -> str:
    numeric = _to_float(value)
    if numeric > 1:
        numeric = numeric / 100
    return _percent(numeric)


def _human_time(value: object) -> str:
    raw = _text(value)
    if raw == "N/A":
        return raw
    normalized = raw.replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(normalized)
    except ValueError:
        return raw
    local_moment = moment.astimezone()
    utc_moment = moment.astimezone(timezone.utc)
    return f"{local_moment.strftime('%Y-%m-%d %H:%M %Z')} | UTC {utc_moment.strftime('%Y-%m-%d %H:%M')}"


def _paragraph(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(html.escape(text), style)


def _section_title(title: str, styles: StyleSheet1) -> list:
    return [_paragraph(title, styles["SectionTitle"]), Spacer(1, 0.08 * inch)]


def _empty_section(styles: StyleSheet1) -> list:
    return [_paragraph("No data available for this section.", styles["BodyMuted"]), Spacer(1, 0.14 * inch)]


def _bullet_lines(lines: Iterable[str], styles: StyleSheet1) -> list:
    story: list = []
    for line in lines:
        story.append(_paragraph(f"- {line}", styles["Body"]))
    story.append(Spacer(1, 0.12 * inch))
    return story


def _table(rows: list[list[str]], headers: list[str], styles: StyleSheet1) -> Table:
    table_rows = [[_paragraph(header, styles["TableHeader"]) for header in headers]]
    for row in rows:
        table_rows.append([_paragraph(_text(value), styles["TableCell"]) for value in row])
    table = Table(table_rows, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dce8f5")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d0d7de")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _page_footer(canvas, doc) -> None:  # pragma: no cover - exercised via PDF rendering
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#d0d7de"))
    canvas.line(doc.leftMargin, 0.65 * inch, doc.pagesize[0] - doc.rightMargin, 0.65 * inch)
    canvas.setFillColor(colors.HexColor("#52606d"))
    canvas.setFont("Helvetica", 8)
    canvas.drawRightString(doc.pagesize[0] - doc.rightMargin, 0.45 * inch, f"Page {canvas.getPageNumber()}")
    canvas.restoreState()


def _build_styles() -> StyleSheet1:
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="ReportTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=27,
            textColor=colors.HexColor("#0f172a"),
            alignment=TA_LEFT,
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            name="ReportSubtitle",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#52606d"),
            spaceAfter=10,
        )
    )
    styles.add(
        ParagraphStyle(
            name="SectionTitle",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=17,
            textColor=colors.HexColor("#133c55"),
            spaceBefore=10,
            spaceAfter=3,
        )
    )
    styles.add(
        ParagraphStyle(
            name="Body",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#243447"),
            spaceAfter=4,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodyMuted",
            parent=styles["Body"],
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#627d98"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="TableHeader",
            parent=styles["Body"],
            fontName="Helvetica-Bold",
            fontSize=8.3,
            leading=10,
            textColor=colors.HexColor("#0f172a"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="TableCell",
            parent=styles["Body"],
            fontSize=8.2,
            leading=10,
            textColor=colors.HexColor("#1f2937"),
        )
    )
    return styles


def write_research_pdf(
    path: Path,
    *,
    title: str,
    generated_at: str,
    forecasts: list[dict],
    rankings: list[dict],
    trade_plans: list[dict],
    backtests: list[dict],
    health: list[dict],
    universes: list[dict],
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    styles = _build_styles()

    top_trades = sorted(
        [item for item in trade_plans if bool(item.get("actionable"))],
        key=lambda item: (
            _to_float(item.get("riskRewardRatio")),
            _to_float(item.get("confidence")),
            _to_float(item.get("rewardPct")),
        ),
        reverse=True,
    )[:10]
    top_signals = sorted(rankings, key=lambda item: _to_float(item.get("score")), reverse=True)[:10]
    top_backtests = sorted(backtests, key=lambda item: _to_float(item.get("sharpe")), reverse=True)[:8]
    model_versions = sorted(
        {
            _text(item.get("modelVersion"))
            for item in forecasts + rankings + trade_plans + backtests
            if item.get("modelVersion")
        }
    )
    best_signal = top_signals[0] if top_signals else None
    best_strategy = top_backtests[0] if top_backtests else None
    best_trade = top_trades[0] if top_trades else None

    story: list = [
        _paragraph(title, styles["ReportTitle"]),
        _paragraph(
            f"Generated { _human_time(generated_at) } | Investment research format | Batch-published artifacts",
            styles["ReportSubtitle"],
        ),
        HRFlowable(width="100%", thickness=0.6, color=colors.HexColor("#d0d7de")),
        Spacer(1, 0.12 * inch),
    ]

    story.extend(_section_title("Executive Summary", styles))
    summary_lines = [
        f"Published {len(forecasts)} forecasts, {len(rankings)} rankings, {len(trade_plans)} trade plans, {len(backtests)} backtests, and {len(health)} data-health records.",
        f"Model versions in production: {', '.join(model_versions) if model_versions else 'No model versions published.'}",
        (
            f"Highest-conviction trade: {_text(best_trade.get('symbol'))} {_text(best_trade.get('side')).upper()} "
            f"with {_number(best_trade.get('riskRewardRatio'))}x risk-reward and {_percent(best_trade.get('confidence'), 0)} confidence."
            if best_trade
            else "No actionable trade passed the current publication thresholds."
        ),
        (
            f"Top signal: {_text(best_signal.get('symbol'))} in {_text(best_signal.get('universe'))} "
            f"with score {_number(best_signal.get('score'), 3)} and target weight {_percent(best_signal.get('targetWeight'))}."
            if best_signal
            else "No ranking signal was available for this report window."
        ),
        (
            f"Best strategy snapshot: {_text(best_strategy.get('strategyId'))} "
            f"with Sharpe {_number(best_strategy.get('sharpe'))}, CAGR {_percent(best_strategy.get('cagr'))}, "
            f"and Max Drawdown {_percent(best_strategy.get('maxDrawdown'))}."
            if best_strategy
            else "No strategy snapshot was available for this report window."
        ),
    ]
    story.extend(_bullet_lines(summary_lines, styles))

    story.extend(_section_title("Market Highlights", styles))
    if health:
        market_lines = [
            (
                f"{_text(item.get('market'))}: coverage {_ratio_percent(item.get('coveragePct'))}, "
                f"tradable {_ratio_percent(item.get('tradableCoveragePct'))}, "
                f"stale {_bool_text(item.get('stale'))}, history start {_text(item.get('historyStartDate'))}."
            )
            for item in health
        ]
        story.extend(_bullet_lines(market_lines, styles))
    else:
        story.extend(_empty_section(styles))

    story.extend(_section_title("Top Actionable Trades", styles))
    if top_trades:
        story.append(
            _table(
                rows=[
                    [
                        _text(item.get("symbol")),
                        _text(item.get("side")).upper(),
                        _price(item.get("entryPrice")),
                        _price(item.get("stopLossPrice")),
                        _price(item.get("takeProfitPrice")),
                        f"{_number(item.get('riskRewardRatio'))}x",
                        _percent(item.get("confidence"), 0),
                        _human_time(item.get("expiresAt")),
                    ]
                    for item in top_trades
                ],
                headers=["Symbol", "Side", "Entry", "SL", "TP", "RR", "Confidence", "Expiry"],
                styles=styles,
            )
        )
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.extend(_empty_section(styles))

    story.extend(_section_title("Top Signals", styles))
    if top_signals:
        story.append(
            _table(
                rows=[
                    [
                        _text(item.get("symbol")),
                        _text(item.get("universe")),
                        _text(item.get("strategyMode")),
                        _text(item.get("rebalanceFreq")),
                        _number(item.get("score"), 3),
                        _percent(item.get("targetWeight")),
                        _text(item.get("modelVersion")),
                    ]
                    for item in top_signals
                ],
                headers=["Symbol", "Universe", "Mode", "Rebalance", "Score", "Target Wt", "Model"],
                styles=styles,
            )
        )
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.extend(_empty_section(styles))

    story.extend(_section_title("Strategy Snapshot", styles))
    if top_backtests:
        story.append(
            _table(
                rows=[
                    [
                        _text(item.get("strategyId")),
                        _number(item.get("sharpe")),
                        _percent(item.get("cagr")),
                        _percent(item.get("maxDrawdown")),
                        _text(item.get("modelVersion")),
                    ]
                    for item in top_backtests
                ],
                headers=["Strategy", "Sharpe", "CAGR", "Max DD", "Model"],
                styles=styles,
            )
        )
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.extend(_empty_section(styles))

    story.extend(_section_title("Data Health", styles))
    if health:
        story.append(
            _table(
                rows=[
                    [
                        _text(item.get("market")),
                        _ratio_percent(item.get("coveragePct")),
                        _ratio_percent(item.get("tradableCoveragePct")),
                        _bool_text(item.get("stale")),
                        _text(item.get("historyStartDate")),
                        _text(item.get("notes")),
                    ]
                    for item in health
                ],
                headers=["Market", "Coverage", "Tradable", "Stale", "History Start", "Notes"],
                styles=styles,
            )
        )
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.extend(_empty_section(styles))

    story.extend(_section_title("Universe Coverage", styles))
    if universes:
        story.append(
            _table(
                rows=[
                    [
                        _text(item.get("universe")),
                        _text(item.get("market")),
                        _text(item.get("coverageMode")),
                        _ratio_percent(item.get("coveragePct")),
                        _number(item.get("memberCount"), 0),
                        _text(item.get("refreshSchedule")),
                    ]
                    for item in universes
                ],
                headers=["Universe", "Market", "Coverage Mode", "Coverage", "Members", "Refresh"],
                styles=styles,
            )
        )
        story.append(Spacer(1, 0.14 * inch))
    else:
        story.extend(_empty_section(styles))

    doc = SimpleDocTemplate(
        str(path),
        pagesize=LETTER,
        leftMargin=0.72 * inch,
        rightMargin=0.72 * inch,
        topMargin=0.72 * inch,
        bottomMargin=0.85 * inch,
        title=title,
        author="newquantmodel",
    )
    doc.build(story, onFirstPage=_page_footer, onLaterPages=_page_footer)
    return path
