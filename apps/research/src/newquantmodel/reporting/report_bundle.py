from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from newquantmodel_shared_types import ReportManifest

from .csv_export import write_csv
from .markdown_report import render_markdown_report
from .pdf_export import write_research_pdf


def _load_items(path: Path) -> list[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("items", []) if isinstance(payload, dict) else []


def generate_report_bundle(exports_dir: Path, published_dir: Path | None = None) -> ReportManifest:
    generated_at = datetime.now(timezone.utc)
    slug = generated_at.strftime("%Y%m%dT%H%M%SZ")
    bundle_dir = exports_dir / slug
    bundle_dir.mkdir(parents=True, exist_ok=True)

    if published_dir:
        universes = _load_items(published_dir / "universes.json")
        forecasts = _load_items(published_dir / "forecasts.json")
        rankings = _load_items(published_dir / "rankings.json")
        trade_plans = _load_items(published_dir / "trade-plans.json")
        backtests = _load_items(published_dir / "backtests.json")
        health = _load_items(published_dir / "data-health.json")
    else:
        universes = []
        forecasts = []
        rankings = []
        trade_plans = []
        backtests = []
        health = []

    markdown_path = render_markdown_report(
        bundle_dir / "weekly_report.md",
        forecasts=forecasts,
        rankings=rankings,
        trade_plans=trade_plans,
        health=health,
        universes=universes,
        backtests=backtests,
    )
    forecast_csv = write_csv(bundle_dir / "forecasts.csv", [SimpleNamespaceLike(item) for item in forecasts])
    ranking_csv = write_csv(bundle_dir / "rankings.csv", [SimpleNamespaceLike(item) for item in rankings])
    trade_plan_csv = write_csv(bundle_dir / "trade_plans.csv", [SimpleNamespaceLike(item) for item in trade_plans])
    pdf_path = write_research_pdf(
        bundle_dir / "weekly_report.pdf",
        title="newquantmodel Weekly Research Report",
        generated_at=generated_at.isoformat(),
        forecasts=forecasts,
        rankings=rankings,
        trade_plans=trade_plans,
        backtests=backtests,
        health=health,
        universes=universes,
    )

    return ReportManifest(
        markdownPath=str(markdown_path),
        csvPaths=[str(forecast_csv), str(ranking_csv), str(trade_plan_csv)],
        pdfPath=str(pdf_path),
        generatedAt=generated_at.isoformat(),
    )


class SimpleNamespaceLike:
    def __init__(self, payload: dict):
        self.__dict__.update(payload)
