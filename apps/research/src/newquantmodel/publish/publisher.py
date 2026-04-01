from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from newquantmodel.config.settings import AppPaths
from newquantmodel.data.sample_payloads import build_sample_snapshot
from newquantmodel.reporting.report_bundle import generate_report_bundle
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel_shared_types import JobRecord, JobStageRecord, ReportManifest, to_dict


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sample_context() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {
        "publishedAt": now.isoformat(),
        "dataSnapshotVersion": now.strftime("%Y%m%dT%H%M%SZ"),
    }


def publish_sample_snapshot(paths: AppPaths) -> ReportManifest:
    snapshot = build_sample_snapshot()
    context = _sample_context()
    universes = []
    for item in snapshot["universes"]:
        payload = to_dict(item)
        payload["stale"] = False
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        universes.append(payload)
    assets = []
    for item in snapshot["assets"]:
        payload = to_dict(item)
        payload["stale"] = False
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        assets.append(payload)
    forecasts = []
    for item in snapshot["forecasts"]:
        payload = to_dict(item)
        payload["stale"] = False
        payload["coverageMode"] = "point_in_time"
        payload["coveragePct"] = 100.0
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        forecasts.append(payload)
    rankings = []
    for item in snapshot["rankings"]:
        payload = to_dict(item)
        payload["stale"] = False
        payload["coverageMode"] = "point_in_time"
        payload["coveragePct"] = 100.0
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        rankings.append(payload)
    trade_plans = []
    for item in snapshot.get("trade_plans", []):
        payload = to_dict(item)
        payload["stale"] = False
        payload["coverageMode"] = "point_in_time"
        payload["coveragePct"] = 100.0
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        trade_plans.append(payload)
    backtests = []
    for item in snapshot["backtests"]:
        payload = to_dict(item)
        payload["stale"] = False
        payload["benchmark"] = payload.get("benchmark")
        payload["publishedAt"] = context["publishedAt"]
        payload["dataSnapshotVersion"] = context["dataSnapshotVersion"]
        backtests.append(payload)

    write_json(paths.published_dir / "universes.json", {"items": universes})
    write_json(paths.published_dir / "assets.json", {"items": assets})
    write_json(paths.published_dir / "forecasts.json", {"items": forecasts})
    write_json(paths.published_dir / "rankings.json", {"items": rankings})
    write_json(paths.published_dir / "trade-plans.json", {"items": trade_plans})
    write_json(paths.published_dir / "backtests.json", {"items": backtests})
    manifest = generate_report_bundle(paths.exports_dir, published_dir=paths.published_dir)
    write_json(paths.published_dir / "report-manifest.json", manifest)
    return manifest


def update_job(paths: AppPaths, record: JobRecord) -> None:
    payload = read_json(paths.published_dir / "jobs.json", {"items": []})
    items = [item for item in payload["items"] if item["id"] != record.id]
    items.insert(0, to_dict(record))
    write_json(paths.published_dir / "jobs.json", {"items": items[:50]})


def _upsert_stage(record: JobRecord, stage: JobStageRecord) -> JobRecord:
    stages = [item for item in record.stages if item.name != stage.name]
    stages.append(stage)
    stages.sort(key=lambda item: item.updatedAt)
    return replace(
        record,
        updatedAt=stage.updatedAt,
        message=stage.message,
        outputPath=stage.outputPath if stage.outputPath is not None else record.outputPath,
        currentStage=stage.name,
        stages=stages,
    )


def mark_job_running(paths: AppPaths, record: JobRecord, stage_name: str, message: str, output_path: str | None = None) -> JobRecord:
    updated = replace(record, status="running", updatedAt=_now_iso(), message=message, currentStage=stage_name)
    updated = _upsert_stage(updated, JobStageRecord(stage_name, "running", updated.updatedAt, message, output_path))
    update_job(paths, updated)
    return updated


def mark_stage_complete(paths: AppPaths, record: JobRecord, stage_name: str, message: str, output_path: str | None = None) -> JobRecord:
    updated = replace(record, updatedAt=_now_iso(), message=message, currentStage=stage_name, outputPath=output_path or record.outputPath)
    updated = _upsert_stage(updated, JobStageRecord(stage_name, "completed", updated.updatedAt, message, output_path))
    update_job(paths, updated)
    return updated


def mark_job_complete(paths: AppPaths, record: JobRecord, output_path: str | None = None, message: str | None = None) -> JobRecord:
    final_message = message or f"Completed {record.type} pipeline stage"
    updated = replace(record, status="completed", updatedAt=_now_iso(), message=final_message, outputPath=output_path, lastError=None)
    if record.currentStage:
        updated = _upsert_stage(updated, JobStageRecord(record.currentStage, "completed", updated.updatedAt, final_message, output_path))
    update_job(paths, updated)
    return updated


def mark_job_failed(paths: AppPaths, record: JobRecord, reason: str, stage_name: str | None = None) -> JobRecord:
    current_stage = stage_name or record.currentStage or "failed"
    updated = replace(
        record,
        status="failed",
        updatedAt=_now_iso(),
        message=reason,
        currentStage=current_stage,
        lastError=reason,
    )
    updated = _upsert_stage(updated, JobStageRecord(current_stage, "failed", updated.updatedAt, reason, record.outputPath))
    update_job(paths, updated)
    return updated
