from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from newquantmodel.config.settings import AppPaths
from newquantmodel.publish.publisher import (
    mark_job_complete,
    mark_job_failed,
    mark_job_running,
    mark_stage_complete,
    publish_sample_snapshot,
    update_job,
)
from newquantmodel.publish.real_pipeline import (
    backtest_models,
    build_baseline_signals,
    build_ml_signals,
    ingest_market,
    publish_real,
    refresh_market,
    refresh_real,
)
from newquantmodel.publish.scheduler import run_scheduler
from newquantmodel.storage.json_store import read_json
from newquantmodel_shared_types import JobRecord, JobStageRecord


def handle_publish_sample(root: str) -> int:
    paths = AppPaths.from_root(root)
    publish_sample_snapshot(paths)
    print(f"Published sample snapshot to {paths.published_dir}")
    return 0


def handle_smoke(root: str) -> int:
    paths = AppPaths.from_root(root)
    publish_sample_snapshot(paths)
    universes = read_json(paths.published_dir / "universes.json", {"items": []})
    forecasts = read_json(paths.published_dir / "forecasts.json", {"items": []})
    rankings = read_json(paths.published_dir / "rankings.json", {"items": []})
    trade_plans = read_json(paths.published_dir / "trade-plans.json", {"items": []})
    report = read_json(paths.published_dir / "report-manifest.json", {})
    print(
        "Smoke OK:",
        f"universes={len(universes['items'])}",
        f"forecasts={len(forecasts['items'])}",
        f"rankings={len(rankings['items'])}",
        f"trade_plans={len(trade_plans['items'])}",
        f"report={report.get('pdfPath', 'missing')}",
    )
    return 0


def handle_ingest(root: str, market: str, years: int, limit: int | None) -> int:
    paths = AppPaths.from_root(root)
    ingest_market(paths, market=market, years=years, limit=limit)
    print(f"Ingested market={market} years={years} limit={limit}")
    return 0


def handle_build_baseline(root: str) -> int:
    paths = AppPaths.from_root(root)
    build_baseline_signals(paths)
    print("Built baseline signals")
    return 0


def handle_build_ml(root: str) -> int:
    paths = AppPaths.from_root(root)
    build_ml_signals(paths)
    print("Built ML overlay signals")
    return 0


def handle_backtest(root: str) -> int:
    paths = AppPaths.from_root(root)
    backtest_models(paths)
    print("Built portfolio backtests")
    return 0


def handle_publish_real(root: str) -> int:
    paths = AppPaths.from_root(root)
    publish_real(paths)
    print(f"Published real snapshot to {paths.published_dir}")
    return 0


def handle_refresh_real(root: str, years: int, limit: int | None) -> int:
    paths = AppPaths.from_root(root)
    refresh_real(paths, years=years, limit=limit)
    print(f"Refreshed real snapshot years={years} limit={limit}")
    return 0


def handle_refresh_market(root: str, market: str, years: int, limit: int | None) -> int:
    paths = AppPaths.from_root(root)
    refresh_market(paths, market=market, years=years, limit=limit)
    print(f"Refreshed market snapshot market={market} years={years} limit={limit}")
    return 0


def _queued_job(job_id: str, job_type: str) -> JobRecord:
    requested_at = datetime.now(timezone.utc).isoformat()
    return JobRecord(
        id=job_id,
        type=job_type,
        status="queued",
        requestedAt=requested_at,
        updatedAt=requested_at,
        message=f"Queued {job_type} pipeline stage",
        outputPath=None,
        currentStage="queued",
        stages=[JobStageRecord("queued", "queued", requested_at, f"Queued {job_type} pipeline stage", None)],
        lastError=None,
    )


def handle_run_job(root: str, job_id: str, job_type: str) -> int:
    paths = AppPaths.from_root(root)
    record = _queued_job(job_id, job_type)
    update_job(paths, record)

    def stage(stage_name: str, message: str, callback):
        nonlocal record
        record = mark_job_running(paths, record, stage_name, message)
        output_path = callback()
        record = mark_stage_complete(paths, record, stage_name, f"Completed {stage_name}", output_path)
        return output_path

    try:
        output_path: str | None = None
        if job_type == "ingest":
            output_path = stage("ingest-crypto", "Refreshing crypto universe and prices", lambda: _ingest_output(paths, "crypto"))
            output_path = stage("ingest-cn-equity", "Refreshing China A-share universe and prices", lambda: _ingest_output(paths, "cn_equity"))
            output_path = stage("ingest-us-equity", "Refreshing US equity universe and prices", lambda: _ingest_output(paths, "us_equity"))
        elif job_type == "feature":
            output_path = stage("baseline-features", "Building baseline features and signals", lambda: _feature_output(paths))
        elif job_type == "train":
            output_path = stage("ml-overlay", "Training ML overlay models", lambda: _train_output(paths))
        elif job_type == "backtest":
            output_path = stage("backtest", "Running portfolio research backtests", lambda: _backtest_output(paths))
        elif job_type == "publish":
            output_path = stage("publish", "Publishing batch artifacts", lambda: _publish_output(paths))
        elif job_type == "report":
            output_path = stage("report", "Generating research report bundle", lambda: _report_output(paths))
        else:
            manifest = publish_sample_snapshot(paths)
            output_path = manifest.pdfPath if job_type == "report" else str(paths.published_dir)

        mark_job_complete(paths, record, output_path=output_path)
        return 0
    except Exception as exc:  # pragma: no cover - defensive path
        mark_job_failed(paths, record, f"{job_type} failed: {exc}")
        return 1


def _ingest_output(paths: AppPaths, market: str) -> str:
    ingest_market(paths, market=market, years=5, limit=None)
    return str(paths.normalized_dir)


def _feature_output(paths: AppPaths) -> str:
    build_baseline_signals(paths)
    return str(paths.normalized_dir / "signal_panel.parquet")


def _train_output(paths: AppPaths) -> str:
    build_ml_signals(paths)
    return str(paths.normalized_dir / "ranking_panel.parquet")


def _backtest_output(paths: AppPaths) -> str:
    backtest_models(paths)
    return str(paths.normalized_dir / "backtest_panel.parquet")


def _publish_output(paths: AppPaths) -> str:
    publish_real(paths)
    return str(paths.published_dir)


def _report_output(paths: AppPaths) -> str:
    publish_real(paths)
    manifest = read_json(paths.published_dir / "report-manifest.json", {})
    return str(manifest.get("pdfPath") or paths.published_dir)


def handle_scheduler(root: str, years: int, limit: int | None, poll_seconds: int, once: bool) -> int:
    paths = AppPaths.from_root(root)
    run_scheduler(paths, years=years, limit=limit, poll_seconds=poll_seconds, once=once)
    return 0


def handle_worker(root: str, years: int, limit: int | None, poll_seconds: int) -> int:
    paths = AppPaths.from_root(root)
    if not (paths.published_dir / "universes.json").exists():
        publish_sample_snapshot(paths)
    print(f"Worker scheduler active at {paths.root}")
    run_scheduler(paths, years=years, limit=limit, poll_seconds=poll_seconds, once=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="newquantmodel")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("publish-sample")
    sample.add_argument("--root", required=True)

    smoke = subparsers.add_parser("smoke")
    smoke.add_argument("--root", required=True)

    ingest = subparsers.add_parser("ingest")
    ingest.add_argument("--root", required=True)
    ingest.add_argument("--market", required=True, choices=["crypto", "cn_equity", "us_equity"])
    ingest.add_argument("--years", type=int, default=5)
    ingest.add_argument("--limit", type=int)

    baseline = subparsers.add_parser("build-baseline-signals")
    baseline.add_argument("--root", required=True)

    ml = subparsers.add_parser("build-ml-signals")
    ml.add_argument("--root", required=True)

    backtest = subparsers.add_parser("backtest-baseline")
    backtest.add_argument("--root", required=True)

    publish = subparsers.add_parser("publish-real")
    publish.add_argument("--root", required=True)

    refresh = subparsers.add_parser("refresh-real")
    refresh.add_argument("--root", required=True)
    refresh.add_argument("--years", type=int, default=5)
    refresh.add_argument("--limit", type=int)

    refresh_market_parser = subparsers.add_parser("refresh-market")
    refresh_market_parser.add_argument("--root", required=True)
    refresh_market_parser.add_argument("--market", required=True, choices=["crypto", "cn_equity", "us_equity"])
    refresh_market_parser.add_argument("--years", type=int, default=5)
    refresh_market_parser.add_argument("--limit", type=int)

    run_job = subparsers.add_parser("run-job")
    run_job.add_argument("--root", required=True)
    run_job.add_argument("--job-id", required=True)
    run_job.add_argument("--job-type", required=True)

    scheduler = subparsers.add_parser("scheduler")
    scheduler.add_argument("--root", required=True)
    scheduler.add_argument("--years", type=int, default=5)
    scheduler.add_argument("--limit", type=int)
    scheduler.add_argument("--poll-seconds", type=int, default=60)
    scheduler.add_argument("--once", action="store_true")

    worker = subparsers.add_parser("worker")
    worker.add_argument("--root", required=True)
    worker.add_argument("--years", type=int, default=5)
    worker.add_argument("--limit", type=int)
    worker.add_argument("--poll-seconds", type=int, default=60)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "publish-sample":
        return handle_publish_sample(args.root)
    if args.command == "smoke":
        return handle_smoke(args.root)
    if args.command == "ingest":
        return handle_ingest(args.root, args.market, args.years, args.limit)
    if args.command == "build-baseline-signals":
        return handle_build_baseline(args.root)
    if args.command == "build-ml-signals":
        return handle_build_ml(args.root)
    if args.command == "backtest-baseline":
        return handle_backtest(args.root)
    if args.command == "publish-real":
        return handle_publish_real(args.root)
    if args.command == "refresh-real":
        return handle_refresh_real(args.root, args.years, args.limit)
    if args.command == "refresh-market":
        return handle_refresh_market(args.root, args.market, args.years, args.limit)
    if args.command == "run-job":
        return handle_run_job(args.root, args.job_id, args.job_type)
    if args.command == "scheduler":
        return handle_scheduler(args.root, args.years, args.limit, args.poll_seconds, args.once)
    if args.command == "worker":
        return handle_worker(args.root, args.years, args.limit, args.poll_seconds)
    raise ValueError(f"Unsupported command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
