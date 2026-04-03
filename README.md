# newquantmodel

`newquantmodel` is a local-first quantitative research terminal for crypto, China A-shares, US equities, and major benchmark indices.

## Stack

- Frontend: React + Vite + TypeScript + ECharts + Lightweight Charts
- API: Fastify + TypeScript
- Research pipeline: Python 3.12
- Data: Postgres + Parquet + DuckDB
- Orchestration: Docker Compose, Redis, batch-published artifacts, local scheduler

## Apps

- `apps/web`: finance-terminal UI
- `apps/api`: read-only publishing API and job control
- `apps/research`: offline ingest, baseline features, ML overlay training, backtest, publish, report generation, scheduler
- `packages/shared-types`: shared TypeScript/Python contracts

## Quick start

1. Enable Docker Desktop WSL integration.
2. Install Node.js and pnpm on the host if you want local package scripts.
3. Copy `.env.example` to `.env`.
4. Run `docker compose up --build`.

## Real research pipeline

The research app supports the full V1 flow:

- real data ingest with cache/fallback
- baseline signal generation
- ML overlay training
- portfolio backtests
- batch publish + Markdown/CSV/PDF research exports

### Full refresh

```bash
pnpm research:refresh-full
```

### Stage-by-stage

```bash
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main ingest --root . --market crypto --years 5
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main ingest --root . --market cn_equity --years 5
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main ingest --root . --market us_equity --years 5
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main build-baseline-signals --root .
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main build-ml-signals --root .
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main backtest-baseline --root .
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m newquantmodel.cli.main publish-real --root .
```

### Long-running scheduler

```bash
pnpm research:scheduler
```

### Always-on worker

If you want trade plans to refresh automatically and expire cleanly without manual intervention, run the long-lived worker instead of relying on one-off publishes:

```bash
bash scripts/run_research_worker.sh
```

The worker writes:

- log: `storage/logs/research-worker.log`
- pid: `storage/logs/research-worker.pid`
- scheduler state: `storage/reference/scheduler_state.json`

On `Windows + WSL`, the recommended auto-start action for Task Scheduler is:

```powershell
powershell.exe -ExecutionPolicy Bypass -File E:\NewQuantModel\scripts\start_research_worker.ps1
```

To register the scheduled task automatically:

```powershell
powershell.exe -ExecutionPolicy Bypass -File E:\NewQuantModel\scripts\register_research_worker_task.ps1
```

To verify the task later:

```powershell
powershell.exe -ExecutionPolicy Bypass -File E:\NewQuantModel\scripts\check_research_worker_task.ps1
```

Recommended Task Scheduler setup:

1. Trigger on user logon or machine startup.
2. Enable restart on failure.
3. Keep a single instance only.

When the worker is running, the frontend and API will reflect:

- current `Actionable`, `Filtered`, `Expired`, and `Stale` trade-plan states
- worker heartbeat and next refresh window
- stale fallback reasons when a market refresh fails

Workbench now supports a `Status` filter so you can focus on:

- `Actionable`
- `Expired`
- `Stale`
- `Filtered`

Default schedule:

- `crypto`: every 4 hours in `UTC`
- `cn_equity`: `Asia/Shanghai 16:30`
- `us_equity`: `America/New_York 17:30`

The published snapshot lands in `storage/published/`, model artifacts in `storage/catalog/models/`, and research exports in `storage/exports/`.

## Model stack

- Stocks: `LightGBM Ranker` overlay for `cn_equity` and `us_equity`
- Crypto: daily regression for ranking/backtest plus hourly classifier/regression/quantile forecast overlay
- Indices: regime classifier + return regressor
- Baseline fallback remains available as `baseline-signals-v1`

Published artifacts carry:

- `modelVersion`
- `publishedAt`
- `dataSnapshotVersion`
- `stale`
- `coverageMode`
- `coveragePct`
- runtime trade-plan state: `status`, `isExpired`, `isBlocked`, `blockedReason`, `evaluatedAt`

## Sample + smoke path

The Python app ships with a sample publish flow so the UI and API can run before live data integrations are finished.

```bash
pnpm research:publish-sample
pnpm research:smoke
```

Published artifacts are written under `storage/published/`.

## Frontend and API validation

```bash
pnpm install
pnpm typecheck
pnpm build
PYTHONPATH=apps/research/src:packages/shared-types/python python3 -m unittest discover -s apps/research/tests
```

The API serves published artifacts from these routes:

- `GET /health`
- `GET /api/universes`
- `GET /api/assets`
- `GET /api/assets/:symbol`
- `GET /api/health/data`
- `GET /api/forecasts`
- `GET /api/rankings`
- `GET /api/backtests/:strategyId`
- `GET /api/jobs`
- `POST /api/jobs/run`
- `GET /api/reports/latest`

To run the built API locally after `pnpm build`:

```bash
APP_ROOT=/absolute/path/to/newquantmodel \
PUBLISHED_DATA_DIR=/absolute/path/to/newquantmodel/storage/published \
PORT=4000 \
pnpm --filter @newquantmodel/api start
```

## Current validation snapshot

The repo currently contains a verified real publish generated on `2026-04-01 UTC`.

- `universes`: `10`
- `assets`: `21`
- `forecasts`: `81`
- `rankings`: `100`
- `backtests`: `12`

That snapshot proves the full chain is working, but it still reflects the smaller verification universe that was ingested earlier. For a true full-market publish, rerun the ingest/refresh commands without any development-time limits.
