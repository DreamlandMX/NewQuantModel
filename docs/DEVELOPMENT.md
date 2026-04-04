# Development Notes

## Local prerequisites

- Docker Desktop with WSL integration
- Node.js + pnpm for host-side web/API development
- Python 3.12 for research scripts

## First sample publish

```bash
PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main publish-sample --root .
```

## Smoke test

```bash
PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main smoke --root .
```

## Real ingest + publish

```bash
PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main ingest --root . --market crypto --years 5

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main ingest --root . --market cn_equity --years 5

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main ingest --root . --market us_equity --years 5

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main build-baseline-signals --root .

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main backtest-baseline --root .

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main publish-real --root .
```

For fast verification, add `--limit 5` to the ingest commands.

## Selective ML training

Use the scoped ML entrypoint when you only need one market/frequency slice:

```bash
PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main build-ml-signals --root . --market us_equity --signal-frequency daily --pipeline equity --fast

PYTHONPATH=apps/research/src:packages/shared-types/python \
python3 -m newquantmodel.cli.main build-ml-signals --root . --market index --signal-frequency weekly --pipeline index --fast
```

The GA cache now keys off a richer `dataSignature` rather than only the latest timestamp, and the stored summary includes a reserved holdout slice for quick out-of-sample inspection.

`publish-real` checks the holdout fitness for signed ML GA rows before writing fresh artifacts. The default threshold is `-1.5`; use `NQM_PUBLISH_MIN_HOLDOUT_FITNESS` to tune the release threshold.

## Docker stack

```bash
docker compose up --build
```

## TypeScript checks

```bash
pnpm install
pnpm typecheck
pnpm build
```

The current implementation supports both sample publish outputs and real batch-published research artifacts.
