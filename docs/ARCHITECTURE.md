# newquantmodel Architecture

## Runtime split

- `apps/web`: React research terminal
- `apps/api`: Fastify read API + job triggers
- `apps/research`: offline Python pipeline and report generation

## Data flow

1. Research jobs ingest and transform data.
2. Baseline deterministic signals and backtests are generated from normalized datasets.
3. Published artifacts land in `storage/published/`.
4. API serves those artifacts without request-time inference.
5. Frontend renders published snapshots, rankings, backtests, and report metadata.

## Phase 2 data layers

- `storage/raw/`: provider snapshots and cached price fetches
- `storage/reference/`: universe metadata and reference snapshots
- `storage/normalized/`: `asset_master`, `universe_membership`, `bars_1h`, `bars_1d`, `signal_panel`, `ranking_panel`, `forecast_panel`, `backtest_panel`, `data_health`
- `storage/catalog/newquantmodel.duckdb`: DuckDB mirror for local analysis
- `storage/published/`: API-facing JSON artifacts
- `storage/exports/`: Markdown, CSV, and PDF report bundles

## Real providers

- Crypto universe: CoinGecko
- Crypto tradability and hourly bars: Binance archive + data API
- China membership snapshots: yfiua current/monthly constituent feed
- China daily bars: EastMoney with Yahoo fallback + local cache
- US membership snapshots: yfiua current/monthly constituent feed
- US daily bars and indices: Yahoo Finance + local cache

## Research defaults

- Crypto uses Top 50 spot universe with perpetual-only tradable proxies.
- China research uses CSI 300 membership plus IF futures hedge proxy.
- US universes preserve overlap across Dow 30, Nasdaq 100, and S&P 500.
- Indices include SSE Composite, Dow, Nasdaq 100, and S&P 500 forecast universes.
- Strategy modes: `long_only`, `hedged`
- Rebalance modes: `daily`, `weekly`
- Coverage metadata is exposed as `approx_bootstrap` or `point_in_time`.
