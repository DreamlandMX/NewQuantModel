from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from newquantmodel.config.settings import AppPaths


DATASET_FILES = {
    "asset_master": "asset_master.parquet",
    "universe_membership": "universe_membership.parquet",
    "bars_1h": "bars_1h.parquet",
    "bars_1d": "bars_1d.parquet",
    "signal_panel": "signal_panel.parquet",
    "crypto_feature_panel": "crypto_feature_panel.parquet",
    "baseline_forecast_panel": "baseline_forecast_panel.parquet",
    "baseline_ranking_panel": "baseline_ranking_panel.parquet",
    "forecast_panel": "forecast_panel.parquet",
    "ranking_panel": "ranking_panel.parquet",
    "trade_plan_panel": "trade_plan_panel.parquet",
    "prediction_history_panel": "prediction_history_panel.parquet",
    "model_run_panel": "model_run_panel.parquet",
    "backtest_panel": "backtest_panel.parquet",
    "data_health": "data_health.parquet",
}


def dataset_path(paths: AppPaths, name: str) -> Path:
    return paths.normalized_dir / DATASET_FILES[name]


def read_frame(paths: AppPaths, name: str) -> pd.DataFrame:
    path = dataset_path(paths, name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def write_frame(paths: AppPaths, name: str, frame: pd.DataFrame) -> Path:
    path = dataset_path(paths, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    frame.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)
    return path


def _align_for_concat(left: pd.DataFrame, right: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = list(dict.fromkeys([*left.columns.tolist(), *right.columns.tolist()]))
    left_aligned = left.reindex(columns=columns).copy()
    right_aligned = right.reindex(columns=columns).copy()

    for column in columns:
        if pd.api.types.is_datetime64_any_dtype(left_aligned[column]) or pd.api.types.is_datetime64_any_dtype(right_aligned[column]):
            left_aligned[column] = pd.to_datetime(left_aligned[column], errors="coerce")
            right_aligned[column] = pd.to_datetime(right_aligned[column], errors="coerce")

    return left_aligned, right_aligned


def _normalize_key_value(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def replace_market_rows(existing: pd.DataFrame, new: pd.DataFrame, market_column: str = "market") -> pd.DataFrame:
    if existing.empty:
        return new.copy()
    if new.empty:
        return existing.copy()
    markets = set(new[market_column].dropna().unique())
    filtered = existing[~existing[market_column].isin(markets)]
    filtered, new_aligned = _align_for_concat(filtered, new)
    return pd.concat([filtered, new_aligned], ignore_index=True)


def replace_rows_by_keys(existing: pd.DataFrame, new: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    if existing.empty:
        return new.copy()
    if new.empty:
        return existing.copy()
    combined: dict[tuple[object, ...], dict] = {}
    columns = list(dict.fromkeys([*existing.columns.tolist(), *new.columns.tolist()]))

    for record in existing.to_dict(orient="records"):
        key = tuple(_normalize_key_value(record.get(column)) for column in key_columns)
        combined[key] = record

    for record in new.to_dict(orient="records"):
        key = tuple(_normalize_key_value(record.get(column)) for column in key_columns)
        combined[key] = record

    out = pd.DataFrame(combined.values())
    if out.empty:
        return out
    return out.reindex(columns=columns)


def sync_duckdb(paths: AppPaths) -> None:
    connection = duckdb.connect(str(paths.duckdb_path))
    try:
        for table_name in DATASET_FILES:
            parquet = dataset_path(paths, table_name)
            if parquet.exists():
                connection.execute(f"create or replace table {table_name} as select * from read_parquet(?)", [str(parquet)])
    finally:
        connection.close()
