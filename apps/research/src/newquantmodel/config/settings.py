from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class AppPaths:
    root: Path
    raw_dir: Path
    reference_dir: Path
    normalized_dir: Path
    catalog_dir: Path
    models_dir: Path
    published_dir: Path
    exports_dir: Path
    duckdb_path: Path

    @classmethod
    def from_root(cls, root: str | Path) -> "AppPaths":
        root_path = Path(root).resolve()
        raw = root_path / "storage" / "raw"
        reference = root_path / "storage" / "reference"
        normalized = root_path / "storage" / "normalized"
        catalog = root_path / "storage" / "catalog"
        models = catalog / "models"
        published = root_path / "storage" / "published"
        exports = root_path / "storage" / "exports"
        raw.mkdir(parents=True, exist_ok=True)
        reference.mkdir(parents=True, exist_ok=True)
        normalized.mkdir(parents=True, exist_ok=True)
        catalog.mkdir(parents=True, exist_ok=True)
        models.mkdir(parents=True, exist_ok=True)
        published.mkdir(parents=True, exist_ok=True)
        exports.mkdir(parents=True, exist_ok=True)
        return cls(
            root=root_path,
            raw_dir=raw,
            reference_dir=reference,
            normalized_dir=normalized,
            catalog_dir=catalog,
            models_dir=models,
            published_dir=published,
            exports_dir=exports,
            duckdb_path=catalog / "newquantmodel.duckdb",
        )
