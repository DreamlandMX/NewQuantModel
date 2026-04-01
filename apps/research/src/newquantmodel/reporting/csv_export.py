from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Iterable


def _as_row(row: object) -> dict:
    if is_dataclass(row):
        return asdict(row)
    if hasattr(row, "__dict__"):
        return dict(row.__dict__)
    raise TypeError(f"Unsupported row type: {type(row)!r}")


def write_csv(path: Path, rows: Iterable[object]) -> Path:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    first = _as_row(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(first.keys()))
        writer.writeheader()
        writer.writerow(first)
        for row in rows[1:]:
            writer.writerow(_as_row(row))
    return path
