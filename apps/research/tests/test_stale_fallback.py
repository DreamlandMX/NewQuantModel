from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from newquantmodel.config.settings import AppPaths
from newquantmodel.publish.real_pipeline import _mark_stale
from newquantmodel.storage.json_store import read_json, write_json
from newquantmodel.storage.parquet_store import write_frame


class StaleFallbackTest(unittest.TestCase):
    def test_mark_stale_handles_legacy_universe_records(self) -> None:
        with tempfile.TemporaryDirectory(prefix="newquantmodel-stale-") as tmp:
            paths = AppPaths.from_root(tmp)
            write_frame(
                paths,
                "data_health",
                pd.DataFrame(
                    [
                        {
                            "market": "crypto",
                            "lastRefreshAt": "2026-04-01T00:00:00Z",
                            "coveragePct": 100.0,
                            "missingBarPct": 0.0,
                            "tradableCoveragePct": 100.0,
                            "membershipMode": "approx_bootstrap",
                            "historyStartDate": "2021-04-01",
                            "stale": False,
                            "notes": ["healthy"],
                        }
                    ]
                ),
            )
            write_json(
                paths.reference_dir / "universes_reference.json",
                [
                    {
                        "market": "crypto",
                        "universe": "crypto_top50_spot",
                        "coverageDate": "2026-04-01",
                        "memberCount": 50,
                        "policyNotes": ["legacy row without stale flag"],
                        "tradableProxy": "Binance perpetual archive",
                        "dataSource": "CoinGecko + Binance",
                        "coverageMode": "approx_bootstrap",
                        "historyStartDate": "2021-04-01",
                        "coveragePct": 100.0,
                        "refreshSchedule": "00:00/04:00/08:00/12:00/16:00/20:00 UTC",
                        "lastRefreshAt": "2026-04-01T00:00:00Z",
                    }
                ],
            )

            _mark_stale(paths, "crypto", "forced failure")

            data_health = pd.read_parquet(Path(paths.normalized_dir) / "data_health.parquet")
            self.assertTrue(bool(data_health.loc[data_health["market"] == "crypto", "stale"].iloc[0]))
            self.assertIn("forced failure", " ".join(data_health.loc[data_health["market"] == "crypto", "notes"].iloc[0]))

            universes = read_json(paths.reference_dir / "universes_reference.json", [])
            self.assertEqual(len(universes), 1)
            self.assertTrue(bool(universes[0]["stale"]))
            self.assertIn("forced failure", " ".join(universes[0]["policyNotes"]))


if __name__ == "__main__":
    unittest.main()
