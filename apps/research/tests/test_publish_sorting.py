from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from newquantmodel.config.settings import AppPaths
from newquantmodel.publish.real_pipeline import _build_sort_lookup


class PublishSortingTest(unittest.TestCase):
    def test_builds_crypto_volume_rank_and_non_crypto_turnover_rank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = AppPaths.from_root(root)
            payload = [
                {"symbol": "BTC", "market_cap_rank": 1, "market_cap": 1_000_000_000_000},
                {"symbol": "ETH", "market_cap_rank": 2, "market_cap": 500_000_000_000},
                {"coingecko_id": "usd1-wlfi", "symbol": "USD1", "market_cap_rank": 3, "market_cap": 450_000_000_000},
                {"symbol": "BNB", "market_cap_rank": 4, "market_cap": 100_000_000_000},
            ]
            (paths.raw_dir / "crypto").mkdir(parents=True, exist_ok=True)
            (paths.raw_dir / "crypto" / "coingecko_top.json").write_text(json.dumps(payload), encoding="utf8")

            asset_master = pd.DataFrame(
                [
                    {"symbol": "BTCUSDT", "market": "crypto"},
                    {"symbol": "ETHUSDT", "market": "crypto"},
                    {"symbol": "BNBUSDT", "market": "crypto"},
                    {"symbol": "AAPL", "market": "us_equity"},
                    {"symbol": "MSFT", "market": "us_equity"},
                    {"symbol": "^GSPC", "market": "index"},
                ]
            )
            rankings = pd.DataFrame(
                [
                    {"symbol": "AAPL", "rank": 1, "score": 1.2, "targetWeight": 0.12},
                    {"symbol": "MSFT", "rank": 2, "score": 0.8, "targetWeight": 0.08},
                ]
            )
            bars_1h = pd.DataFrame(
                [
                    {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 80000.0, "volume": 10_000.0},
                    {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 3000.0, "volume": 20_000.0},
                    {"symbol": "BNBUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 600.0, "volume": 5_000.0},
                ]
            )
            bars_1d = pd.DataFrame(
                [
                    {"symbol": "AAPL", "market": "us_equity", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 200.0, "volume": 1_000.0},
                    {"symbol": "MSFT", "market": "us_equity", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 100.0, "volume": 1_000.0},
                    {"symbol": "^GSPC", "market": "index", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 5000.0, "volume": 0.0},
                ]
            )

            lookup = _build_sort_lookup(paths, asset_master, rankings, bars_1d, bars_1h)

            self.assertEqual(int(lookup[("crypto", "BTCUSDT")]["sortRank"]), 1)
            self.assertEqual(int(lookup[("crypto", "ETHUSDT")]["sortRank"]), 2)
            self.assertEqual(int(lookup[("crypto", "BNBUSDT")]["sortRank"]), 3)
            self.assertEqual(str(lookup[("crypto", "BTCUSDT")]["sortMetricLabel"]), "24h turnover rank")
            self.assertEqual(float(lookup[("crypto", "BTCUSDT")]["sortMetric"]), 800_000_000.0)
            self.assertNotIn(("crypto", "USD1USDT"), lookup)

            self.assertEqual(int(lookup[("us_equity", "AAPL")]["sortRank"]), 1)
            self.assertEqual(int(lookup[("us_equity", "MSFT")]["sortRank"]), 2)
            self.assertEqual(str(lookup[("us_equity", "AAPL")]["sortMetricLabel"]), "1d turnover rank")

            self.assertEqual(int(lookup[("index", "^GSPC")]["sortRank"]), 1)
            self.assertEqual(str(lookup[("index", "^GSPC")]["sortMetricLabel"]), "1d turnover rank")

    def test_forces_btc_eth_sol_into_top_three(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = AppPaths.from_root(root)
            payload = [
                {"symbol": "BTC", "market_cap_rank": 1, "market_cap": 1_000_000_000_000},
                {"symbol": "ETH", "market_cap_rank": 2, "market_cap": 500_000_000_000},
                {"symbol": "XRP", "market_cap_rank": 4, "market_cap": 200_000_000_000},
                {"symbol": "BNB", "market_cap_rank": 5, "market_cap": 150_000_000_000},
                {"symbol": "SOL", "market_cap_rank": 7, "market_cap": 100_000_000_000},
            ]
            (paths.raw_dir / "crypto").mkdir(parents=True, exist_ok=True)
            (paths.raw_dir / "crypto" / "coingecko_top.json").write_text(json.dumps(payload), encoding="utf8")

            asset_master = pd.DataFrame(
                [
                    {"symbol": "BTCUSDT", "market": "crypto"},
                    {"symbol": "ETHUSDT", "market": "crypto"},
                    {"symbol": "XRPUSDT", "market": "crypto"},
                    {"symbol": "BNBUSDT", "market": "crypto"},
                    {"symbol": "SOLUSDT", "market": "crypto"},
                ]
            )
            bars_1h = pd.DataFrame(
                [
                    {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 80000.0, "volume": 10_000.0},
                    {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 3000.0, "volume": 20_000.0},
                    {"symbol": "XRPUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 2.0, "volume": 2_000_000_000.0},
                    {"symbol": "BNBUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 600.0, "volume": 4_000_000.0},
                    {"symbol": "SOLUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-04-01T00:00:00Z"), "close": 180.0, "volume": 8_000_000.0},
                ]
            )

            lookup = _build_sort_lookup(paths, asset_master, pd.DataFrame(), pd.DataFrame(), bars_1h)

            self.assertEqual(int(lookup[("crypto", "BTCUSDT")]["sortRank"]), 1)
            self.assertEqual(int(lookup[("crypto", "ETHUSDT")]["sortRank"]), 2)
            self.assertEqual(int(lookup[("crypto", "SOLUSDT")]["sortRank"]), 3)
            self.assertEqual(int(lookup[("crypto", "XRPUSDT")]["sortRank"]), 4)
            self.assertEqual(int(lookup[("crypto", "BNBUSDT")]["sortRank"]), 5)
            self.assertEqual(str(lookup[("crypto", "SOLUSDT")]["sortMetricLabel"]), "24h turnover rank")


if __name__ == "__main__":
    unittest.main()
