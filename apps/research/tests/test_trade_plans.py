from __future__ import annotations

import unittest

import pandas as pd

from newquantmodel.analytics.trade_plans import build_trade_plan_panel


class TradePlanPanelTest(unittest.TestCase):
    def test_builds_actionable_long_and_short_trade_plans(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "name": "Apple",
                    "market": "us_equity",
                    "timezone": "America/New_York",
                    "isTradable": True,
                    "hedgeProxy": "QQQ / PSQ",
                    "memberships": ["nasdaq100"],
                    "riskBucket": "mega-cap",
                    "primaryVenue": "NASDAQ",
                    "tradableSymbol": "AAPL",
                    "quoteAsset": "USD",
                    "hasPerpetualProxy": False,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "market": "us_equity",
                    "universe": "nasdaq100",
                    "horizon": "5D",
                    "pUp": 0.62,
                    "expectedReturn": 0.015,
                    "q10": -0.01,
                    "q50": 0.012,
                    "q90": 0.03,
                    "alphaScore": 0.7,
                    "confidence": 0.7,
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "equity-lgbm-ranker-v1",
                    "asOfDate": "2026-03-31",
                },
                {
                    "symbol": "AAPL",
                    "market": "us_equity",
                    "universe": "nasdaq100",
                    "horizon": "20D",
                    "pUp": 0.35,
                    "expectedReturn": -0.02,
                    "q10": -0.03,
                    "q50": -0.01,
                    "q90": 0.01,
                    "alphaScore": -0.6,
                    "confidence": 0.73,
                    "regime": "risk-off",
                    "riskFlags": ["test"],
                    "modelVersion": "equity-lgbm-ranker-v1",
                    "asOfDate": "2026-03-31",
                },
            ]
        )
        rankings = pd.DataFrame(
            [
                {"symbol": "AAPL", "universe": "nasdaq100", "strategyMode": "long_only", "rebalanceFreq": "daily", "modelVersion": "equity-lgbm-ranker-v1"},
                {"symbol": "AAPL", "universe": "nasdaq100", "strategyMode": "hedged", "rebalanceFreq": "weekly", "modelVersion": "equity-lgbm-ranker-v1"},
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {"symbol": "AAPL", "market": "us_equity", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 220.0, "high": 222.0, "low": 219.0, "close": 221.0, "volume": 1000.0}
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, pd.DataFrame(), universes=[])

        actionable = frame[frame["actionable"]]
        long_plan = actionable[(actionable["side"] == "long") & (actionable["horizon"] == "5D")].iloc[0]
        short_plan = actionable[(actionable["side"] == "short") & (actionable["horizon"] == "20D")].iloc[0]

        self.assertAlmostEqual(float(long_plan["entryPrice"]), 221.0, places=6)
        self.assertAlmostEqual(float(long_plan["stopLossPrice"]), 218.79, places=2)
        self.assertAlmostEqual(float(long_plan["takeProfitPrice"]), 227.63, places=2)
        self.assertAlmostEqual(float(long_plan["riskRewardRatio"]), 3.0, places=6)
        self.assertEqual(str(short_plan["executionSymbol"]), "AAPL")
        self.assertEqual(str(short_plan["executionMode"]), "research_short")
        self.assertGreater(float(short_plan["riskRewardRatio"]), 1.5)

    def test_skips_cn_equity_short_and_flags_non_tradable_crypto(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "name": "Moutai",
                    "market": "cn_equity",
                    "timezone": "Asia/Shanghai",
                    "isTradable": True,
                    "hedgeProxy": "IF main contract",
                    "memberships": ["csi300"],
                    "riskBucket": "quality",
                    "primaryVenue": "SSE",
                    "tradableSymbol": "600519.SH",
                    "quoteAsset": "CNY",
                    "hasPerpetualProxy": False,
                    "historyCoverageStart": "2021-01-01",
                },
                {
                    "symbol": "DOGEUSDT",
                    "name": "Dogecoin",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": False,
                    "hedgeProxy": "DOGEUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": None,
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": False,
                    "historyCoverageStart": "2021-01-01",
                },
            ]
        )
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "market": "cn_equity",
                    "universe": "csi300",
                    "horizon": "5D",
                    "pUp": 0.40,
                    "expectedReturn": -0.01,
                    "q10": -0.03,
                    "q50": -0.01,
                    "q90": 0.01,
                    "alphaScore": -0.5,
                    "confidence": 0.8,
                    "regime": "defensive",
                    "riskFlags": ["test"],
                    "modelVersion": "equity-lgbm-ranker-v1",
                    "asOfDate": "2026-03-31",
                },
                {
                    "symbol": "DOGEUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1H",
                    "pUp": 0.7,
                    "expectedReturn": 0.02,
                    "q10": -0.01,
                    "q50": 0.01,
                    "q90": 0.03,
                    "alphaScore": 0.9,
                    "confidence": 0.8,
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "crypto-ts-v1",
                    "asOfDate": "2026-03-31",
                },
            ]
        )
        rankings = pd.DataFrame(
            [
                {"symbol": "600519.SH", "universe": "csi300", "strategyMode": "hedged", "rebalanceFreq": "weekly", "modelVersion": "equity-lgbm-ranker-v1"},
                {"symbol": "DOGEUSDT", "universe": "crypto_top50_spot", "strategyMode": "hedged", "rebalanceFreq": "daily", "modelVersion": "crypto-ts-v1"},
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {"symbol": "600519.SH", "market": "cn_equity", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 1500.0, "high": 1510.0, "low": 1490.0, "close": 1505.0, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 0.12, "high": 0.13, "low": 0.11, "close": 0.125, "volume": 1000.0},
            ]
        )
        bars_1h = pd.DataFrame(
            [
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T01:00:00Z"), "open": 0.12, "high": 0.13, "low": 0.11, "close": 0.124, "volume": 1000.0}
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=[])

        self.assertTrue(frame[(frame["symbol"] == "600519.SH") & (frame["side"] == "short")].empty)
        crypto_long = frame[(frame["symbol"] == "DOGEUSDT") & (frame["side"] == "long")].iloc[0]
        self.assertFalse(bool(crypto_long["actionable"]))
        self.assertIn("non_tradable_or_missing_perpetual_proxy", str(crypto_long["rejectionReason"]))


if __name__ == "__main__":
    unittest.main()
