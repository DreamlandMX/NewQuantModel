from __future__ import annotations

import unittest

import pandas as pd

from newquantmodel.analytics.trade_plans import build_trade_plan_panel


class TradePlanPanelTest(unittest.TestCase):
    def test_keeps_only_one_active_side_per_symbol_horizon(self) -> None:
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
                    "pUp": 0.67,
                    "expectedReturn": 0.018,
                    "q10": -0.01,
                    "q50": 0.018,
                    "q90": 0.03,
                    "alphaScore": 0.7,
                    "confidence": 0.7,
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "equity-lgbm-ranker-v1",
                    "asOfDate": pd.Timestamp.utcnow().date().isoformat(),
                }
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "universe": "nasdaq100",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "equity-lgbm-ranker-v1",
                    "rank": 1,
                    "score": 1.35,
                    "targetWeight": 0.09,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "market": "us_equity",
                    "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"),
                    "open": 220.0,
                    "high": 222.0,
                    "low": 219.0,
                    "close": 221.0,
                    "volume": 1000.0,
                }
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, pd.DataFrame(), universes=[])

        active = frame[frame["actionable"]]
        self.assertEqual(len(active), 1)
        self.assertEqual(str(active.iloc[0]["side"]), "long")
        self.assertEqual(int(active.iloc[0]["selectionRank"]), 1)
        self.assertEqual(str(active.iloc[0]["selectionReason"]), "selected_active_trade")

        self.assertTrue(frame[(frame["side"] == "short") & (frame["horizon"] == "5D")].empty)

    def test_allows_multiple_horizons_same_direction_and_varied_trade_confidence(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "name": "Ethereum",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": True,
                    "hedgeProxy": "ETHUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": True,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1H",
                    "pUp": 0.63,
                    "expectedReturn": 0.014,
                    "q10": -0.004,
                    "q50": 0.014,
                    "q90": 0.018,
                    "alphaScore": 0.6,
                    "confidence": 0.7,
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "crypto-ts-v1",
                    "asOfDate": as_of,
                },
                {
                    "symbol": "ETHUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1D",
                    "pUp": 0.71,
                    "expectedReturn": 0.024,
                    "q10": -0.01,
                    "q50": 0.024,
                    "q90": 0.03,
                    "alphaScore": 1.0,
                    "confidence": 0.7,
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "crypto-ts-v1",
                    "asOfDate": as_of,
                },
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "universe": "crypto_top50_spot",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "crypto-ts-v1",
                    "rank": 1,
                    "score": 1.10,
                    "targetWeight": 0.12,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "market": "crypto",
                    "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"),
                    "open": 2000.0,
                    "high": 2050.0,
                    "low": 1980.0,
                    "close": 2100.0,
                    "volume": 1000.0,
                }
            ]
        )
        bars_1h = pd.DataFrame(
            [
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 2087.0, "high": 2098.0, "low": 2085.0, "close": 2092.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T01:00:00Z"), "open": 2092.0, "high": 2104.0, "low": 2088.0, "close": 2098.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T02:00:00Z"), "open": 2098.0, "high": 2112.0, "low": 2094.0, "close": 2105.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T03:00:00Z"), "open": 2105.0, "high": 2118.0, "low": 2098.0, "close": 2108.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T04:00:00Z"), "open": 2108.0, "high": 2120.0, "low": 2101.0, "close": 2110.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T05:00:00Z"), "open": 2110.0, "high": 2122.0, "low": 2104.0, "close": 2111.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T06:00:00Z"), "open": 2094.0, "high": 2098.0, "low": 2086.0, "close": 2090.0, "volume": 1000.0},
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=[])

        active = frame[frame["actionable"]]
        self.assertEqual(set(active["side"].tolist()), {"long"})
        self.assertEqual(set(active["horizon"].tolist()), {"1H", "1D"})
        confidences = active["tradeConfidence"].tolist()
        self.assertEqual(len(confidences), 2)
        self.assertNotAlmostEqual(float(confidences[0]), float(confidences[1]), places=6)
        self.assertTrue((active["directionProbability"] > 0.55).all())

    def test_skips_conflicting_index_long_geometry(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "^GSPC",
                    "name": "S&P 500 Index",
                    "market": "index",
                    "timezone": "America/New_York",
                    "isTradable": True,
                    "hedgeProxy": "SPY / SH",
                    "memberships": ["sp500_index"],
                    "riskBucket": "benchmark",
                    "primaryVenue": "Yahoo Finance",
                    "tradableSymbol": "^GSPC",
                    "quoteAsset": "USD",
                    "hasPerpetualProxy": False,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "^GSPC",
                    "market": "index",
                    "universe": "sp500_index",
                    "horizon": "1D",
                    "pUp": 0.60,
                    "expectedReturn": -0.016,
                    "q10": -0.026,
                    "q50": -0.016,
                    "q90": -0.006,
                    "alphaScore": 0.0,
                    "confidence": 0.55,
                    "regime": "risk-off",
                    "riskFlags": ["test"],
                    "modelVersion": "index-regime-sr-ind-v1",
                    "asOfDate": as_of,
                    "forecastValidity": "adjusted",
                    "forecastConflictReason": "direction_quantile_mismatch_auto_flipped_bearish",
                    "forecastAdjusted": True,
                }
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "^GSPC",
                    "universe": "sp500_index",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "index-regime-sr-ind-v1",
                    "rank": 1,
                    "score": 0.2,
                    "targetWeight": 0.0,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {
                    "symbol": "^GSPC",
                    "market": "index",
                    "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"),
                    "open": 6500.0,
                    "high": 6600.0,
                    "low": 6480.0,
                    "close": 6575.32,
                    "volume": 1000.0,
                }
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, pd.DataFrame(), universes=[])
        self.assertTrue(frame.empty)

    def test_includes_indicator_alignment_and_notes(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "name": "Bitcoin",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": True,
                    "hedgeProxy": "BTCUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": True,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1D",
                    "pUp": 0.69,
                    "expectedReturn": 0.03,
                    "q10": -0.01,
                    "q50": 0.03,
                    "q90": 0.05,
                    "alphaScore": 0.8,
                    "confidence": 0.7,
                    "indicatorUnavailable": False,
                    "macdLine": 12.0,
                    "macdSignal": 9.0,
                    "macdHist": 3.0,
                    "macdState": "bullish_cross",
                    "rsi14": 58.0,
                    "rsiState": "neutral",
                    "atr14": 800.0,
                    "atrPct": 0.015,
                    "bbUpper": 72000.0,
                    "bbMid": 69000.0,
                    "bbLower": 66000.0,
                    "bbWidth": 0.08,
                    "bbPosition": 0.64,
                    "bbState": "upper_half",
                    "kValue": 61.0,
                    "dValue": 54.0,
                    "jValue": 75.0,
                    "kdjState": "above_signal",
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "crypto-ts-sr-ind-v1",
                    "asOfDate": as_of,
                }
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "universe": "crypto_top50_spot",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "crypto-ts-sr-ind-v1",
                    "rank": 1,
                    "score": 1.5,
                    "targetWeight": 0.12,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "market": "crypto",
                    "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"),
                    "open": 68000.0,
                    "high": 70500.0,
                    "low": 67500.0,
                    "close": 69000.0,
                    "volume": 1000.0,
                }
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, pd.DataFrame(), universes=[])

        self.assertIn("indicatorAlignmentScore", frame.columns)
        self.assertIn("indicatorNotes", frame.columns)
        self.assertGreater(float(frame.iloc[0]["indicatorAlignmentScore"]), 0.0)
        self.assertIn("MACD", str(frame.iloc[0]["indicatorNotes"]))

    def test_generates_support_resistance_setup_and_level_targets(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "name": "Ethereum",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": True,
                    "hedgeProxy": "ETHUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": True,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1D",
                    "pUp": 0.68,
                    "expectedReturn": 0.025,
                    "q10": -0.012,
                    "q50": 0.02,
                    "q90": 0.04,
                    "alphaScore": 0.9,
                    "confidence": 0.75,
                    "indicatorUnavailable": False,
                    "macdLine": 4.2,
                    "macdSignal": 3.7,
                    "macdHist": 0.5,
                    "macdState": "bullish_cross",
                    "rsi14": 48.0,
                    "rsiState": "neutral",
                    "atr14": 4.0,
                    "atrPct": 0.04,
                    "bbUpper": 109.0,
                    "bbMid": 103.0,
                    "bbLower": 97.0,
                    "bbWidth": 0.11,
                    "bbPosition": 0.42,
                    "bbState": "middle_band",
                    "kValue": 59.0,
                    "dValue": 52.0,
                    "jValue": 73.0,
                    "kdjState": "above_signal",
                    "regime": "risk-on",
                    "riskFlags": ["test"],
                    "modelVersion": "crypto-ts-sr-ind-v1",
                    "asOfDate": as_of,
                }
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "universe": "crypto_top50_spot",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "crypto-ts-sr-ind-v1",
                    "rank": 1,
                    "score": 1.25,
                    "targetWeight": 0.15,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-24T00:00:00Z"), "open": 101.0, "high": 106.0, "low": 100.0, "close": 104.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-25T00:00:00Z"), "open": 104.0, "high": 108.0, "low": 101.0, "close": 106.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-26T00:00:00Z"), "open": 106.0, "high": 109.0, "low": 100.5, "close": 103.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-27T00:00:00Z"), "open": 103.0, "high": 107.0, "low": 99.5, "close": 101.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"), "open": 101.0, "high": 105.0, "low": 98.0, "close": 99.5, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-29T00:00:00Z"), "open": 99.5, "high": 103.0, "low": 97.5, "close": 100.5, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-30T00:00:00Z"), "open": 100.5, "high": 104.0, "low": 98.5, "close": 101.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 101.0, "high": 104.0, "low": 98.8, "close": 99.2, "volume": 1000.0},
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, pd.DataFrame(), universes=[])

        row = frame.iloc[0]
        self.assertEqual(str(row["setupType"]), "bounce_long")
        self.assertEqual(str(row["entrySource"]), "support_resistance")
        self.assertEqual(str(row["stopSource"]), "support_resistance")
        self.assertEqual(str(row["targetSource"]), "support_resistance")
        self.assertGreater(float(row["nearestSupport"]), 0.0)
        self.assertGreater(float(row["nearestResistance"]), float(row["nearestSupport"]))
        self.assertGreater(float(row["riskRewardRatio"]), 1.0)
        self.assertFalse(bool(row["srUnavailable"]))

    def test_keeps_crypto_intraday_quantile_fallback_candidates_as_filtered_rows(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "name": "Bitcoin",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": True,
                    "hedgeProxy": "BTCUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": True,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1H",
                    "pUp": 0.49,
                    "expectedReturn": -0.0002,
                    "q10": -0.010,
                    "q50": -0.0002,
                    "q90": 0.008,
                    "alphaScore": 0.1,
                    "confidence": 0.4,
                    "indicatorUnavailable": False,
                    "macdState": "below_signal",
                    "rsi14": 49.0,
                    "rsiState": "neutral",
                    "atrPct": 0.035,
                    "bbState": "lower_half",
                    "kdjState": "above_signal",
                    "modelVersion": "baseline-ga-mf-v2",
                    "asOfDate": as_of,
                    "signalFrequency": "daily",
                    "sourceFrequency": "daily",
                    "isDerivedSignal": False,
                },
                {
                    "symbol": "BTCUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1D",
                    "pUp": 0.71,
                    "expectedReturn": 0.025,
                    "q10": -0.010,
                    "q50": 0.018,
                    "q90": 0.030,
                    "alphaScore": 0.8,
                    "confidence": 0.7,
                    "indicatorUnavailable": False,
                    "macdState": "bullish_cross",
                    "rsi14": 56.0,
                    "rsiState": "neutral",
                    "atrPct": 0.018,
                    "bbState": "inside_band",
                    "kdjState": "above_signal",
                    "modelVersion": "baseline-ga-mf-v2",
                    "asOfDate": as_of,
                    "signalFrequency": "daily",
                    "sourceFrequency": "daily",
                    "isDerivedSignal": False,
                },
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "BTCUSDT",
                    "universe": "crypto_top50_spot",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "baseline-ga-mf-v2",
                    "rank": 1,
                    "score": 1.0,
                    "targetWeight": 0.10,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-25T00:00:00Z"), "open": 67500.0, "high": 69000.0, "low": 66800.0, "close": 68400.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-26T00:00:00Z"), "open": 68400.0, "high": 70100.0, "low": 67600.0, "close": 69400.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-27T00:00:00Z"), "open": 69400.0, "high": 70600.0, "low": 68100.0, "close": 68800.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"), "open": 68800.0, "high": 69900.0, "low": 67000.0, "close": 67800.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-29T00:00:00Z"), "open": 67800.0, "high": 69200.0, "low": 66800.0, "close": 68600.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-30T00:00:00Z"), "open": 68600.0, "high": 70400.0, "low": 68000.0, "close": 69800.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 69800.0, "high": 71200.0, "low": 69000.0, "close": 70600.0, "volume": 1000.0},
            ]
        )
        bars_1h = pd.DataFrame(
            [
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 66620.0, "high": 66980.0, "low": 66560.0, "close": 66840.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T01:00:00Z"), "open": 66840.0, "high": 67120.0, "low": 66790.0, "close": 67080.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T02:00:00Z"), "open": 67080.0, "high": 67210.0, "low": 66810.0, "close": 66890.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T03:00:00Z"), "open": 66890.0, "high": 67020.0, "low": 66680.0, "close": 66730.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T04:00:00Z"), "open": 66730.0, "high": 66900.0, "low": 66640.0, "close": 66810.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T05:00:00Z"), "open": 66810.0, "high": 66920.0, "low": 66710.0, "close": 66820.0, "volume": 1000.0},
                {"symbol": "BTCUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T06:00:00Z"), "open": 66820.0, "high": 66910.0, "low": 66740.0, "close": 66800.0, "volume": 1000.0},
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=[])

        intraday = frame[(frame["symbol"] == "BTCUSDT") & (frame["horizon"] == "1H")]
        self.assertEqual(len(intraday), 1)
        self.assertEqual(str(intraday.iloc[0]["rebalanceFreq"]), "intraday")
        self.assertFalse(bool(intraday.iloc[0]["actionable"]))
        self.assertIn("no_valid_sr_setup", str(intraday.iloc[0]["rejectionReason"]))
        self.assertEqual(set(frame["horizon"].tolist()), {"1H", "1D"})

    def test_keeps_crypto_intraday_when_daily_signal_has_valid_intraday_sr_structure(self) -> None:
        asset_master = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "name": "Ethereum",
                    "market": "crypto",
                    "timezone": "UTC",
                    "isTradable": True,
                    "hedgeProxy": "ETHUSDT perpetual",
                    "memberships": ["crypto_top50_spot"],
                    "riskBucket": "beta",
                    "primaryVenue": "Binance Spot",
                    "tradableSymbol": "ETHUSDT",
                    "quoteAsset": "USDT",
                    "hasPerpetualProxy": True,
                    "historyCoverageStart": "2021-01-01",
                }
            ]
        )
        as_of = pd.Timestamp.utcnow().date().isoformat()
        forecasts = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "market": "crypto",
                    "universe": "crypto_top50_spot",
                    "horizon": "1H",
                    "pUp": 0.78,
                    "expectedReturn": 0.050,
                    "q10": -0.010,
                    "q50": 0.020,
                    "q90": 0.030,
                    "alphaScore": 1.2,
                    "confidence": 0.8,
                    "indicatorUnavailable": False,
                    "macdLine": 3.0,
                    "macdSignal": 1.5,
                    "macdHist": 1.5,
                    "macdState": "bullish_cross",
                    "rsi14": 53.0,
                    "rsiState": "neutral",
                    "atr14": 1.2,
                    "atrPct": 0.010,
                    "bbUpper": 112.0,
                    "bbMid": 106.0,
                    "bbLower": 100.0,
                    "bbWidth": 0.06,
                    "bbPosition": 0.42,
                    "bbState": "inside_band",
                    "kValue": 64.0,
                    "dValue": 51.0,
                    "jValue": 90.0,
                    "kdjState": "above_signal",
                    "modelVersion": "baseline-ga-mf-v2",
                    "asOfDate": as_of,
                    "signalFrequency": "daily",
                    "sourceFrequency": "daily",
                    "isDerivedSignal": False,
                }
            ]
        )
        rankings = pd.DataFrame(
            [
                {
                    "symbol": "ETHUSDT",
                    "universe": "crypto_top50_spot",
                    "strategyMode": "long_only",
                    "rebalanceFreq": "daily",
                    "modelVersion": "baseline-ga-mf-v2",
                    "rank": 1,
                    "score": 1.5,
                    "targetWeight": 0.12,
                }
            ]
        )
        bars_1d = pd.DataFrame(
            [
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-28T00:00:00Z"), "open": 96.0, "high": 103.0, "low": 95.0, "close": 101.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-29T00:00:00Z"), "open": 101.0, "high": 106.0, "low": 100.0, "close": 104.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-30T00:00:00Z"), "open": 104.0, "high": 108.0, "low": 102.0, "close": 107.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 107.0, "high": 111.0, "low": 105.0, "close": 109.0, "volume": 1000.0},
            ]
        )
        bars_1h = pd.DataFrame(
            [
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 102.4, "high": 104.5, "low": 100.0, "close": 103.2, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T01:00:00Z"), "open": 103.2, "high": 106.0, "low": 101.1, "close": 104.1, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T02:00:00Z"), "open": 104.1, "high": 108.0, "low": 102.2, "close": 105.2, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T03:00:00Z"), "open": 105.2, "high": 109.4, "low": 103.1, "close": 106.0, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T04:00:00Z"), "open": 106.0, "high": 110.6, "low": 104.0, "close": 107.1, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T05:00:00Z"), "open": 107.1, "high": 111.5, "low": 105.0, "close": 108.1, "volume": 1000.0},
                {"symbol": "ETHUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T06:00:00Z"), "open": 101.2, "high": 101.6, "low": 100.2, "close": 100.6, "volume": 1000.0},
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=[])

        self.assertEqual(len(frame), 1)
        row = frame.iloc[0]
        self.assertEqual(str(row["horizon"]), "1H")
        self.assertEqual(str(row["rebalanceFreq"]), "intraday")
        self.assertEqual(str(row["signalFrequency"]), "daily")
        self.assertEqual(str(row["setupType"]), "bounce_long")
        self.assertEqual(str(row["entrySource"]), "support_resistance")
        self.assertEqual(str(row["stopSource"]), "support_resistance")
        self.assertEqual(str(row["targetSource"]), "support_resistance")
        self.assertGreater(float(row["riskRewardRatio"]), 1.5)
        self.assertTrue(bool(row["actionable"]))

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
                    "asOfDate": pd.Timestamp.utcnow().date().isoformat(),
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
                    "asOfDate": pd.Timestamp.utcnow().date().isoformat(),
                },
            ]
        )
        rankings = pd.DataFrame(
            [
                {"symbol": "600519.SH", "universe": "csi300", "strategyMode": "long_only", "rebalanceFreq": "weekly", "modelVersion": "equity-lgbm-ranker-v1"},
                {"symbol": "DOGEUSDT", "universe": "crypto_top50_spot", "strategyMode": "long_only", "rebalanceFreq": "daily", "modelVersion": "crypto-ts-v1"},
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
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T00:00:00Z"), "open": 0.116, "high": 0.124, "low": 0.110, "close": 0.118, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T01:00:00Z"), "open": 0.118, "high": 0.128, "low": 0.112, "close": 0.121, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T02:00:00Z"), "open": 0.121, "high": 0.132, "low": 0.116, "close": 0.125, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T03:00:00Z"), "open": 0.125, "high": 0.135, "low": 0.119, "close": 0.127, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T04:00:00Z"), "open": 0.127, "high": 0.136, "low": 0.121, "close": 0.129, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T05:00:00Z"), "open": 0.129, "high": 0.137, "low": 0.123, "close": 0.130, "volume": 1000.0},
                {"symbol": "DOGEUSDT", "market": "crypto", "timestamp": pd.Timestamp("2026-03-31T06:00:00Z"), "open": 0.120, "high": 0.123, "low": 0.111, "close": 0.114, "volume": 1000.0},
            ]
        )

        frame = build_trade_plan_panel(asset_master, forecasts, rankings, bars_1d, bars_1h, universes=[])

        self.assertTrue(frame[(frame["symbol"] == "600519.SH") & (frame["side"] == "short")].empty)
        crypto_long = frame[(frame["symbol"] == "DOGEUSDT") & (frame["side"] == "long")].iloc[0]
        self.assertFalse(bool(crypto_long["actionable"]))
        self.assertIn("non_tradable_or_missing_perpetual_proxy", str(crypto_long["rejectionReason"]))


if __name__ == "__main__":
    unittest.main()
