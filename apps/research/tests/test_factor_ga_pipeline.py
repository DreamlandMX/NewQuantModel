from __future__ import annotations

import shutil
import tempfile
import unittest
import json

import numpy as np
import pandas as pd

from newquantmodel.analytics.backtest import build_backtests
from newquantmodel.cli.main import build_parser
from newquantmodel.analytics.factor_library import build_multifrequency_signal_panel
from newquantmodel.config.settings import AppPaths
from newquantmodel.models.genetic import GAConfig, decode_feature_subset, run_genetic_search
import newquantmodel.models.pipeline as pipeline_module
from newquantmodel.models.pipeline import bootstrap_baseline_outputs, build_ml_overlay
from newquantmodel.storage.parquet_store import read_frame, write_frame


def _synthetic_daily_bars(symbol: str, market: str, periods: int, drift: float, volume_scale: float = 1_000.0) -> pd.DataFrame:
    timestamps = pd.date_range("2025-01-01", periods=periods, freq="D", tz="UTC")
    base = 100.0 + np.cumsum(np.linspace(drift, drift * 1.2, periods))
    wiggle = np.sin(np.arange(periods) / 5.0) * 2.0
    close = base + wiggle
    open_ = close * (1.0 - 0.002)
    high = close * 1.01
    low = close * 0.99
    volume = volume_scale + np.arange(periods) * 5.0
    return pd.DataFrame(
        {
            "symbol": symbol,
            "market": market,
            "timestamp": timestamps,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def _synthetic_hourly_bars(symbol: str, periods: int) -> pd.DataFrame:
    timestamps = pd.date_range("2025-02-01", periods=periods, freq="H", tz="UTC")
    close = 150.0 + np.cumsum(np.sin(np.arange(periods) / 11.0) * 0.8 + 0.15)
    return pd.DataFrame(
        {
            "symbol": symbol,
            "market": "crypto",
            "timestamp": timestamps,
            "open": close * 0.999,
            "high": close * 1.003,
            "low": close * 0.997,
            "close": close,
            "volume": 3_000.0 + np.arange(periods) * 2.0,
        }
    )


class FactorLibraryAndGATest(unittest.TestCase):
    def test_factor_library_builds_expected_columns_and_merges_external(self) -> None:
        bars = pd.concat(
            [
                _synthetic_daily_bars("AAA", "us_equity", 140, 0.18),
                _synthetic_daily_bars("BBB", "us_equity", 140, 0.10),
                _synthetic_daily_bars("BTCUSDT", "crypto", 140, 0.25, volume_scale=5_000.0),
            ],
            ignore_index=True,
        )
        external = pd.DataFrame(
            {
                "market": ["us_equity", "us_equity", "crypto", "crypto"],
                "symbol": ["AAA", "BBB", "BTCUSDT", "BTCUSDT"],
                "timestamp": [bars["timestamp"].iloc[-1], bars["timestamp"].iloc[-1], bars["timestamp"].iloc[-1], bars["timestamp"].iloc[-2]],
                "signalFrequency": ["daily", "daily", "daily", "daily"],
                "macro_regime_score": [0.5, 0.5, 0.2, 0.1],
                "basis_rate": [0.0, 0.0, 0.03, 0.02],
                "funding_rate": [0.0, 0.0, 0.001, 0.001],
                "open_interest_change": [0.0, 0.0, 0.04, 0.03],
                "taker_buy_imbalance": [0.0, 0.0, 0.10, 0.08],
            }
        )
        panel = build_multifrequency_signal_panel(bars, external_factor_panel=external)
        self.assertFalse(panel.empty)
        for column in ["ret_20", "trend_10_50", "realized_vol20", "z_ret_20", "macro_regime_score", "basis_rate", "score"]:
            self.assertIn(column, panel.columns)
        self.assertLess(float(panel["score"].isna().mean()), 0.05)

    def test_genetic_search_is_stable_with_fixed_seed(self) -> None:
        feature_names = ["f1", "f2", "f3", "f4"]

        def evaluator(chromosome: np.ndarray):
            selected = decode_feature_subset(chromosome, feature_names)
            fitness = float(sum(1.0 for item in selected if item in {"f1", "f2"}) - 0.1 * len(selected))
            return fitness, {"fitness": fitness}, {"selected": selected}

        result_a = run_genetic_search(dimensions=len(feature_names), evaluator=evaluator, config=GAConfig(population=12, generations=10, seed=7, patience=4))
        result_b = run_genetic_search(dimensions=len(feature_names), evaluator=evaluator, config=GAConfig(population=12, generations=10, seed=7, patience=4))
        self.assertEqual(result_a.payload["selected"], result_b.payload["selected"])
        self.assertGreaterEqual(result_a.fitness, 1.0)

    def test_frame_signature_includes_data_signature(self) -> None:
        frame = pd.DataFrame(
            {
                "symbol": ["AAA", "BBB"],
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True),
                "score": [0.1, 0.2],
            }
        )

        signature = pipeline_module._frame_signature(frame, ["score"])

        self.assertIn("dataSignature", signature)
        self.assertGreater(len(str(signature["dataSignature"])), 8)
        self.assertEqual(signature["rows"], 2)

    def test_split_optimization_and_holdout_reserves_recent_tail(self) -> None:
        frame = pd.DataFrame(
            {
                "symbol": ["AAA"] * 24,
                "timestamp": pd.date_range("2025-01-01", periods=24, freq="D", tz="UTC"),
                "score": np.linspace(0.0, 1.0, 24),
            }
        )

        optimization, holdout = pipeline_module._split_optimization_and_holdout(frame, "daily")

        self.assertFalse(optimization.empty)
        self.assertFalse(holdout.empty)
        self.assertLess(optimization["timestamp"].max(), holdout["timestamp"].min())

    def test_backtest_cost_stress_recomputes_portfolio_metrics(self) -> None:
        signal_panel = pd.DataFrame(
            {
                "market": ["crypto"] * 4,
                "symbol": ["AAA", "BBB", "AAA", "BBB"],
                "timestamp": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"], utc=True),
                "score": [1.0, 0.5, 0.2, 1.1],
                "predictedReturn": [0.1, 0.05, 0.03, 0.12],
                "modelVersion": ["test-model"] * 4,
                "signalFrequency": ["daily"] * 4,
                "sourceFrequency": ["daily"] * 4,
                "isDerivedSignal": [False] * 4,
            }
        )
        bars_1d = pd.DataFrame(
            {
                "market": ["crypto"] * 6,
                "symbol": ["AAA", "AAA", "AAA", "BBB", "BBB", "BBB"],
                "timestamp": pd.to_datetime(
                    ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-01", "2025-01-02", "2025-01-03"],
                    utc=True,
                ),
                "close": [100.0, 110.0, 121.0, 100.0, 90.0, 99.0],
            }
        )

        backtests = build_backtests(signal_panel, bars_1d)
        row = backtests.loc[(backtests["strategyId"] == "crypto-long_only-daily")].iloc[0]
        cost_stress = row["costStress"]

        self.assertEqual(cost_stress[0]["label"], "base")
        self.assertEqual(cost_stress[1]["label"], "+10bps")
        self.assertNotAlmostEqual(float(cost_stress[1]["cagr"]), float(row["cagr"]) - 0.01, places=9)

    def test_equity_forecast_uses_model_sigma_for_intervals(self) -> None:
        latest = pd.DataFrame(
            {
                "symbol": ["AAA"],
                "timestamp": [pd.Timestamp("2025-02-01T00:00:00Z")],
                "score": [1.2],
                "predictedReturn": [0.04],
                "pred_1D": [0.04],
                "pred_5D": [0.08],
                "pred_20D": [0.12],
                "pUp_1D": [0.73],
                "sigma_1D": [0.02],
                "sigma_5D": [0.03],
                "sigma_20D": [0.05],
                "volume": [1_000.0],
                "z_mom20": [0.9],
                "ret_1d": [0.01],
            }
        )
        memberships = pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "name": "AAA",
                    "universe": "sp500",
                    "market": "us_equity",
                    "effective_from": pd.Timestamp("2025-01-01"),
                    "effective_to": pd.NaT,
                    "coverage_mode": "point_in_time",
                    "data_source": "test",
                }
            ]
        )

        _rankings, forecasts = pipeline_module._build_equity_rankings_and_forecasts(
            latest,
            memberships,
            "us_equity",
            {"z_mom20": 1.0},
            "daily",
            {"1D": 1, "5D": 5, "20D": 20},
        )

        one_day = forecasts.loc[forecasts["horizon"] == "1D"].iloc[0]
        self.assertAlmostEqual(float(one_day["pUp"]), 0.73, places=6)
        self.assertAlmostEqual(float(one_day["q10"]), 0.04 - (1.2816 * 0.02), places=6)
        self.assertAlmostEqual(float(one_day["q90"]), 0.04 + (1.2816 * 0.02), places=6)


class PipelineGATest(unittest.TestCase):
    def test_build_ml_cli_accepts_market_frequency_and_pipeline_filters(self) -> None:
        parser = build_parser()

        args = parser.parse_args(
            [
                "build-ml-signals",
                "--root",
                ".",
                "--market",
                "crypto",
                "--signal-frequency",
                "weekly",
                "--pipeline",
                "crypto",
                "--fast",
            ]
        )

        self.assertEqual(args.market, "crypto")
        self.assertEqual(args.signal_frequency, "weekly")
        self.assertEqual(args.pipeline, "crypto")
        self.assertTrue(args.fast)

    def test_baseline_and_ml_pipeline_emit_ga_artifacts(self) -> None:
        tempdir = tempfile.mkdtemp(prefix="newquantmodel-ga-")
        self.addCleanup(lambda: shutil.rmtree(tempdir, ignore_errors=True))
        paths = AppPaths.from_root(tempdir)
        original_config = pipeline_module.GENETIC_CONFIG
        original_freqs = list(pipeline_module.SIGNAL_FREQUENCIES)
        original_optimizer = pipeline_module._optimize_ml_feature_subset
        pipeline_module.GENETIC_CONFIG = GAConfig(population=8, generations=6, patience=3, seed=7)
        pipeline_module.SIGNAL_FREQUENCIES = ["daily"]
        pipeline_module._optimize_ml_feature_subset = lambda frame, market, signal_frequency, evaluator_builder, **kwargs: (
            [column for column in pipeline_module._feature_candidates_for_market(market, signal_frequency) if column in frame.columns][:6],
            {"n_estimators": 80, "learning_rate": 0.05, "num_leaves": 24, "min_child_samples": 12, "subsample": 0.8, "colsample_bytree": 0.8},
            '{"fitness": 0.0, "selected": []}',
            {"fitness": 0.0},
            str(pd.Timestamp(frame["timestamp"].max()).isoformat()) if not frame.empty else None,
            False,
            "test-signature",
        )
        self.addCleanup(lambda: setattr(pipeline_module, "GENETIC_CONFIG", original_config))
        self.addCleanup(lambda: setattr(pipeline_module, "SIGNAL_FREQUENCIES", original_freqs))
        self.addCleanup(lambda: setattr(pipeline_module, "_optimize_ml_feature_subset", original_optimizer))

        bars_1d = pd.concat(
            [
                _synthetic_daily_bars("AAA", "us_equity", 100, 0.18),
                _synthetic_daily_bars("BBB", "us_equity", 100, 0.12),
                _synthetic_daily_bars("CCC", "us_equity", 100, 0.08),
                _synthetic_daily_bars("BTCUSDT", "crypto", 100, 0.24, volume_scale=5_000.0),
                _synthetic_daily_bars("ETHUSDT", "crypto", 100, 0.18, volume_scale=4_000.0),
                _synthetic_daily_bars("SOLUSDT", "crypto", 100, 0.15, volume_scale=3_500.0),
                _synthetic_daily_bars("^GSPC", "index", 100, 0.05, volume_scale=1_000.0),
                _synthetic_daily_bars("^NDX", "index", 100, 0.07, volume_scale=1_000.0),
            ],
            ignore_index=True,
        )
        bars_1h = pd.concat(
            [
                _synthetic_hourly_bars("BTCUSDT", 360),
                _synthetic_hourly_bars("ETHUSDT", 360),
                _synthetic_hourly_bars("SOLUSDT", 360),
            ],
            ignore_index=True,
        )
        memberships = pd.DataFrame(
            [
                {"symbol": "AAA", "name": "AAA", "universe": "sp500", "market": "us_equity", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "point_in_time", "data_source": "test"},
                {"symbol": "BBB", "name": "BBB", "universe": "sp500", "market": "us_equity", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "point_in_time", "data_source": "test"},
                {"symbol": "CCC", "name": "CCC", "universe": "sp500", "market": "us_equity", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "point_in_time", "data_source": "test"},
                {"symbol": "BTCUSDT", "name": "BTC", "universe": "crypto_top50_spot", "market": "crypto", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "approx_bootstrap", "data_source": "test"},
                {"symbol": "ETHUSDT", "name": "ETH", "universe": "crypto_top50_spot", "market": "crypto", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "approx_bootstrap", "data_source": "test"},
                {"symbol": "SOLUSDT", "name": "SOL", "universe": "crypto_top50_spot", "market": "crypto", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "approx_bootstrap", "data_source": "test"},
                {"symbol": "^GSPC", "name": "SP500", "universe": "sp500_index", "market": "index", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "point_in_time", "data_source": "test"},
                {"symbol": "^NDX", "name": "NDX", "universe": "nasdaq100_index", "market": "index", "effective_from": pd.Timestamp("2025-01-01"), "effective_to": pd.NaT, "coverage_mode": "point_in_time", "data_source": "test"},
            ]
        )
        signal_panel = build_multifrequency_signal_panel(bars_1d)

        write_frame(paths, "bars_1d", bars_1d)
        write_frame(paths, "bars_1h", bars_1h)
        write_frame(paths, "signal_panel", signal_panel)
        write_frame(paths, "universe_membership", memberships)

        bootstrap_baseline_outputs(paths)
        baseline_rankings = read_frame(paths, "baseline_ranking_panel")
        ga_runs = read_frame(paths, "ga_run_panel")
        self.assertFalse(baseline_rankings.empty)
        self.assertTrue((baseline_rankings["signalFamily"] == "ga_baseline_multifactor").any())
        self.assertFalse(ga_runs.empty)
        self.assertIn("baseline-ga", set(ga_runs["pipeline"].astype(str)))

        build_ml_overlay(paths)
        model_runs = read_frame(paths, "model_run_panel")
        ga_runs = read_frame(paths, "ga_run_panel")
        self.assertFalse(model_runs.empty)
        self.assertTrue(model_runs["metricSummary"].astype(str).str.contains('"ga"').any())
        self.assertTrue(ga_runs["pipeline"].astype(str).str.contains("ml-ga").any())

    def test_ga_pipeline_records_data_signature_and_holdout_metrics(self) -> None:
        tempdir = tempfile.mkdtemp(prefix="newquantmodel-ga-signature-")
        self.addCleanup(lambda: shutil.rmtree(tempdir, ignore_errors=True))
        paths = AppPaths.from_root(tempdir)
        signal_panel = pd.DataFrame(
            {
                "market": ["us_equity"] * 36,
                "symbol": ["AAA"] * 18 + ["BBB"] * 18,
                "timestamp": list(pd.date_range("2025-01-01", periods=18, freq="D", tz="UTC")) * 2,
                "signalFrequency": ["daily"] * 36,
                "sourceFrequency": ["daily"] * 36,
                "isDerivedSignal": [False] * 36,
                "close": np.linspace(100.0, 118.0, 36),
                "volume": np.linspace(1_000.0, 2_000.0, 36),
                "ret_1d": np.linspace(0.01, 0.03, 36),
                "score": np.linspace(-1.0, 1.0, 36),
                "z_mom20": np.linspace(-0.5, 0.5, 36),
            }
        )
        memberships = pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "name": "AAA",
                    "universe": "sp500",
                    "market": "us_equity",
                    "effective_from": pd.Timestamp("2025-01-01"),
                    "effective_to": pd.NaT,
                    "coverage_mode": "point_in_time",
                    "data_source": "test",
                },
                {
                    "symbol": "BBB",
                    "name": "BBB",
                    "universe": "sp500",
                    "market": "us_equity",
                    "effective_from": pd.Timestamp("2025-01-01"),
                    "effective_to": pd.NaT,
                    "coverage_mode": "point_in_time",
                    "data_source": "test",
                },
            ]
        )
        write_frame(paths, "signal_panel", signal_panel)
        write_frame(paths, "universe_membership", memberships)

        baseline_rankings = pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "universe": "sp500",
                    "rebalanceFreq": "daily",
                    "strategyMode": "long_only",
                    "score": 1.0,
                    "rank": 1,
                    "expectedReturn": 0.02,
                    "targetWeight": 1.0,
                    "liquidityBucket": "high",
                    "factorExposures": {"momentum": 1.0},
                    "signalFamily": "baseline",
                    "signalBreakdown": {"momentum": 1.0},
                    "asOfDate": "2025-01-18",
                    "modelVersion": "baseline",
                    "signalFrequency": "daily",
                    "sourceFrequency": "daily",
                    "isDerivedSignal": False,
                }
            ]
        )
        write_frame(paths, "baseline_ranking_panel", baseline_rankings)
        write_frame(
            paths,
            "baseline_forecast_panel",
            pd.DataFrame(
                columns=[
                    "symbol",
                    "market",
                    "universe",
                    "horizon",
                    "pUp",
                    "expectedReturn",
                    "q10",
                    "q50",
                    "q90",
                    "alphaScore",
                    "confidence",
                    "regime",
                    "riskFlags",
                    "modelVersion",
                    "asOfDate",
                    "signalFrequency",
                    "sourceFrequency",
                    "isDerivedSignal",
                ]
            ),
        )

        original_optimizer = pipeline_module._optimize_ml_feature_subset
        pipeline_module._optimize_ml_feature_subset = lambda frame, market, signal_frequency, evaluator_builder, **kwargs: (
            ([column for column in pipeline_module._feature_candidates_for_market(market, signal_frequency) if column in frame.columns][:1] or ["z_mom20"]),
            {"n_estimators": 80},
            json.dumps({"fitness": 0.2, "holdout": {"fitness": 0.1}, "selected": ["z_mom20"]}),
            {"fitness": 0.2},
            "sig-123",
            False,
            "sig-123",
        )
        self.addCleanup(lambda: setattr(pipeline_module, "_optimize_ml_feature_subset", original_optimizer))

        build_ml_overlay(paths, market_filter="us_equity", signal_frequency_filter="daily", pipeline_filter="equity")
        ga_runs = read_frame(paths, "ga_run_panel")

        self.assertIn("dataSignature", ga_runs.columns)
        self.assertTrue(ga_runs["dataSignature"].astype(str).str.len().gt(3).all())
        self.assertTrue(ga_runs["metricSummary"].astype(str).str.contains('"holdout"').any())


if __name__ == "__main__":
    unittest.main()
