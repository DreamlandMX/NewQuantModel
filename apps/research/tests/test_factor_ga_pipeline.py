from __future__ import annotations

import shutil
import tempfile
import unittest

import numpy as np
import pandas as pd

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


class PipelineGATest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
