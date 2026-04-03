from __future__ import annotations

import unittest

from newquantmodel.models.pipeline import _cohere_index_forecast


class IndexForecastCoherenceTest(unittest.TestCase):
    def test_flips_bullish_probability_when_quantiles_are_bearish(self) -> None:
        result = _cohere_index_forecast(0.60, -0.016, 0.01)
        self.assertEqual(result["forecastValidity"], "adjusted")
        self.assertTrue(result["forecastAdjusted"])
        self.assertLess(float(result["pUp"]), 0.45)
        self.assertLess(float(result["q50"]), 0.0)

    def test_marks_mixed_quantiles_as_conflict(self) -> None:
        result = _cohere_index_forecast(0.58, 0.0, 0.01)
        self.assertEqual(result["forecastValidity"], "conflict")
        self.assertEqual(result["forecastConflictReason"], "direction_quantile_mismatch")
        self.assertAlmostEqual(float(result["pUp"]), 0.50, places=6)
