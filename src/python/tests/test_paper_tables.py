"""Independent scientific selection, directional rank and M4 aggregation regressions."""

import unittest

from p06_evaluate.reports import M4_FREQUENCIES, TableReports


class PaperTableTests(unittest.TestCase):
    """Exercise public report calculations without legacy storage or orchestration."""

    def test_all_selection_tie_levels(self):
        """No hardcoded historical pair or coarse/tolerance tie can pass these orderings."""
        def row(owa, up, down):
            """Return one selection-only candidate, independent of implementation output."""
            return {"metrics": {"owa": owa}, "lambda_up": up, "lambda_down": down}
        cases = [
            ([row(.9, 1.12, .9), row(1, 1, 1)], (.9, 1.12, .9)),
            ([row(1, 1.1, .9), row(1, 1.01, 1)], (1, 1.01, 1)),
            ([row(1, 1.125, 1), row(1, 1, .875)], (1, 1, .875)),
            # Symmetric upward departures force stable lambda ordering.
            ([row(1, 1.125, .875), row(1, .875, .875)], (1, .875, .875)),
            ([row(1, 1.125, 1.125), row(1, 1.125, .875)], (1, 1.125, .875)),
        ]
        for rows, expected in cases:
            result = TableReports.select(rows[::-1])
            self.assertEqual((result["metrics"]["owa"], result["lambda_up"], result["lambda_down"]), expected)

    def test_ranks_ties_incomplete_and_duplicate_models(self):
        """Higher accuracy is better; average ties and missing-model reports are explicit."""
        rows = []
        for h, scores in ((1, (4, 4, 2)), (2, (2, 4, 2))):
            for model, correct in zip(("a", "b", "c"), scores):
                rows.append({"frequency": "Daily", "model": model, "horizon": h,
                             "correct_count": correct, "evaluation_count": 4})
        result = TableReports.directional_profile(rows, ["a", "b", "c"], {"Daily": [1, 2]})
        self.assertEqual(result["mean_ranks"], {"a": 2, "b": 1.25, "c": 2.75})
        self.assertEqual([row["accuracy"] for row in result["matrix"]], [1, 1, .5, .5, 1, .5])
        incomplete = TableReports.directional_profile(rows[:-1], ["a", "b", "c"], {"Daily": [1, 2]})
        self.assertIsNone(incomplete["mean_ranks"])
        self.assertEqual(incomplete["incomplete_models"], {"c": [("Daily", 2)]})
        self.assertEqual(incomplete["matrix"], result["matrix"][:-1])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            TableReports.directional_profile(rows + rows[:1], ["a", "b", "c"], {"Daily": [1, 2]})

    def test_complete_all_recalculates_owa_and_no_daily_all(self):
        """Weighted components, not weighted OWAs, establish the synthetic POC3 boundary."""
        rows = []
        for i, frequency in enumerate(M4_FREQUENCIES, 1):
            for model, factor in (("naive2", 1), ("m4_smyl", 2)):
                rows.append({"frequency": frequency, "model": model, "evaluation_count": i,
                             "metrics": {"smape": i * factor, "mase": (7 - i) / factor,
                                         "da": i / 10, "owa": 99}})
        with self.assertRaisesRegex(ValueError, "complete"):
            TableReports.aggregate_all([r for r in rows if r["frequency"] == "Daily"])
        aggregate = TableReports.aggregate_all(rows)
        smyl = next(r for r in aggregate if r["model"] == "m4_smyl")
        self.assertEqual(smyl["metrics"]["owa"], 1.25)
        self.assertEqual(smyl["metrics"]["smape"], 2 * 91 / 21)
        self.assertEqual(smyl["metrics"]["mase"], 56 / 42)
        self.assertAlmostEqual(smyl["metrics"]["da"], 91 / 210)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            TableReports.aggregate_all(rows + rows[:1])


if __name__ == "__main__":
    unittest.main()
