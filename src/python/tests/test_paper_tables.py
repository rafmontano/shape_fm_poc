"""Independent scientific selection, directional rank and M4 aggregation regressions."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from p06_evaluate.application import Application
from p06_evaluate.reports import M4_FREQUENCIES, POINT_MODELS, TableReports
from util.runtime import WorkItem


class PaperTableTests(unittest.TestCase):
    """Exercise public report calculations without legacy storage or orchestration."""

    @patch("p06_evaluate.application.run_r")
    def test_sensitivity_scores_record_raw_basis(self, run_r):
        """The unchanged paper kernel's counts retain their raw-origin provenance."""
        job = {"model": "directional_mantis_rf", "lambda_up": None, "lambda_down": None}
        run_r.return_value = {"results": [{**job, "correct_counts": [0] * 14,
            "evaluation_count": 1, "metrics": {"smape": None, "mase": None, "da": 0}}]}
        result = Application.calculate_diagnostic_batch([WorkItem("candidate", {"job": job})],
            series=[{"context": [10], "actual": [5] * 14}])[0][1]
        self.assertEqual(result["evaluation_basis"], "raw_origin")
        self.assertEqual(result["correct_counts"], [0] * 14)

    def test_prepared_classifier_figures_and_raw_tables_use_same_predictions(self):
        """Raw endpoint 10 and prepared endpoint 0 give opposite actual labels."""
        store = Mock()
        windows = Mock()
        windows.rows.return_value = {"official": {"series_key": "s", "series_id": "s",
            "frequency": "Daily", "labels": [1] * 14}}
        direct = ("directional_dtw", "directional_mantis_rf")
        stored = {
            "01.02": {"s": {"target": [10] + [5] * 14, "test_start": 1, "start": "2000-01-01"}},
            "04.01": {"point": {"input_key": "official", "model": "m4_smyl", "mean": [5] * 14}},
            "05.01": {}, "05.02": {},
            "04.04": {m: {"input_key": "official", "model": m, "labels": [1] * 14} for m in direct}}
        store.rows.side_effect = lambda stage: stored[stage]
        store.plan.side_effect = lambda stage, items: items
        context = SimpleNamespace(repository=store, windows=lambda: windows,
            configuration={"evaluation": {"table_reproduction": {"figure_2": {
                "horizon_models": ["m4_smyl", *direct], "cd_models": ["m4_smyl", *direct]}}}},
            ref=lambda stage: stage)
        app = Application(context)
        items = app.pending_directional_evaluations(None)
        scores = {item.key: app.calculate_directional_evaluation(item) for item in items}
        self.assertIn("labels", windows.rows.call_args.kwargs["fields"])
        for score in scores.values():
            self.assertEqual(score["correct_counts"], [1] * 14)
            self.assertEqual(score["evaluation_basis"],
                "prepared_origin" if score["model"] in direct else "raw_origin")
        # Two accepted series prove aggregation, not selection of a single score.
        stored["06.02"] = scores | {"second/" + m: {**scores["official/" + m],
            "correct_counts": [0] * 14} for m in direct}
        selected = {m: {"frequency": "Daily", "correct_counts": [0 if m in direct else 1] * 14,
            "evaluation_count": 1, "evaluation_basis": "raw_origin",
            "metrics": {"smape": 1., "mase": 1., "owa": 1.}}
            for m in (*POINT_MODELS, *direct)}
        stored["selected_settings"] = {"selection": selected}
        app.assemble_research_results(None)
        report = store.accept.call_args.args[1][0][1]
        mantis = next(r for r in report["Table2"]["A"] if r["model"] == direct[1])
        self.assertEqual(mantis["value"], 0)
        for row in report["figure_2"]["horizon"]["matrix"]:
            self.assertEqual(row["accuracy"], .5 if row["model"] in direct else 1)
        self.assertEqual(report["figure_2"]["cd"]["mean_ranks"],
            {"m4_smyl": 1, direct[0]: 2.5, direct[1]: 2.5})
        self.assertEqual(report["evaluation_provenance"]["tables"], "raw_origin")
        self.assertEqual(report["evaluation_provenance"]["figure_2"][direct[1]], "prepared_origin")
        stored["06.02"] = {}
        app.assemble_research_results(None)
        missing = store.accept.call_args.args[1][0][1]["figure_2"]["cd"]
        self.assertIsNone(missing["mean_ranks"])
        self.assertEqual(set(missing["incomplete_models"]), set(direct))
        stored["06.02"] = {"old": {**scores["official/" + direct[0]], "evaluation_basis": "raw_origin"}}
        with self.assertRaisesRegex(ValueError, "prepared-origin"):
            app.assemble_research_results(None)

    def test_corrected_reference_changes_selection_using_retained_errors(self):
        """Unequal reference changes can reverse OWA order without new scoring."""
        rows = [{"lambda_up": 1.01, "lambda_down": 1., "correct_counts": [7, 4],
                 "evaluation_count": 9, "metrics": {"smape": 2., "mase": 8., "owa": .5}},
                {"lambda_up": 1.02, "lambda_down": .99, "correct_counts": [3, 6],
                 "evaluation_count": 9, "metrics": {"smape": 6., "mase": 2., "owa": .4}}]
        self.assertIs(TableReports.select(rows), rows[1])
        corrected = TableReports.rebase_owa(rows, {"metrics": {"smape": 4., "mase": 10.}})
        self.assertAlmostEqual(corrected[0]["metrics"]["owa"], .65)
        self.assertAlmostEqual(corrected[1]["metrics"]["owa"], .85)
        self.assertIs(TableReports.select(corrected), corrected[0])
        for old, new in zip(rows, corrected):
            self.assertEqual({**new, "metrics": {**new["metrics"], "owa": old["metrics"]["owa"]}}, old)
        self.assertEqual([r["metrics"]["owa"] for r in rows], [.5, .4])

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
