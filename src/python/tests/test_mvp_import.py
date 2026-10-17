"""Process01 and shared-runtime regressions through the public experiment flow.

Synthetic Arrow inputs exercise canonical preservation, partial failure/restart,
and single resumable research tracking without comparison executions.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
from mlflow import MlflowClient

from experiment import run_experiment, status
from util.runtime import CODE_ROOT, ExperimentRequest, Repository, open_experiment


def write_source(root: Path, targets: list[list[float]], frequencies: list[str] | None = None) -> None:
    """Write a small external-source fixture with asymmetric histories."""
    from datetime import datetime
    directory = root / "source/m4_daily"
    directory.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("item_id", pa.string()), ("start", pa.timestamp("s")),
                        ("freq", pa.string()), ("target", pa.list_(pa.float32()))])
    table = pa.Table.from_pylist([{"item_id": str(i), "start": datetime(2020, 1, 1),
        "freq": frequencies[i] if frequencies else "D", "target": target}
        for i, target in enumerate(targets)], schema=schema)
    with pa.OSFile(str(directory / "data-00000-of-00001.arrow"), "wb") as sink:
        with ipc.new_stream(sink, schema) as writer:
            writer.write_table(table)


def configuration(root: Path, count: int) -> None:
    """Create an isolated experiment bootstrap; production settings stay unchanged."""
    document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
    document["data"]["source"]["directory"] = str(root / "source")
    document["data"]["selection"]["count"] = count
    document["archived_forecasts"]["enabled"] = []
    document["execution_profiles"] = {"local_dask": {"max_in_flight": 2}}
    (root / "config").mkdir()
    (root / "config/experiment.json").write_text(json.dumps(document))


class MVPImportTests(unittest.TestCase):
    """Public Process01 contract: source values, atomic restart and one MLflow run."""

    def test_archived_import_restart_and_common_forecast_retrieval(self):
        """Imported/calculated methods share windows and survive partial import unchanged."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_source(root, [[float(i) for i in range(n)] for n in (43, 51)])
            configuration(root, 2)
            path = root / "config/experiment.json"
            document = json.loads(path.read_text())
            document["archived_forecasts"]["enabled"] = ["m4_smyl", "m4_fforma"]
            path.write_text(json.dumps(document))
            request = ExperimentRequest((1,), root)

            def archive_response(script, payload, **kwargs):
                records = []
                for job in reversed(payload["jobs"]):
                    model = job["forecast_ids"][0]
                    value = 100 * (job["official_position"] + 1) + (1 if model == "m4_smyl" else 7)
                    records.append({"dataset_id": job["dataset_id"], "series_id": job["series_id"],
                        "forecast_id": model, "horizon": 14, "mean": [float(value + h) for h in range(14)],
                        "median": None, "quantiles": None})
                return {"records": records}

            original_accept = Repository.accept

            def interrupted_accept(store, subflow, results, **kwargs):
                if subflow == "04.01" and store.keys("04.01"):
                    raise RuntimeError("interrupted import")
                return original_accept(store, subflow, results, **kwargs)

            with patch("p01_import.application.run_r", side_effect=archive_response), \
                 patch.object(Repository, "accept", interrupted_accept):
                with self.assertRaisesRegex(RuntimeError, "interrupted import"):
                    run_experiment(request)
            context = open_experiment(ExperimentRequest((4,), root))
            try:
                before = context.repository.connection.execute(
                    "SELECT * FROM atomic_results WHERE subflow_id IN ('01.02','04.01') ORDER BY 1,2").fetchall()
                self.assertEqual(len(context.repository.rows("04.01")), 1)
                self.assertEqual(context.repository.keys("01.04"), context.repository.keys("04.01"))
                # The preserved experiment already accepted Process01 before archives existed.
                context.state.accept_process("01")
                with self.assertRaisesRegex(RuntimeError, "incomplete Process01 archived import prerequisite"):
                    context.forecast.pending_individual_forecasts(None)
            finally:
                context.close()
            with patch("p01_import.application.run_r", side_effect=archive_response) as retrieve:
                run_experiment(request)
                self.assertEqual(len(retrieve.call_args.args[1]["jobs"]), 3)
            with patch("p01_import.application.run_r", side_effect=AssertionError("archive reopened")):
                run_experiment(request)
            context = open_experiment(ExperimentRequest((4,), root))
            try:
                after = context.repository.connection.execute(
                    "SELECT * FROM atomic_results WHERE subflow_id IN ('01.02','04.01') ORDER BY 1,2").fetchall()
                self.assertTrue(all(row in after for row in before))
                canonical = context.repository.rows("01.02")
                for key, source in canonical.items():
                    input_key = f"{key}/official/{source['test_start']:09d}"
                    for model in ("auto_arima", "chronos_2"):
                        context.forecast.accept_individual_forecasts([(f"{input_key}/{model}", {
                            "horizon": 14, "model": model, "input_key": input_key, "mean": [3.0] * 14})])
                    context.windows().accept("03.02", [(input_key, {"kind": "official", "series_key": key,
                        "series_id": source["series_id"], "frequency": "D", "r_period": 1,
                        "point_context": [1.0] * 64, "point_state": {}})])
                    # One normal repository query includes every method for this exact window.
                    rows = {row["model"]: row for row in context.repository.rows("04.01").values()
                            if row["input_key"] == input_key and row["horizon"] == 14}
                    self.assertEqual(set(rows), {"auto_arima", "chronos_2", "m4_smyl", "m4_fforma"})
                    for model, delta in (("m4_smyl", 1), ("m4_fforma", 7)):
                        value = 100 * (source["source_row"] + 1) + delta
                        self.assertEqual(rows[model]["mean"], [float(value + h) for h in range(14)])
                stored = context.repository.connection.execute("SELECT * FROM atomic_results WHERE subflow_id='04.01'").fetchall()
                pending = context.forecast.pending_individual_forecasts(None)
                self.assertFalse({item.payload["model"] for item in pending} &
                                 {"auto_arima", "chronos_2", "m4_smyl", "m4_fforma"})
                self.assertEqual(stored, context.repository.connection.execute(
                    "SELECT * FROM atomic_results WHERE subflow_id='04.01'").fetchall())
                self.assertTrue(all(set(row) == {"subflow_id", "atomic_key"}
                                    for row in context.repository.rows("01.04").values()))
            finally:
                context.close()

    def test_partial_failure_and_database_only_resume(self):
        """A later malformed item preserves earlier work and reuses tracking on resume."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            targets = [[float(i * i + offset) for i in range(35 + offset)] for offset in (0, 3, 5)]
            invalid = [list(t) for t in targets]
            invalid[1][4] = float("inf")
            write_source(root, invalid)
            configuration(root, 3)
            request = ExperimentRequest((1,), root)
            with self.assertRaisesRegex(ValueError, "invalid canonical source"):
                run_experiment(request)
            with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                accepted = connection.execute("SELECT atomic_key,accepted_at FROM atomic_results WHERE subflow_id='01.02'").fetchall()
            self.assertEqual(len(accepted), 1)
            run_id = status(root)["mlflow_run_id"]
            write_source(root, targets)
            (root / "config/experiment.json").unlink()
            run_experiment(request)
            with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                after = connection.execute("SELECT atomic_key,accepted_at FROM atomic_results WHERE subflow_id='01.02'").fetchall()
                values = [json.loads(row[0])["target"] for row in connection.execute(
                    "SELECT result FROM atomic_results WHERE subflow_id='01.02' ORDER BY atomic_key").fetchall()]
            self.assertIn(accepted[0], after)
            self.assertEqual(values, targets)
            run_experiment(request)
            self.assertEqual(status(root)["mlflow_run_id"], run_id)
            self.assertFalse((root / "results/windows.duckdb").exists())
            client = MlflowClient(tracking_uri=f"sqlite:///{root / '.runtime/mlflow/tracking.sqlite'}")
            experiment = client.get_experiment_by_name("ShapeFM")
            runs = client.search_runs([experiment.experiment_id])
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].data.tags["shapefm.last_completed_subflow"], "01.03")
            self.assertEqual(runs[0].data.tags["shapefm.last_completed_process"], "01")
