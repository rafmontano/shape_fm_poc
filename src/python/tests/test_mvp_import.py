"""Process01 and shared-runtime regressions through the public experiment flow.

Synthetic Arrow inputs exercise canonical preservation, partial failure/restart,
single resumable research tracking and sequential/Dask execution parity.
"""

import json
import tempfile
import unittest
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
from mlflow import MlflowClient

from experiment import run_experiment, status
from util.runtime import CODE_ROOT, ExperimentRequest


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
    document["execution_profiles"] = {"local_dask": {"max_in_flight": 2}}
    (root / "config").mkdir()
    (root / "config/experiment.json").write_text(json.dumps(document))


class MVPImportTests(unittest.TestCase):
    """Public Process01 contract: source values, atomic restart and one MLflow run."""

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

    def test_same_science_sequential_and_dask(self):
        """Worker placement changes neither canonical values nor atomic identities."""
        targets = [[float(i + offset * i * i) for i in range(36)] for offset in (2, 5, 7)]
        outputs = []
        for profile in ("sequential_safe", "local_dask"):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                write_source(root, [targets[0], targets[1], targets[0], targets[2]], ["D", "W", "D", "D"])
                configuration(root, 3)
                run_experiment(ExperimentRequest((1,), root, execution_profile=profile))
                with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                    outputs.append([(key, json.loads(value)["target"]) for key, value in connection.execute(
                        "SELECT atomic_key,result FROM atomic_results WHERE subflow_id='01.02' ORDER BY atomic_key").fetchall()])
                self.assertEqual([value for _, value in outputs[-1]], [targets[0], targets[0], targets[2]])
        self.assertEqual(outputs[0], outputs[1])
