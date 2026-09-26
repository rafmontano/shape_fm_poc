from __future__ import annotations

import ast
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shapefm.calibration import (
    CALIBRATION_CANDIDATES,
    _chronos_context_responses,
    _chronos_differences,
    _chronos_reference_forecasts,
    _recommended_setting,
    _scientific_comparison,
)
from shapefm.execution import (
    ExecutionSettings,
    PersistentChronosWorker,
    resolve_execution_profile,
    system_hardware,
)
from shapefm.poc1 import _length_aware_batches, expected_task_counts


class ExecutionProfileTests(unittest.TestCase):
    def test_committed_profile_values_and_single_writer(self) -> None:
        sequential, _ = resolve_execution_profile("sequential_safe")
        mac, _ = resolve_execution_profile("mac_m1pro_10core_16gb")
        ubuntu, _ = resolve_execution_profile("ubuntu_3950x_16core_128gb_rtx5090")
        self.assertEqual(
            (
                sequential.cleaning_workers,
                sequential.transformation_workers,
                sequential.autoarima_workers,
                sequential.chronos_processes,
                sequential.chronos_inference_batch_size,
                sequential.combination_workers,
                sequential.evaluation_workers,
                sequential.cpu_gpu_overlap,
            ),
            (1, 1, 1, 1, 1, 1, 1, False),
        )
        self.assertEqual(
            (
                mac.cleaning_workers,
                mac.transformation_workers,
                mac.autoarima_workers,
                mac.chronos_inference_batch_size,
                mac.combination_workers,
                mac.evaluation_workers,
                mac.cpu_gpu_overlap,
                mac.required_accelerator,
            ),
            (2, 4, 2, 8, 4, 1, False, "mps"),
        )
        self.assertEqual(
            (
                ubuntu.cleaning_workers,
                ubuntu.transformation_workers,
                ubuntu.autoarima_workers,
                ubuntu.chronos_inference_batch_size,
                ubuntu.combination_workers,
                ubuntu.evaluation_workers,
                ubuntu.cpu_gpu_overlap,
                ubuntu.required_accelerator,
            ),
            (8, 16, 12, 16, 16, 1, True, "cuda"),
        )
        self.assertTrue(all(p.database_writers == 1 for p in (sequential, mac, ubuntu)))
        self.assertTrue(all(p.chronos_processes == 1 for p in (mac, ubuntu)))

    def test_hardware_provenance_records_cpu_model(self) -> None:
        with patch("shapefm.execution.cpu_model", return_value="Test CPU"):
            self.assertEqual(system_hardware()["cpu_model"], "Test CPU")

    def test_execution_settings_are_invocation_only_and_validated(self) -> None:
        settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address="tcp://scheduler:8786",
            dask_expected_workers=7,
            dask_max_in_flight=12,
            dask_retries=2,
        )
        self.assertEqual(settings.mode, "dask")
        self.assertEqual(settings.dask_expected_workers, 7)
        with self.assertRaisesRegex(ValueError, "execution mode"):
            ExecutionSettings(mode="remote")

    def test_full_m4_daily_task_counts(self) -> None:
        counts = expected_task_counts(4_227)
        self.assertEqual(
            counts,
            {2: 8_454, 3: 16_908, 4: 33_816, 5: 50_724, 6: 12},
        )
        self.assertEqual(sum(counts.values()), 109_914)

    def test_unknown_and_invalid_overrides_fail(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown execution profile"):
            resolve_execution_profile("not-a-profile")
        with self.assertRaisesRegex(ValueError, "exactly one database writer"):
            resolve_execution_profile("sequential_safe", {"database_writers": 2})
        with self.assertRaisesRegex(ValueError, "must be positive"):
            resolve_execution_profile("sequential_safe", {"cleaning_workers": 0})
        with self.assertRaisesRegex(ValueError, "one Chronos process"):
            resolve_execution_profile("mac_m1pro_10core_16gb", {"chronos_processes": 2})
        with self.assertRaisesRegex(ValueError, "accelerator cannot be overridden"):
            resolve_execution_profile(
                "mac_m1pro_10core_16gb", {"required_accelerator": "cpu"}
            )

    def test_length_aware_batches_are_bounded_and_do_not_mix_ranges(self) -> None:
        jobs = [
            {"id": "long", "context": [0] * 1025},
            {"id": "short-b", "context": [0] * 12},
            {"id": "medium", "context": [0] * 130},
            {"id": "short-a", "context": [0] * 9},
            {"id": "short-c", "context": [0] * 15},
        ]
        batches = _length_aware_batches(jobs, 2)
        self.assertTrue(all(len(batch) <= 2 for batch in batches))
        self.assertEqual(sum((batch for batch in batches), []), [
            jobs[3], jobs[1], jobs[4], jobs[2], jobs[0]
        ])
        for batch in batches:
            self.assertEqual(len({max(1, len(job["context"])).bit_length() for job in batch}), 1)


class PersistentWorkerTests(unittest.TestCase):
    def test_multiple_batches_use_exactly_one_model_load(self) -> None:
        script = """\
import json, sys
loads = 1
print(json.dumps({'type':'ready','model_load_count':loads}), flush=True)
for line in sys.stdin:
    message = json.loads(line)
    if message.get('command') == 'shutdown':
        break
    print(json.dumps({'type':'result','batch_id':message['batch_id'],'load_count':loads}), flush=True)
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fake_worker.py"
            path.write_text(script, encoding="utf-8")
            with PersistentChronosWorker([sys.executable, str(path)], startup_timeout=5) as worker:
                process_id = worker.process.pid
                first = worker.request({"command": "predict", "batch_id": "one"}, timeout=5)
                second = worker.request({"command": "predict", "batch_id": "two"}, timeout=5)
                self.assertEqual(worker.process.pid, process_id)
                self.assertEqual(worker.ready["model_load_count"], 1)
                self.assertEqual(first["load_count"], 1)
                self.assertEqual(second["load_count"], 1)

    def test_large_stderr_is_continuously_drained_and_bounded(self) -> None:
        script = """\
import json, sys
sys.stderr.write('startup-' + ('x' * 131072) + '-startup-tail\\n')
sys.stderr.flush()
print(json.dumps({'type':'ready','model_load_count':1}), flush=True)
for line in sys.stdin:
    message = json.loads(line)
    if message.get('command') == 'shutdown':
        break
    sys.stderr.write(message['batch_id'] + '-' + ('y' * 131072) + '-batch-tail\\n')
    sys.stderr.flush()
    print(json.dumps({'type':'result','batch_id':message['batch_id']}), flush=True)
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "noisy_worker.py"
            path.write_text(script, encoding="utf-8")
            worker = PersistentChronosWorker(
                [sys.executable, str(path)],
                startup_timeout=5,
                stderr_tail_bytes=4096,
            )
            with worker:
                self.assertEqual(
                    worker.request(
                        {"command": "predict", "batch_id": "first"}, timeout=5
                    )["batch_id"],
                    "first",
                )
                self.assertEqual(
                    worker.request(
                        {"command": "predict", "batch_id": "second"}, timeout=5
                    )["batch_id"],
                    "second",
                )
            self.assertLessEqual(len(worker.stderr_tail.encode()), 4096)
            self.assertIn("batch-tail", worker.stderr_tail)

    def test_worker_source_has_no_duckdb_access(self) -> None:
        source = (Path(__file__).parents[1] / "src/shapefm/chronos_worker.py").read_text(
            encoding="utf-8"
        )
        imports = [
            node.names[0].name
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Import)
        ]
        imports.extend(
            node.module or ""
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom)
        )
        self.assertNotIn("duckdb", imports)

        dask_source = (
            Path(__file__).parents[1] / "src/shapefm/dask_execution.py"
        ).read_text(encoding="utf-8")
        dask_imports = [
            node.names[0].name
            for node in ast.walk(ast.parse(dask_source))
            if isinstance(node, ast.Import)
        ]
        dask_imports.extend(
            node.module or ""
            for node in ast.walk(ast.parse(dask_source))
            if isinstance(node, ast.ImportFrom)
        )
        self.assertNotIn("duckdb", dask_imports)

    def test_local_dask_batch_matches_sequential_transform(self) -> None:
        from distributed import Client, LocalCluster

        from shapefm.dask_execution import run_batches, transform_batch
        from shapefm.transformations import transform

        jobs = [
            {
                "id": f"task-{index}",
                "values": [float(index), float(index + 2), float(index - 1)],
                "method": "minmax_then_standardize",
            }
            for index in range(5)
        ]
        cluster = LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            processes=False,
            dashboard_address=None,
            resources={"CPU": 1},
        )
        try:
            with Client(cluster) as client:
                returned = []
                for _, response in run_batches(
                    client,
                    transform_batch,
                    [jobs[:3], jobs[3:]],
                    resources={"CPU": 1},
                    max_in_flight=1,
                    retries=1,
                ):
                    returned.extend(response["results"])
                    self.assertEqual(response["worker"]["resources"], {"CPU": 1})
            expected = [
                transform(job["values"], job["method"]) for job in jobs
            ]
            self.assertEqual(
                [tuple(result["values"]) for result in returned],
                [result.values for result in expected],
            )
        finally:
            cluster.close()

    def test_dask_batch_retry_count_is_explicit(self) -> None:
        from distributed import Client, LocalCluster

        from shapefm.dask_execution import run_batches

        def succeed_on_retry(batch, retry_count=0):
            if retry_count == 0:
                raise RuntimeError("first attempt fails")
            return {"ids": [job["id"] for job in batch], "retry_count": retry_count}

        cluster = LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            processes=False,
            dashboard_address=None,
            resources={"CPU": 1},
        )
        try:
            with Client(cluster) as client:
                result = list(
                    run_batches(
                        client,
                        succeed_on_retry,
                        [[{"id": "task/retry"}]],
                        resources={"CPU": 1},
                        max_in_flight=1,
                        retries=2,
                    )
                )
            self.assertEqual(result[0][1]["retry_count"], 1)
        finally:
            cluster.close()


class CalibrationSafetyTests(unittest.TestCase):
    def test_distributed_scientific_comparison_uses_requested_tolerances(self) -> None:
        reference = {
            "forecast": {
                "mean": [1.0],
                "median": [1.0],
                "quantiles": [[1.0] for _ in range(9)],
            }
        }
        within = {
            "forecast": {
                "mean": [1.000009],
                "median": [1.000009],
                "quantiles": [[1.000009] for _ in range(9)],
            }
        }
        outside = {
            "forecast": {
                "mean": [1.001],
                "median": [1.001],
                "quantiles": [[1.001] for _ in range(9)],
            }
        }
        self.assertTrue(_scientific_comparison(within, reference)["equivalent"])
        self.assertFalse(_scientific_comparison(outside, reference)["equivalent"])
        self.assertFalse(_scientific_comparison({}, reference)["equivalent"])

    def test_ubuntu_candidates_compare_with_independent_batch_one_reference(self) -> None:
        contexts = [
            {"label": label, "context": [float(index)]}
            for index, label in enumerate(("short", "median", "long"))
        ]

        class FakeWorker:
            def __init__(self) -> None:
                self.batch_sizes = []

            def request(self, message):
                batch_size = message["inference_batch_size"]
                self.batch_sizes.append(batch_size)
                value = float(batch_size)
                forecast = {
                    "mean": [value],
                    "median": [value],
                    "quantiles": [[value] for _ in range(9)],
                }
                return {
                    "type": "result",
                    "results": [forecast for _ in message["jobs"]],
                }

        worker = FakeWorker()
        references = _chronos_reference_forecasts(worker, contexts)
        candidates = CALIBRATION_CANDIDATES[
            "ubuntu_3950x_16core_128gb_rtx5090"
        ]["chronos_batch_sizes"]
        comparisons = {}
        for batch_size in candidates:
            responses = _chronos_context_responses(worker, contexts, batch_size)
            comparisons[batch_size] = _chronos_differences(
                contexts, responses, references
            )

        self.assertEqual(candidates, [8, 16, 32, 64])
        self.assertEqual(worker.batch_sizes[:3], [1, 1, 1])
        self.assertEqual(
            worker.batch_sizes[3:], [8] * 3 + [16] * 3 + [32] * 3 + [64] * 3
        )
        self.assertEqual(comparisons[8][0]["max_abs"], 7.0)
        self.assertFalse(comparisons[8][0]["equivalent"])

    def test_faster_unsafe_candidate_is_not_recommended(self) -> None:
        measurements = [
            {
                "kind": "chronos_2",
                "batch_size": 8,
                "tasks_per_second": 10.0,
                "safe": True,
                "failure": None,
                "strictly_equivalent_to_batch_one": True,
            },
            {
                "kind": "chronos_2",
                "batch_size": 16,
                "tasks_per_second": 20.0,
                "safe": False,
                "safety_rejection_reason": "system memory below threshold",
                "failure": None,
                "strictly_equivalent_to_batch_one": True,
            },
        ]
        self.assertEqual(
            _recommended_setting(
                measurements,
                "chronos_2",
                "batch_size",
                "strictly_equivalent_to_batch_one",
            ),
            8,
        )


if __name__ == "__main__":
    unittest.main()
