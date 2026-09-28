# ==============================================================================
# test_execution.py
#
# Purpose: Verify execution-profile constraints, worker protocols, cluster validation, and source-level single-writer safeguards.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_execution
# ==============================================================================

"""Verify execution-profile constraints, worker protocols, cluster validation, and source-level single-writer safeguards."""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from util.execution_calibration import (
    CALIBRATION_CANDIDATES,
    _chronos_context_responses,
    _chronos_differences,
    _chronos_reference_forecasts,
    _recommended_setting,
    _scientific_comparison,
)
from util.execution_profiles import (
    ExecutionSettings,
    PersistentChronosWorker,
    resolve_execution_profile,
    system_hardware,
)
from util.configuration import load_experiment_configuration
from util.experiment_execution import (
    POC1Coordinator,
    _length_aware_batches,
    expected_task_counts,
)


class ExecutionProfileTests(unittest.TestCase):
    """Verify profile defaults, validation, task sizing, and coordinator settings."""
    def test_committed_profile_values_and_single_writer(self) -> None:
        """Profiles retain their tuned concurrency and exactly one database writer."""
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
        distributed, _ = resolve_execution_profile("two_machine_dask")
        self.assertEqual(
            (
                distributed.dask_mac_cpu_workers,
                distributed.dask_ubuntu_cpu_workers,
                distributed.chronos_inference_batch_size,
                distributed.dask_max_in_flight,
            ),
            (2, 4, 16, 12),
        )

    def test_hardware_provenance_records_cpu_model(self) -> None:
        """Hardware provenance includes the CPU model reported by the platform helper."""
        with patch("util.execution_profiles.cpu_model", return_value="Test CPU"):
            self.assertEqual(system_hardware()["cpu_model"], "Test CPU")

    def test_execution_settings_are_invocation_only_and_validated(self) -> None:
        """Invocation settings serialize Dask options and reject invalid modes or counts."""
        settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address="tcp://scheduler:8786",
            dask_expected_workers=7,
            dask_expected_gpu_workers=3,
            dask_max_in_flight=12,
            dask_retries=2,
        )
        self.assertEqual(settings.mode, "dask")
        self.assertEqual(settings.dask_expected_workers, 7)
        self.assertEqual(settings.dask_expected_gpu_workers, 3)
        self.assertEqual(settings.to_dict()["dask_expected_gpu_workers"], 3)
        with self.assertRaisesRegex(ValueError, "execution mode"):
            ExecutionSettings(mode="remote")
        with self.assertRaisesRegex(ValueError, "GPU-worker"):
            ExecutionSettings(dask_expected_gpu_workers=0)

    def test_run_gate_propagates_expected_gpu_worker_count_to_validation(self) -> None:
        """The Dask run gate passes its expected GPU count to cluster validation."""
        coordinator = object.__new__(POC1Coordinator)
        coordinator.root = Path(__file__).resolve().parents[3]
        coordinator.configuration = load_experiment_configuration(
            coordinator.root / "config/experiments/poc2_m4_daily_100.json"
        )
        coordinator.config = coordinator.configuration.workflow
        coordinator.execution_hardware = MagicMock(return_value={})
        profile = resolve_execution_profile("sequential_safe")
        settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address="tcp://scheduler:8786",
            dask_expected_workers=35,
            dask_expected_gpu_workers=15,
        )
        client = MagicMock()
        with (
            patch("distributed.Client", return_value=client),
            patch(
                "util.distributed_execution.validate_cluster",
                side_effect=RuntimeError("validation sentinel"),
            ) as validate,
            patch(
                "util.experiment_execution.subprocess.run",
                return_value=MagicMock(stdout="revision\n"),
            ),
            self.assertRaisesRegex(RuntimeError, "validation sentinel"),
        ):
            coordinator.run_gate(
                "experiment",
                4,
                execution=profile,
                execution_settings=settings,
            )
        self.assertEqual(validate.call_args.kwargs["expected_gpu_workers"], 15)
        client.close.assert_called_once()

    def test_full_m4_daily_task_counts(self) -> None:
        """A 4,227-series M4 Daily run expands to the expected tasks per stage."""
        workflow = load_experiment_configuration(
            Path(__file__).resolve().parents[3]
            / "config/experiments/poc2_m4_daily_100.json"
        ).workflow
        counts = expected_task_counts(4_227, workflow)
        self.assertEqual(
            counts,
            {2: 8_454, 3: 16_908, 4: 33_816, 5: 50_724, 6: 12},
        )
        self.assertEqual(sum(counts.values()), 109_914)

    def test_unknown_and_invalid_overrides_fail(self) -> None:
        """Profile resolution rejects unknown names and unsafe concurrency overrides."""
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
        """Length-aware batches are size-bounded, ordered, and homogeneous by length band."""
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
    """Verify persistent worker protocols, isolation, batching, and Dask retries."""
    def test_relocated_r_workers_preserve_json_contracts(self) -> None:
        """R cleaning and forecasting entry points preserve their JSON response schemas."""
        root = Path(__file__).parents[3]
        clean_payload = {
            "action": "clean",
            "jobs": [
                {
                    "id": "identity",
                    "context": [1, None, 3, 4],
                    "method": "identity",
                    "seasonality": 1,
                },
                {
                    "id": "tsclean",
                    "context": [1, 2, 100, 4, 5, 6, 7, 8],
                    "method": "tsclean",
                    "seasonality": 1,
                },
            ],
        }
        forecast_payload = {
            "action": "forecast",
            "settings": load_experiment_configuration(
                root / "config/experiments/poc2_m4_daily_100.json"
            ).workflow["models"]["auto_arima"]["settings"],
            "jobs": [
                {
                    "id": "forecast",
                    "context": list(range(1, 13)),
                    "horizon": 3,
                    "seasonality": 1,
                }
            ],
        }

        responses = []
        for script, payload in (
            ("src/r/02_preprocess_series.R", clean_payload),
            ("src/r/04_forecast_auto_arima.R", forecast_payload),
        ):
            completed = subprocess.run(
                ["Rscript", script],
                cwd=root,
                input=json.dumps(payload),
                check=True,
                capture_output=True,
                env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
                text=True,
                timeout=60,
            )
            responses.append(json.loads(completed.stdout))

        clean, forecast = responses
        self.assertEqual(
            clean["results"],
            [
                {"id": "identity", "values": [1, 3, 4]},
                {"id": "tsclean", "values": list(range(1, 9))},
            ],
        )
        expected = [13, 14, 15]
        self.assertEqual(forecast["results"][0]["mean"], expected)
        self.assertEqual(forecast["results"][0]["median"], expected)
        self.assertEqual(forecast["results"][0]["quantiles"], [expected] * 9)
        for response in responses:
            self.assertEqual(set(response), {"results", "packages"})
            self.assertEqual(set(response["packages"]), {"R", "forecast", "jsonlite"})

    def test_multiple_batches_use_exactly_one_model_load(self) -> None:
        """One persistent process serves multiple batches without reloading its model."""
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
        """Worker stderr is drained without deadlock and retained only to the byte limit."""
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
        """Chronos and distributed worker modules do not import DuckDB."""
        source = (Path(__file__).parents[3] / "src/python/04_forecast_chronos.py").read_text(
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
            Path(__file__).parents[3] / "src/python/util/distributed_execution.py"
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
        """Dask transformation batches match direct transforms and report CPU resources."""
        from distributed import Client, LocalCluster

        from util.distributed_execution import run_batches, transform_batch
        from util.transformations import transform

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
        """A retried Dask batch receives the incremented retry count."""
        from distributed import Client, LocalCluster

        from util.distributed_execution import run_batches

        def succeed_on_retry(batch, retry_count=0):
            """Fail the initial attempt and identify jobs on the first retry."""
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
    """Verify calibration equivalence baselines and safe-setting selection."""
    def test_distributed_scientific_comparison_uses_requested_tolerances(self) -> None:
        """Scientific comparison accepts small drift and rejects larger or missing output."""
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
        """Each Ubuntu batch candidate is compared with per-context batch-one output."""
        contexts = [
            {"label": label, "context": [float(index)]}
            for index, label in enumerate(("short", "median", "long"))
        ]

        class FakeWorker:
            """Record requested batch sizes and return forecasts equal to that size."""
            def __init__(self) -> None:
                """Initialize the ordered record of requested batch sizes."""
                self.batch_sizes = []

            def request(self, message):
                """Record one request and return one constant forecast per job."""
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
        quantile_levels = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        references = _chronos_reference_forecasts(
            worker, contexts, horizon=14, quantile_levels=quantile_levels
        )
        candidates = CALIBRATION_CANDIDATES[
            "ubuntu_3950x_16core_128gb_rtx5090"
        ]["chronos_batch_sizes"]
        comparisons = {}
        for batch_size in candidates:
            responses = _chronos_context_responses(
                worker,
                contexts,
                batch_size,
                horizon=14,
                quantile_levels=quantile_levels,
            )
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
        """Recommendation favors the fastest safe equivalent candidate over unsafe speed."""
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
