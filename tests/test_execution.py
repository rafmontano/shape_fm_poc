from __future__ import annotations

import ast
import sys
import tempfile
import unittest
from pathlib import Path

from shapefm.calibration import _recommended_setting
from shapefm.execution import PersistentChronosWorker, resolve_execution_profile
from shapefm.poc1 import _length_aware_batches


class ExecutionProfileTests(unittest.TestCase):
    def test_committed_profile_values_and_single_writer(self) -> None:
        sequential, _ = resolve_execution_profile("sequential_safe")
        mac, _ = resolve_execution_profile("mac_m1pro_10core_16gb")
        ubuntu, _ = resolve_execution_profile("ubuntu_5950x_16core_128gb_rtx5090")
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


class CalibrationSafetyTests(unittest.TestCase):
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
