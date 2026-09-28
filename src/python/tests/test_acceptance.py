# ==============================================================================
# test_acceptance.py
#
# Purpose: Verify acceptance orchestration, memory budgets, evidence validation, and restart-report decisions without running the two-machine workload.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_acceptance
# ==============================================================================

"""Verify acceptance orchestration, memory budgets, evidence validation, and restart-report decisions without running the two-machine workload."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import tests.acceptance as acceptance
from tests.acceptance import (
    GIB,
    _activate_configuration,
    _contribution_checks,
    _contribution_topology,
    _memory_budget,
    _record_success,
    _report_document,
    _resolve_acceptance_profile,
    _summarize_resources,
    _write_failure_report,
    run_acceptance,
)
from util.configuration import load_experiment_configuration
from util.distributed_execution import CHRONOS_GPU_RESOURCE


class AcceptanceReadinessTests(unittest.TestCase):
    """Verify acceptance evidence, resource safety, resumability, and report decisions."""
    def setUp(self):
        """Activate the stored contract and build its two-worker topology."""
        _activate_configuration(
            load_experiment_configuration(
                Path(__file__).resolve().parents[3]
                / "config/experiments/poc2_m4_daily_100.json"
            )
        )
        gpu_workers = [
            f"gpu-worker-{index}" for index in range(acceptance.UBUNTU_GPU_WORKERS)
        ]
        self.topology = {
            "topology": {
                "mac_hosts": ["mac"],
                "ubuntu_hosts": ["ubuntu"],
                "gpu_worker_addresses": gpu_workers,
            },
            "workers": {
                "mac-worker": {"hostname": "mac", "resources": {"CPU": 1}},
                **{
                    worker: {
                        "hostname": "ubuntu",
                        "resources": {CHRONOS_GPU_RESOURCE: 1},
                    }
                    for worker in gpu_workers
                },
            },
        }

    def _evidence(self, ubuntu_resource="CPU"):
        """Build host and GPU-worker task contributions, optionally changing Ubuntu's resource."""
        return {
            "task_contribution_by_host_and_resource": [
                {
                    "hostname": "mac",
                    "advertised_resource": "CPU",
                    "completed_tasks": 10,
                },
                {
                    "hostname": "ubuntu",
                    "advertised_resource": ubuntu_resource,
                    "completed_tasks": 20,
                },
            ],
            "chronos_contribution": [
                {
                    "hostname": "ubuntu",
                    "dask_worker": f"gpu-worker-{index}",
                    "advertised_resource": CHRONOS_GPU_RESOURCE,
                    "completed_tasks": (
                        acceptance.EXPECTED_CHRONOS_TASKS
                    ),
                }
                for index in range(acceptance.UBUNTU_GPU_WORKERS)
            ],
        }

    def _sample(self, *, mac_available=4 * GIB, spill=0, swap=0):
        """Build a resource sample with configurable Mac headroom, spill, and swap usage."""
        return {
            "workers": {
                "mac-worker": {
                    "hostname": "mac",
                    "system_available_memory_bytes": mac_available,
                    "swap_used_bytes": swap,
                    "dask_spilled_memory_bytes": spill,
                    "dask_spilled_disk_bytes": 0,
                },
                "ubuntu-worker": {
                    "hostname": "ubuntu",
                    "system_available_memory_bytes": 20 * GIB,
                    "swap_used_bytes": 0,
                    "dask_spilled_memory_bytes": 0,
                    "dask_spilled_disk_bytes": 0,
                },
                **{
                    f"gpu-worker-{index}": {
                        "hostname": "ubuntu",
                        "system_available_memory_bytes": 20 * GIB,
                        "swap_used_bytes": 0,
                        "dask_spilled_memory_bytes": 0,
                        "dask_spilled_disk_bytes": 0,
                        "gpu": {"available_memory_bytes": 8 * GIB},
                    }
                    for index in range(acceptance.UBUNTU_GPU_WORKERS)
                },
            },
            "scheduler_workers": {
                "mac-worker": {"memory_limit": acceptance.MAC_CPU_MEMORY_GIB * GIB},
                **{
                    f"gpu-worker-{index}": {
                        "memory_limit": acceptance.UBUNTU_GPU_MEMORY_GIB * GIB
                    }
                    for index in range(acceptance.UBUNTU_GPU_WORKERS)
                },
            },
        }

    def test_requires_mac_cpu_and_ubuntu_gpu_contribution(self):
        """Contribution checks require both hosts and the exact GPU task count."""
        result = _contribution_checks(self._evidence(), self.topology)
        self.assertTrue(result["passed"])
        self.assertEqual(result["mac_cpu_completed_tasks"], 10)
        self.assertEqual(result["ubuntu_cpu_completed_tasks"], 20)
        self.assertEqual(
            result["chronos_completed_tasks"], acceptance.EXPECTED_CHRONOS_TASKS
        )

        wrong_count = self._evidence()
        wrong_count["chronos_contribution"][0]["completed_tasks"] -= 1
        self.assertFalse(_contribution_checks(wrong_count, self.topology)["passed"])

        wrong_worker = self._evidence()
        wrong_worker["chronos_contribution"][0]["dask_worker"] = "ubuntu-worker"
        self.assertFalse(_contribution_checks(wrong_worker, self.topology)["passed"])

    def test_accepts_gpu_only_ubuntu_contribution(self):
        """The approved two-worker topology needs no separate Ubuntu CPU worker."""
        result = _contribution_checks(
            self._evidence(CHRONOS_GPU_RESOURCE), self.topology
        )
        self.assertTrue(result["passed"])
        self.assertEqual(result["ubuntu_cpu_completed_tasks"], 0)

    def test_rejects_zero_samples_and_unsafe_resources(self):
        """Resource validation fails without samples or with pressure, spill, swap, or worker loss."""
        empty = _summarize_resources([], [], self.topology)
        self.assertFalse(empty["passed"])
        self.assertEqual(empty["valid_sample_count"], 0)

        unsafe = _summarize_resources(
            [self._sample(), self._sample(mac_available=2 * GIB, spill=1, swap=1)],
            ["synthetic sampler error"],
            self.topology,
        )
        self.assertFalse(unsafe["passed"])
        self.assertGreater(unsafe["host_observations"]["mac"]["swap_growth_bytes"], 0)
        self.assertGreater(
            unsafe["host_observations"]["mac"]["maximum_dask_spill_bytes"], 0
        )
        self.assertIn("mac memory headroom fell below threshold", unsafe["unsafe_reasons"])

        removed = self._sample()
        del removed["workers"]["gpu-worker-0"]
        replacement = _summarize_resources([removed], [], self.topology)
        self.assertFalse(replacement["passed"])
        self.assertEqual(replacement["worker_replacements_or_removals"], 1)

    def test_writes_failure_report(self):
        """Failure reports preserve phase diagnostics and mark pre-scientific databases reusable."""
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            report = {"initial_run": None, "restart_run": None, "failure_history": []}
            _write_failure_report(
                report_path,
                report,
                "initial",
                phase="plan",
                error=RuntimeError("synthetic failure"),
                started_at="start",
                runtime_seconds=1.5,
                diagnostics={"resources": {"sample_count": 1}},
            )
            written = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(written["decision"], "fail")
            self.assertEqual(written["initial_run"]["phase"], "plan")
            self.assertTrue(written["database_reusable"])
            self.assertEqual(written["initial_run"]["resources"]["sample_count"], 1)

    def test_run_writes_failure_report_after_database_work_starts(self):
        """Import failures are recorded after preflight and before cluster startup."""
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "acceptance.duckdb"
            report_path = Path(directory) / "report.json"
            cluster = MagicMock()
            cluster.preflight.return_value = {
                "repository_revision": "revision",
                "gift_eval_revision": "gift",
            }
            coordinator = MagicMock()
            coordinator.__enter__.return_value.import_configured.side_effect = RuntimeError(
                "synthetic import failure"
            )
            with (
                patch("tests.acceptance._TwoMachineCluster", return_value=cluster),
                patch("tests.acceptance.ImportCoordinator", return_value=coordinator),
                self.assertRaisesRegex(RuntimeError, "synthetic import failure"),
            ):
                run_acceptance(
                    root,
                    database,
                    report_path,
                    {"repository_revision": "revision"},
                )
            written = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(written["decision"], "fail")
            self.assertEqual(written["initial_run"]["phase"], "import")
            self.assertTrue(written["database_reusable"])
            cluster.start.assert_not_called()

    def test_unsafe_scientific_failure_cannot_reuse_database(self):
        """A resource-validation failure makes the scientific database ineligible for resume."""
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = (Path(directory) / "acceptance.duckdb").resolve()
            from util.database import initialize_experiment_database

            initialize_experiment_database(
                database, root / "config/experiments/poc2_m4_daily_100.json"
            )
            report_path = Path(directory) / "report.json"
            report = _report_document(
                None,
                database=database,
                entry_invocation={"repository_revision": "revision"},
                metadata={},
            )
            _write_failure_report(
                report_path,
                report,
                "initial",
                phase="resource_validation",
                error=RuntimeError("unsafe resources"),
                started_at="start",
                runtime_seconds=1.0,
                diagnostics={},
            )
            written = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertFalse(written["database_reusable"])
            with self.assertRaisesRegex(ValueError, "non-reusable.*fresh database"):
                run_acceptance(
                    root,
                    database,
                    report_path,
                    {"repository_revision": "revision"},
                )

    def test_pre_scientific_failure_can_resume_matching_database(self):
        """A matching database can resume after failure before scientific execution."""
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = (Path(directory) / "acceptance.duckdb").resolve()
            from util.database import initialize_experiment_database

            initialize_experiment_database(
                database, root / "config/experiments/poc2_m4_daily_100.json"
            )
            report_path = Path(directory) / "report.json"
            report = _report_document(
                None,
                database=database,
                entry_invocation={"repository_revision": "revision"},
                metadata={},
            )
            _write_failure_report(
                report_path,
                report,
                "initial",
                phase="cluster_start",
                error=RuntimeError("cluster unavailable"),
                started_at="start",
                runtime_seconds=1.0,
                diagnostics={},
            )
            cluster = MagicMock()
            cluster.preflight.return_value = {"repository_revision": "revision"}
            coordinator = MagicMock()
            coordinator.__enter__.return_value.import_configured.side_effect = RuntimeError(
                "resumed import reached"
            )
            with (
                patch("tests.acceptance._TwoMachineCluster", return_value=cluster),
                patch("tests.acceptance.ImportCoordinator", return_value=coordinator),
                self.assertRaisesRegex(RuntimeError, "resumed import reached"),
            ):
                run_acceptance(
                    root,
                    database,
                    report_path,
                    {"repository_revision": "revision"},
                )
            cluster.preflight.assert_called_once()

    def test_nonmatching_report_is_never_overwritten(self):
        """A report owned by another database or revision remains unchanged."""
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = (Path(directory) / "fresh.duckdb").resolve()
            report_path = Path(directory) / "report.json"
            original = {
                "database": str((Path(directory) / "other.duckdb").resolve()),
                "repository_revision": "other-revision",
                "sentinel": "preserve me",
            }
            report_path.write_text(json.dumps(original) + "\n", encoding="utf-8")
            before = report_path.read_bytes()
            with self.assertRaisesRegex(ValueError, "another database.*fresh report path"):
                run_acceptance(
                    root,
                    database,
                    report_path,
                    {"repository_revision": "revision"},
                )
            self.assertEqual(report_path.read_bytes(), before)

    def test_preserves_initial_evidence_when_recording_restart(self):
        """Recording restart success retains initial evidence and finalizes acceptance."""
        report = _report_document(
            None,
            database=Path("acceptance.duckdb").resolve(),
            entry_invocation={"repository_revision": "revision"},
            metadata={},
        )
        initial = {
            "scientific_run_complete": True,
            "restart_passed": False,
            "topology": {"initial": True},
            "resources": {"sample_count": 7},
            "runtime_seconds": 12.0,
        }
        report = _record_success(report, "initial", initial)
        self.assertTrue(report["database_reusable"])
        self.assertEqual(report["database_reuse_reason"], "required_restart_test")
        restart = {
            "scientific_run_complete": True,
            "restart_passed": True,
            "topology": {"restart": True},
        }
        report = _record_success(report, "restart", restart)
        self.assertEqual(
            {key: report["initial_run"][key] for key in initial}, initial
        )
        self.assertEqual(
            {key: report["restart_run"][key] for key in restart}, restart
        )
        self.assertTrue(report["acceptance_passed"])
        self.assertFalse(report["database_reusable"])

    def test_restart_uses_retained_initial_gpu_worker_topology(self):
        """Restart contribution is checked against the initial GPU worker identities."""
        initial_topology = self.topology
        current_topology = {
            **self.topology,
            "topology": {
                **self.topology["topology"],
                "gpu_worker_addresses": [
                    f"new-gpu-worker-{index}"
                    for index in range(acceptance.UBUNTU_GPU_WORKERS)
                ],
            },
        }
        report = {
            "initial_run": {
                "scientific_run_complete": True,
                "topology": initial_topology,
            }
        }
        retained = _contribution_topology(report, "restart", current_topology)
        self.assertTrue(_contribution_checks(self._evidence(), retained)["passed"])
        self.assertFalse(
            _contribution_checks(self._evidence(), current_topology)["passed"]
        )

    def test_profile_records_actual_topology_overrides(self):
        """The acceptance profile records worker overrides within both hosts' memory budgets."""
        profile, overrides = _resolve_acceptance_profile()
        self.assertEqual(profile.dask_mac_cpu_workers, 1)
        self.assertEqual(profile.dask_ubuntu_cpu_workers, 0)
        self.assertEqual(profile.dask_max_in_flight, acceptance.MAX_IN_FLIGHT)
        self.assertGreaterEqual(
            acceptance.MAX_IN_FLIGHT, acceptance.UBUNTU_GPU_WORKERS
        )
        self.assertEqual(
            overrides,
            {
                "dask_mac_cpu_workers": 1,
                "dask_ubuntu_cpu_workers": 0,
                "dask_max_in_flight": acceptance.MAX_IN_FLIGHT,
            },
        )
        budget = _memory_budget()
        self.assertEqual(acceptance.UBUNTU_CPU_MEMORY_GIB, 0)
        self.assertEqual(acceptance.UBUNTU_GPU_MEMORY_GIB, 4)
        self.assertEqual(
            budget["ubuntu"]["configured_worker_memory_ceiling_bytes"],
            4 * GIB,
        )
        self.assertGreaterEqual(
            budget["mac"]["memory_outside_worker_ceilings_bytes"],
            budget["mac"]["required_headroom_bytes"],
        )
        self.assertGreaterEqual(
            budget["ubuntu"]["memory_outside_worker_ceilings_bytes"],
            budget["ubuntu"]["required_headroom_bytes"],
        )


if __name__ == "__main__":
    unittest.main()
