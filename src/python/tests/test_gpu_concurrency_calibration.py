# ==============================================================================
# test_gpu_concurrency_calibration.py
#
# Purpose: Verify logical-GPU calibration settings, resource validation, telemetry safety decisions, and result equivalence boundaries.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_gpu_concurrency_calibration
# ==============================================================================

"""Verify logical-GPU calibration settings, resource validation, telemetry safety decisions, and result equivalence boundaries."""

import unittest
from pathlib import Path
from unittest.mock import MagicMock

from util.distributed_execution import (
    CHRONOS_GPU_RESOURCE,
    EXPECTED_DASK_VERSION,
    validate_cluster,
)
from util.configuration import load_experiment_configuration
from util.gpu_concurrency_calibration import (
    GIB,
    GpuCalibrationSettings,
    _configuration_decisions,
    _telemetry_summary,
    candidate_settings,
    control_settings,
)


class GpuConcurrencyCalibrationTests(unittest.TestCase):
    """Purpose: Verify calibration settings, cluster capacity, telemetry, and acceptance decisions.

    Inputs: Committed configuration, mocked worker reports, and synthetic GPU telemetry mappings.
    Outputs: Validation and decision assertions; no GPU process, database, or file side effects.
    """
    # Test/calibration value: resolved committed experiment fixture used as the
    # assertion authority; it does not override production configuration.
    configuration = load_experiment_configuration(
        Path(__file__).resolve().parents[3]
        / "config/experiments/poc2_m4_daily_100.json"
    )
    def test_one_worker_defaults_preserve_existing_calibration_behaviour(self):
        """Control settings retain the single-worker calibration and safety defaults."""
        settings = control_settings()
        settings.validate()
        self.assertEqual(settings.physical_gpu_count, 1)
        self.assertEqual(settings.gpu_worker_processes, 1)
        self.assertEqual(settings.gpu_max_in_flight_batches, 1)
        self.assertEqual(settings.gpu_memory_headroom_gib, 4.0)
        self.assertEqual(settings.telemetry_interval_seconds, 0.2)

    def test_fifteen_worker_request_expects_fifteen_logical_workers(self):
        """Candidate settings require exactly fifteen conforming logical GPU workers."""
        settings = candidate_settings()
        settings.validate()
        self.assertEqual(settings.physical_gpu_count, 1)
        self.assertEqual(settings.gpu_worker_processes, 15)
        self.assertEqual(settings.gpu_max_in_flight_batches, 15)
        self.assertEqual(settings.worker_memory_limit_gib * 15, 60)

        client = MagicMock()
        reports = {
            f"gpu-{index}": {
                "git_commit": "revision",
                "git_dirty": False,
                "python_version": "3.12.14",
                "dask_version": EXPECTED_DASK_VERSION,
                "distributed_version": EXPECTED_DASK_VERSION,
                "configuration_hash": "configuration",
                "gift_eval_revision": self.configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
                "resources": {CHRONOS_GPU_RESOURCE: 1},
                "r_packages": {
                    "R": "4.6.1",
                    "renv": "1.2.4",
                    "forecast": "8.24.0",
                    "jsonlite": "2.0.0",
                },
                "chronos": {
                    "chronos_forecasting": "2.2.2",
                    "checkpoint_revision": self.configuration.resolved["models"]["chronos_2"]["revision"],
                    "checkpoint_present": True,
                    "cuda_available": True,
                    "cuda_name": "NVIDIA GeForce RTX 5090",
                },
            }
            for index in range(15)
        }
        client.run.return_value = reports
        self.assertEqual(
            len(
                validate_cluster(
                    client,
                    expected_workers=15,
                    expected_gpu_workers=15,
                    timeout=1,
                    expected_commit="revision",
                    expected_configuration_hash="configuration",
                    expected_gift_eval_revision=self.configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
                    expected_chronos_revision=self.configuration.resolved["models"]["chronos_2"]["revision"],
                    expected_chronos_version=self.configuration.resolved["models"]["chronos_2"]["chronos_forecasting"],
                    chronos_repository=self.configuration.resolved["models"]["chronos_2"]["repository"],
                    chronos_environment=self.configuration.resolved["execution"]["paths"]["chronos_environment"],
                    gift_eval_source_directory=self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
                    require_gpu=True,
                    expected_gpu_name=self.configuration.resolved["execution"]["final_acceptance"]["gpu_name"],
                )
            ),
            15,
        )
        with self.assertRaisesRegex(RuntimeError, "expected exactly 15"):
            client.run.return_value = dict(list(reports.items())[:14])
            validate_cluster(
                client,
                expected_workers=15,
                expected_gpu_workers=15,
                timeout=1,
                expected_commit="revision",
                expected_configuration_hash="configuration",
                expected_gift_eval_revision=self.configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
                expected_chronos_revision=self.configuration.resolved["models"]["chronos_2"]["revision"],
                expected_chronos_version=self.configuration.resolved["models"]["chronos_2"]["chronos_forecasting"],
                chronos_repository=self.configuration.resolved["models"]["chronos_2"]["repository"],
                chronos_environment=self.configuration.resolved["execution"]["paths"]["chronos_environment"],
                gift_eval_source_directory=self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
                require_gpu=True,
                expected_gpu_name=self.configuration.resolved["execution"]["final_acceptance"]["gpu_name"],
            )

    def test_chronos_queue_requires_one_slot_per_logical_worker(self):
        """Validation requires an in-flight Chronos slot for every logical worker."""
        with self.assertRaisesRegex(ValueError, "at least gpu_worker_processes"):
            GpuCalibrationSettings(
                gpu_worker_processes=15,
                gpu_max_in_flight_batches=14,
            ).validate()

    def test_report_identity_distinguishes_physical_gpu_and_worker_processes(self):
        """Calibration evidence distinguishes one physical GPU from fifteen workers."""
        settings = candidate_settings()
        evidence = {
            "physical_gpu_count": settings.physical_gpu_count,
            "gpu_worker_processes": settings.gpu_worker_processes,
        }
        self.assertEqual(evidence, {"physical_gpu_count": 1, "gpu_worker_processes": 15})

    def test_all_fifteen_workers_must_contribute_and_resources_must_be_safe(self):
        """Candidate acceptance requires safe telemetry and contribution from every worker."""
        workers = {
            f"gpu-{index}": {
                "worker_name": f"gpu-{index}",
                "hostname": "ubuntu",
                "completed_tasks": 1,
                "completed_batches": 1,
            }
            for index in range(15)
        }
        telemetry = _telemetry_summary(
            [
                {
                    "configured_interval_seconds": 0.2,
                    "gpu_utilisation_percent": 50,
                    "gpu_memory_used_bytes": 10 * GIB,
                    "gpu_memory_free_bytes": 20 * GIB,
                    "gpu_power_watts": 200,
                    "gpu_temperature_celsius": 60,
                    "system_available_memory_bytes": 80 * GIB,
                    "swap_used_bytes": 0,
                }
            ],
            [],
            0,
            None,
        )
        measurement = {
            "complete": True,
            "completed_task_count": 400,
            "unique_task_count": 400,
            "duplicate_task_count": 0,
            "scientific_comparison": {"equivalent": True},
            "resource_safe": telemetry["passed"],
            "all_expected_workers_contributed": len(workers) == 15
            and all(item["completed_tasks"] for item in workers.values()),
        }
        decisions = _configuration_decisions(candidate_settings(), [measurement])
        self.assertTrue(decisions["scientifically_equivalent"])
        self.assertTrue(decisions["resource_safe"])
        self.assertTrue(decisions["all_logical_gpu_workers_contributed"])

        measurement["all_expected_workers_contributed"] = False
        rejected = _configuration_decisions(candidate_settings(), [measurement])
        self.assertFalse(rejected["all_logical_gpu_workers_contributed"])


if __name__ == "__main__":
    unittest.main()
