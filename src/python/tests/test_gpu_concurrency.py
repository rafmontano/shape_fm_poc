import unittest
from unittest.mock import MagicMock

from shapefm.dask_execution import (
    EXPECTED_CHRONOS_REVISION,
    EXPECTED_DASK_VERSION,
    EXPECTED_GIFT_EVAL_REVISION,
    validate_cluster,
)
from tests.gpu_concurrency import (
    GIB,
    GpuCalibrationSettings,
    _configuration_decisions,
    _telemetry_summary,
    candidate_settings,
    control_settings,
)


class GpuConcurrencyCalibrationTests(unittest.TestCase):
    def test_one_worker_defaults_preserve_existing_calibration_behaviour(self):
        settings = control_settings()
        settings.validate()
        self.assertEqual(settings.physical_gpu_count, 1)
        self.assertEqual(settings.gpu_worker_processes, 1)
        self.assertEqual(settings.gpu_max_in_flight_batches, 1)
        self.assertEqual(settings.gpu_memory_headroom_gib, 4.0)
        self.assertEqual(settings.telemetry_interval_seconds, 0.2)

    def test_fifteen_worker_request_expects_fifteen_logical_workers(self):
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
                "gift_eval_revision": EXPECTED_GIFT_EVAL_REVISION,
                "resources": {"GPU": 1},
                "r_packages": {
                    "R": "4.6.1",
                    "renv": "1.2.4",
                    "forecast": "8.24.0",
                    "jsonlite": "2.0.0",
                },
                "chronos": {
                    "chronos_forecasting": "2.2.2",
                    "checkpoint_revision": EXPECTED_CHRONOS_REVISION,
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
                    require_gpu=True,
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
                require_gpu=True,
            )

    def test_chronos_queue_requires_one_slot_per_logical_worker(self):
        with self.assertRaisesRegex(ValueError, "at least gpu_worker_processes"):
            GpuCalibrationSettings(
                gpu_worker_processes=15,
                gpu_max_in_flight_batches=14,
            ).validate()

    def test_report_identity_distinguishes_physical_gpu_and_worker_processes(self):
        settings = candidate_settings()
        evidence = {
            "physical_gpu_count": settings.physical_gpu_count,
            "gpu_worker_processes": settings.gpu_worker_processes,
        }
        self.assertEqual(evidence, {"physical_gpu_count": 1, "gpu_worker_processes": 15})

    def test_all_fifteen_workers_must_contribute_and_resources_must_be_safe(self):
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
