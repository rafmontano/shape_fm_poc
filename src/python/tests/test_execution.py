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
from util.shared_execution_profiles import (
    ExecutionSettings,
    PersistentChronosWorker,
    resolve_execution_profile,
    system_hardware,
    validate_heavy_tuning_execution,
)
from util.shared_configuration import load_experiment_configuration
from util.shared_machine_environment import resolve_machine_environment
from util.shared_experiment_execution import (
    ExperimentCoordinator,
    _length_aware_batches,
    expected_task_counts,
)


class ExecutionProfileTests(unittest.TestCase):
    """Purpose: Verify profile defaults, validation, task sizing, and coordinator settings.

    Inputs: Committed profiles, synthetic jobs, configuration files, and mocked Dask clients.
    Outputs: Assertions over settings and request shapes; no persistent state or external side effects.
    """
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
        recovery, _ = resolve_execution_profile("poc2_seasonal_recovery")
        self.assertEqual(recovery.profile_version, 4)
        self.assertEqual(recovery.dask_max_in_flight, 23)
        self.assertEqual(recovery.dask_autoarima_max_in_flight, 8)
        self.assertEqual(recovery.dask_ets_max_in_flight, 15)
        self.assertEqual(recovery.dask_autoarima_fit_budget_gib, 12)
        self.assertEqual(recovery.accelerator_memory_min_available_gib, 4.0)
        self.assertIsNotNone(recovery.machine_environment)
        environment = recovery.machine_environment
        self.assertEqual(environment.coordinator_id, "macbook_pro")
        self.assertEqual(environment.prefect_api_url,
                         "http://RMMacbookPro.local:4200/api")
        self.assertEqual(environment.scheduler_address,
                         "tcp://RMMacbookPro.local:8786")
        self.assertEqual(
            recovery.distributed_topology(False)["workers_by_machine"],
            {
                "macbook_pro": {"cpu_workers": 8, "gpu_workers": 0,
                                "tuning_workers": 8},
                "ubuntu_primary": {"cpu_workers": 15, "gpu_workers": 0,
                                   "tuning_workers": 15},
            },
        )
        self.assertEqual(recovery.distributed_topology(True)["total_workers"], 38)
        self.assertEqual(len(recovery.fingerprint), 64)

    def test_hardware_provenance_records_cpu_model(self) -> None:
        """Hardware provenance includes the CPU model reported by the platform helper."""
        with patch("util.shared_execution_profiles.cpu_model", return_value="Test CPU"):
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
        self.assertEqual(ExecutionSettings(dask_expected_gpu_workers=0).dask_expected_gpu_workers, 0)
        with self.assertRaisesRegex(ValueError, "GPU-worker"):
            ExecutionSettings(dask_expected_gpu_workers=-1)

    def test_heavy_guard_rejects_version_and_fingerprint_drift(self) -> None:
        """The startup guard requires the exact approved v3 profile identity."""
        from dataclasses import replace

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        settings = ExecutionSettings(mode="dask", dask_scheduler_address="managed")
        validate_heavy_tuning_execution(profile, settings, {})
        for drifted in (
            replace(profile, profile_version=2),
            replace(profile, accelerator_memory_min_available_gib=3.0),
        ):
            with self.assertRaisesRegex(RuntimeError, "profile poc2_seasonal_recovery v4"):
                validate_heavy_tuning_execution(drifted, settings, {})

    def test_managed_cluster_resolves_cpu_only_and_gpu_topology(self) -> None:
        """The managed launcher uses workload topology and rejects missing GPU safety."""
        from dataclasses import replace
        from util.shared_distributed_cluster import ManagedTuningCluster

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        cpu = ManagedTuningCluster(profile)
        gpu = ManagedTuningCluster(profile, requires_gpu=True)
        bounded_gpu = ManagedTuningCluster(profile, requires_gpu=True, gpu_workers=1)
        self.assertEqual(cpu.topology["gpu_workers"], 0)
        self.assertEqual(cpu.topology["total_workers"], 23)
        self.assertEqual(gpu.topology["gpu_workers"], 15)
        self.assertEqual(gpu.topology["total_workers"], 38)
        self.assertEqual(bounded_gpu.topology["gpu_workers"], 1)
        self.assertEqual(bounded_gpu.topology["total_workers"], 24)
        with self.assertRaisesRegex(ValueError, "exceeds enabled profile capacity"):
            ManagedTuningCluster(profile, requires_gpu=True, gpu_workers=16)
        with self.assertRaisesRegex(ValueError, "positive accelerator memory floor"):
            ManagedTuningCluster(
                replace(profile, accelerator_memory_min_available_gib=0),
                requires_gpu=True,
            )

    def test_managed_cluster_partial_remote_cleanup_releases_local_processes(self) -> None:
        """A remote cleanup failure cannot strand owned local process groups."""
        from util.shared_distributed_cluster import ManagedTuningCluster

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        process = MagicMock(pid=123)
        process.poll.return_value = None
        with (
            patch("util.shared_distributed_cluster.os.killpg") as kill_group,
        ):
            cluster = ManagedTuningCluster(profile)
            cluster.remote_started = {"ubuntu_primary"}
            cluster.processes = [process]
            cluster._ssh = MagicMock(side_effect=subprocess.SubprocessError("offline"))
            cluster.stop()
        self.assertEqual(cluster.remote_started, set())
        self.assertEqual(cluster.processes, [])
        kill_group.assert_called_once_with(123, 15)
        process.wait.assert_called_once_with(timeout=10)

    def test_managed_cluster_derives_and_propagates_service_endpoints(self) -> None:
        """Workers use coordinator-hostname endpoints without researcher exports."""
        from util.shared_distributed_cluster import ManagedTuningCluster

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        with patch.dict(os.environ, {}, clear=True):
            cluster = ManagedTuningCluster(profile)
            cluster.preflight = MagicMock(return_value={})
            cluster._start_local = MagicMock()
            cluster._wait_for_prefect = MagicMock()
            cluster._verify_remote_endpoint = MagicMock()
            cluster._ssh = MagicMock()
            evidence = cluster.start()
        remote_command = cluster._ssh.call_args.args[1]
        self.assertIn("PREFECT_API_URL=http://RMMacbookPro.local:4200/api", remote_command)
        self.assertIn("PREFECT_SERVER_EPHEMERAL_ENABLED=false", remote_command)
        self.assertEqual(evidence["prefect_api_url"],
                         "http://RMMacbookPro.local:4200/api")
        self.assertEqual(evidence["scheduler_address"],
                         "tcp://RMMacbookPro.local:8786")
        self.assertNotIn("127.0.0.1", json.dumps(evidence))
        cluster.remote_started.clear()
        cluster.stop()

    def test_managed_cluster_startup_failure_restores_prefect_environment(self) -> None:
        """A controlled service failure cleans up automatic client settings."""
        from util.shared_distributed_cluster import ManagedTuningCluster

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        cluster = ManagedTuningCluster(profile)
        cluster.preflight = MagicMock(return_value={})
        cluster._start_local = MagicMock()
        cluster._wait_for_prefect = MagicMock(
            side_effect=RuntimeError("Prefect did not start")
        )
        with patch.dict(
            os.environ,
            {
                "PREFECT_API_URL": "http://existing.example:4200/api",
                "PREFECT_SERVER_EPHEMERAL_ENABLED": "true",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "Prefect did not start"):
                cluster.start()
            self.assertEqual(
                os.environ["PREFECT_API_URL"],
                "http://existing.example:4200/api",
            )
            self.assertEqual(os.environ["PREFECT_SERVER_EPHEMERAL_ENABLED"], "true")

    def test_machine_inventory_validation_override_and_future_coordinator(self) -> None:
        """A synthetic Mac Studio topology resolves without production branching."""
        inventory = {
            "configuration_version": 1,
            "machines": {
                "mac_studio": {
                    "hostname": "Studio.example",
                    "project_root": "/srv/shape_fm_poc",
                    "capabilities": ["cpu", "mps"],
                },
                "worker": {
                    "hostname": "Worker.example",
                    "ssh_user": "researcher",
                    "project_root": "/opt/shape_fm_poc",
                    "capabilities": ["cpu", "cuda"],
                },
            },
            "services": {
                "prefect_port": 14200,
                "dask_scheduler_port": 18786,
                "dask_dashboard_port": 18787,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "machines.json"
            path.write_text(json.dumps(inventory), encoding="utf-8")
            allocations = {
                "mac_studio": {"enabled": True, "cpu_workers": 2},
                "worker": {"enabled": True, "cpu_workers": 3, "gpu_capacity": 1},
            }
            resolved = resolve_machine_environment(
                "mac_studio", allocations, path, environment={}
            )
            self.assertEqual(resolved.coordinator.machine.machine_id, "mac_studio")
            self.assertEqual(resolved.prefect_api_url,
                             "http://Studio.example:14200/api")
            self.assertEqual(resolved.topology(True)["total_workers"], 6)
            overridden = resolve_machine_environment(
                "mac_studio", allocations, path,
                environment={"SHAPEFM_COORDINATOR_ADDRESS": "192.0.2.8"},
            )
            self.assertEqual(overridden.scheduler_address, "tcp://192.0.2.8:18786")
            self.assertEqual(overridden.client_host, "192.0.2.8")
            self.assertEqual(dict(overridden.overrides),
                             {"coordinator_address": "192.0.2.8"})
            local_only = resolve_machine_environment(
                "mac_studio",
                {
                    "mac_studio": {"enabled": True, "cpu_workers": 2},
                    "worker": {"enabled": False, "cpu_workers": 3,
                               "gpu_capacity": 1},
                },
                path,
                environment={},
            )
            self.assertEqual(local_only.remotes, ())
            self.assertEqual(local_only.topology(False)["total_workers"], 2)
            disabled = {**allocations, "mac_studio": {"enabled": False}}
            with self.assertRaisesRegex(ValueError, "exactly one enabled"):
                resolve_machine_environment("mac_studio", disabled, path, environment={})
            with self.assertRaisesRegex(ValueError, "unknown profile machine"):
                resolve_machine_environment(
                    "mac_studio", {"missing": {"enabled": True}}, path, environment={}
                )
            with self.assertRaisesRegex(ValueError, "lacks CUDA"):
                resolve_machine_environment(
                    "mac_studio",
                    {"mac_studio": {"enabled": True, "gpu_capacity": 1}},
                    path,
                    environment={},
                )
            invalid = {**inventory, "unexpected": True}
            path.write_text(json.dumps(invalid), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unknown machine inventory"):
                resolve_machine_environment(
                    "mac_studio", allocations, path, environment={}
                )

    def test_operational_topology_does_not_change_scientific_configuration(self) -> None:
        """Coordinator address overrides remain outside scientific identity."""
        path = Path(__file__).resolve().parents[3] / (
            "config/experiments/poc2_m4_daily_100_directional_dtw_mantis_rf.json"
        )
        before = load_experiment_configuration(path)
        with patch.dict(
            os.environ, {"SHAPEFM_COORDINATOR_ADDRESS": "192.0.2.9"}, clear=False
        ):
            profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
            after = load_experiment_configuration(path)
        self.assertEqual(before.scientific_hash, after.scientific_hash)
        self.assertEqual(
            profile.machine_environment.prefect_api_url,
            "http://192.0.2.9:4200/api",
        )
        self.assertNotIn("192.0.2.9", json.dumps(after.scientific_configuration))

    def test_run_process_propagates_expected_gpu_worker_count_to_validation(self) -> None:
        """Dask process execution passes its expected GPU count to cluster validation."""
        coordinator = object.__new__(ExperimentCoordinator)
        coordinator.root = Path(__file__).resolve().parents[3]
        coordinator.configuration = load_experiment_configuration(
            coordinator.root / "config/experiments/poc2_m4_daily_100_standardised.json"
        )
        coordinator.config = coordinator.configuration.workflow
        coordinator.execution_hardware = MagicMock(return_value={})
        profile = resolve_execution_profile("poc2_seasonal_recovery")
        settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address="tcp://scheduler:8786",
            dask_expected_workers=38,
            dask_expected_gpu_workers=15,
        )
        client = MagicMock()
        with (
            patch("distributed.Client", return_value=client),
            patch(
                "util.shared_distributed_execution.validate_cluster",
                side_effect=RuntimeError("validation sentinel"),
            ) as validate,
            patch(
                "util.shared_experiment_execution.subprocess.run",
                return_value=MagicMock(stdout="revision\n"),
            ),
            patch("util.shared_distributed_execution.repository_source_manifest", return_value={"source": "hash"}),
            self.assertRaisesRegex(RuntimeError, "validation sentinel"),
        ):
            coordinator.run_process(
                "experiment",
                4,
                execution=profile,
                execution_settings=settings,
            )
        self.assertEqual(validate.call_args.kwargs["expected_gpu_workers"], 15)
        self.assertEqual(validate.call_args.kwargs["expected_topology"],
                         profile[0].distributed_topology(True))
        client.close.assert_called_once()

    def test_tuning_preflight_rejects_wrong_topology_and_stale_source(self) -> None:
        """CPU-only tuning checks exact host pools and synchronized dirty-tree content."""
        from util.shared_distributed_execution import (
            EXPECTED_DASK_VERSION,
            source_manifest_fingerprint,
            validate_tuning_cluster,
        )

        manifest = {"src/example.py": "digest"}
        base = {
            "source_manifest": source_manifest_fingerprint(manifest),
            "source_mismatches": [],
            "python_version": "3.12.14",
            "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION,
            "r_packages": {
                "R": "4.6.1",
                "forecast": "8.24.0",
                "jsonlite": "2.0.0",
                "tsfeatures": "1.1.1",
            },
            "resources": {"CPU": 1, "TUNING_R_SLOT": 1},
        }
        client = MagicMock()
        client.run.return_value = {
            "worker/mac": {**base, "hostname": "MacHost"},
            "worker/ubuntu": {
                **base,
                "hostname": "WSUbuntu1",
                "resources": {
                    **base["resources"],
                    "AUTOARIMA_R_SLOT": 1,
                },
            },
        }
        with patch("util.shared_distributed_execution.socket.gethostname", return_value="MacHost"):
            reports = validate_tuning_cluster(
                client,
                expected_workers=2,
                expected_mac_workers=1,
                expected_ubuntu_workers=1,
                expected_tuning_workers=2,
                timeout=5,
                expected_manifest=manifest,
            )
        self.assertEqual(len(reports), 2)
        client.run.return_value["worker/ubuntu"] = {
            **client.run.return_value["worker/ubuntu"],
            "source_mismatches": ["src/example.py"],
        }
        with patch("util.shared_distributed_execution.socket.gethostname", return_value="MacHost"):
            with self.assertRaisesRegex(RuntimeError, "stale source"):
                validate_tuning_cluster(
                    client,
                    expected_workers=2,
                    expected_mac_workers=1,
                    expected_ubuntu_workers=1,
                    expected_tuning_workers=2,
                    timeout=5,
                    expected_manifest=manifest,
                )
            with self.assertRaisesRegex(RuntimeError, "worker topology"):
                validate_tuning_cluster(
                    client,
                    expected_workers=3,
                    expected_mac_workers=1,
                    expected_ubuntu_workers=2,
                    expected_tuning_workers=3,
                    timeout=5,
                    expected_manifest=manifest,
                )

    def test_heavy_tuning_requires_exact_profile_or_recorded_local_exception(self) -> None:
        """Heavy execution rejects stored/local defaults and profile drift."""
        approved, _ = resolve_execution_profile("poc2_seasonal_recovery")
        distributed = ExecutionSettings(
            mode="dask", dask_scheduler_address="tcp://scheduler:8786"
        )
        validate_heavy_tuning_execution(approved, distributed, {})
        local, _ = resolve_execution_profile("sequential_safe")
        with self.assertRaisesRegex(RuntimeError, "approved distributed"):
            validate_heavy_tuning_execution(local, ExecutionSettings(), {})
        validate_heavy_tuning_execution(
            local,
            ExecutionSettings(mode="sequential"),
            {"local_heavy_exception": {"approval_reference": "researcher-approval/test"}},
        )
        with self.assertRaisesRegex(RuntimeError, "approval reference"):
            validate_heavy_tuning_execution(
                local,
                ExecutionSettings(mode="sequential"),
                {"local_heavy_exception": {"approval_reference": ""}},
            )

    def test_tuning_queues_use_all_eligible_ets_work_without_sampling_quota(self) -> None:
        """ETS queue retains every payload and consumes profile-owned limits."""
        from util.p04_04_seasonal_period_tuning import distributed_tuning_queue_groups

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        payloads = [
            {"id": f"ets-{index}", "tasks": [{"model": "ets"}]}
            for index in range(41)
        ] + [
            {"id": f"auto-{index}", "tasks": [{"model": "auto_arima"}]}
            for index in range(3)
        ]
        groups = distributed_tuning_queue_groups(payloads, profile, ("arguments",))
        self.assertEqual(len(groups["ets"][1]), 41)
        self.assertEqual(len(groups["auto_arima"][1]), 3)
        self.assertEqual(groups["ets"][2], {"TUNING_R_SLOT": 1})
        self.assertEqual(groups["auto_arima"][2], {"AUTOARIMA_R_SLOT": 1})
        self.assertEqual(groups["ets"][4], profile.dask_ets_max_in_flight)
        self.assertEqual(
            groups["auto_arima"][4], profile.dask_autoarima_max_in_flight
        )

    def test_memory_monitor_handles_transient_and_sustained_pressure(self) -> None:
        """Only sustained floor pressure terminates the registered owned process."""
        from util.shared_distributed_execution import (
            ResourceSafetyInterruption,
            TuningMemoryMonitor,
        )

        snapshots = iter(
            [
                {"available_gib": 8.0, "swap_used_gib": 1.0},
                {"available_gib": 2.0, "swap_used_gib": 1.0},
                {"available_gib": 8.0, "swap_used_gib": 1.0},
                {"available_gib": 2.0, "swap_used_gib": 1.0},
                {"available_gib": 2.0, "swap_used_gib": 1.0},
            ]
        )
        times = iter([0.0, 1.0, 2.0, 10.0])
        process = MagicMock(pid=321)
        process.poll.return_value = None
        monitor = TuningMemoryMonitor(
            minimum_available_gib=3,
            poll_interval_seconds=1,
            breach_grace_seconds=5,
            swap_growth_limit_gib=0.25,
            probe=lambda: next(snapshots),
            clock=lambda: next(times),
        )
        monitor.register_process(process)
        with patch("util.shared_distributed_execution.os.killpg") as terminate:
            monitor.sample_once()
            monitor.sample_once()
            monitor.sample_once()
            monitor.sample_once()
        terminate.assert_called_once_with(321, 15)
        process.wait.assert_called_once_with(timeout=2)
        with self.assertRaisesRegex(ResourceSafetyInterruption, "resource safety"):
            monitor.raise_if_unsafe()
        self.assertEqual(monitor.evidence()["safety_responses"], 1)

    def test_memory_monitor_detects_swap_growth_without_unrelated_kill(self) -> None:
        """Sustained swap growth records pressure without killing unowned work."""
        from util.shared_distributed_execution import (
            ResourceSafetyInterruption,
            TuningMemoryMonitor,
        )

        snapshots = iter(
            [
                {"available_gib": 8.0, "swap_used_gib": 1.0},
                {"available_gib": 8.0, "swap_used_gib": 1.5},
                {"available_gib": 8.0, "swap_used_gib": 1.5},
            ]
        )
        times = iter([0.0, 6.0])
        monitor = TuningMemoryMonitor(
            minimum_available_gib=3,
            poll_interval_seconds=1,
            breach_grace_seconds=5,
            swap_growth_limit_gib=0.25,
            probe=lambda: next(snapshots),
            clock=lambda: next(times),
        )
        with patch("util.shared_distributed_execution.os.killpg") as terminate:
            monitor.sample_once()
            monitor.sample_once()
        terminate.assert_not_called()
        with self.assertRaisesRegex(ResourceSafetyInterruption, "swap grew"):
            monitor.raise_if_unsafe()

    def test_memory_admission_waits_for_headroom_and_records_throttling(self) -> None:
        """Admission pauses on pressure and proceeds only after budget plus floor fits."""
        from util import shared_distributed_execution as distributed_execution

        memory = [
            MagicMock(available=5 * 1024**3),
            MagicMock(available=9 * 1024**3),
            MagicMock(available=9 * 1024**3),
            MagicMock(available=9 * 1024**3),
        ]
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(
                distributed_execution,
                "TUNING_RESERVATION_DIRECTORY",
                Path(directory),
            ),
            patch("psutil.virtual_memory", side_effect=memory),
            patch("psutil.swap_memory", return_value=MagicMock(used=0)),
            patch("util.shared_distributed_execution.time.sleep") as sleep,
        ):
            with distributed_execution.tuning_memory_reservation(
                3,
                3,
                timeout_seconds=10,
                poll_interval_seconds=0.25,
                breach_grace_seconds=5,
                swap_growth_limit_gib=0.25,
            ) as monitor:
                evidence = monitor.evidence()
        sleep.assert_called_once_with(0.25)
        self.assertEqual(evidence["throttled_seconds"], 0.25)
        self.assertEqual(evidence["available_gib_at_admission"], 9)

    def test_full_m4_daily_task_counts(self) -> None:
        """A 4,227-series M4 Daily run expands to the expected tasks per process."""
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
            {"task_id": "long", "context": [0] * 1025},
            {"task_id": "short-b", "context": [0] * 12},
            {"task_id": "medium", "context": [0] * 130},
            {"task_id": "short-a", "context": [0] * 9},
            {"task_id": "short-c", "context": [0] * 15},
        ]
        batches = _length_aware_batches(jobs, 2)
        self.assertTrue(all(len(batch) <= 2 for batch in batches))
        self.assertEqual(sum((batch for batch in batches), []), [
            jobs[3], jobs[1], jobs[4], jobs[2], jobs[0]
        ])
        for batch in batches:
            self.assertEqual(len({max(1, len(job["context"])).bit_length() for job in batch}), 1)


class PersistentWorkerTests(unittest.TestCase):
    """Purpose: Verify persistent worker protocols, isolation, batching, and Dask retries.

    Inputs: JSON worker requests, temporary scripts, local Dask clusters, and synthetic jobs.
    Outputs: Protocol and result assertions; temporary processes/files are closed or removed.
    """
    def test_relocated_r_workers_preserve_json_contracts(self) -> None:
        """R cleaning and forecasting entry points preserve their JSON response schemas."""
        root = Path(__file__).parents[3]
        clean_payload = {
            "action": "preprocess",
            "jobs": [
                {
                    "id": "standard",
                    "context": [1, None, 3, 4],
                    "mode": "standard",
                    "seasonality": 7,
                },
                {
                    "id": "robust",
                    "context": [1, 2, 100, 4, 5, 6, 7, 8],
                    "mode": "robust",
                    "seasonality": 7,
                },
            ],
        }
        forecast_payload = {
            "action": "forecast",
            "settings": load_experiment_configuration(
                root / "config/experiments/poc2_m4_daily_100_r_pool.json"
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
            ("src/r/02_01_preprocess_series.R", clean_payload),
            ("src/r/04_01_forecast_auto_arima.R", forecast_payload),
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
        standard, robust = clean["results"]
        self.assertEqual(standard["id"], "standard")
        self.assertEqual(standard["values"], [1, 2, 3, 4])
        self.assertEqual(standard["missing_count_before"], 1)
        self.assertEqual(standard["missing_count_after"], 0)
        self.assertEqual(robust["id"], "robust")
        self.assertEqual(robust["values"], list(range(1, 9)))
        expected = [13, 14, 15]
        forecast_result = forecast["results"][0]
        self.assertEqual(forecast_result["mean"], expected)
        self.assertEqual(forecast_result["median"], expected)
        self.assertEqual(forecast_result["quantiles"], [expected] * 9)
        self.assertEqual(forecast_result["requested_method_id"], "auto_arima_forec")
        self.assertEqual(forecast_result["executed_method_id"], "auto_arima_forec")
        self.assertFalse(forecast_result["fallback_used"])
        self.assertIsNone(forecast_result["fallback_reason"])
        self.assertEqual(forecast_result["provenance"]["package"], "forecast")
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
        source = (Path(__file__).parents[3] / "src/python/04_02_forecast_chronos.py").read_text(
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
            Path(__file__).parents[3] / "src/python/util/shared_distributed_execution.py"
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

        from util.shared_distributed_execution import run_batches, transform_batch
        from util.shared_transformations import transform

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

        from util.shared_distributed_execution import run_batches

        def succeed_on_retry(batch, retry_count=0):
            """Purpose: Model a Dask task that succeeds only after its initial attempt.

            Inputs: A list of job mappings and the scheduler-supplied integer retry count.
            Outputs: Job IDs and retry count, or an initial RuntimeError; no persistent side effects.
            """
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

    def test_simulated_worker_loss_remains_retryable(self) -> None:
        """A scheduler worker-loss error retries without losing completed input identity."""
        from types import SimpleNamespace

        from distributed import Client, LocalCluster
        from distributed.scheduler import KilledWorker

        from util.shared_distributed_execution import run_batches

        def survive_worker_loss(batch, retry_count=0):
            """Model one lost worker followed by a successful replacement attempt."""
            if retry_count == 0:
                raise KilledWorker(
                    "task/worker-loss",
                    SimpleNamespace(address="tcp://lost-worker:1"),
                    1,
                )
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
                        survive_worker_loss,
                        [[{"id": "task/worker-loss"}]],
                        resources={"CPU": 1},
                        max_in_flight=1,
                        retries=1,
                    )
                )
            self.assertEqual(result[0][0][0]["id"], "task/worker-loss")
            self.assertEqual(result[0][1]["ids"], ["task/worker-loss"])
            self.assertEqual(result[0][1]["retry_count"], 1)
        finally:
            cluster.close()


class CalibrationSafetyTests(unittest.TestCase):
    """Purpose: Verify calibration equivalence baselines and safe-setting selection.

    Inputs: Synthetic forecasts, calibration candidates, and in-memory worker doubles.
    Outputs: Equivalence and recommendation assertions; no process, database, or file effects.
    """
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
            """Purpose: Emulate Chronos while recording requested inference batch sizes.

            Inputs: Request mappings containing batch size and a list of forecast jobs.
            Outputs: Result mappings with constant forecast arrays; mutates only ``batch_sizes``.
            """
            def __init__(self) -> None:
                """Purpose: Initialize request-history state for the fake worker.

                Inputs: None.
                Outputs: An empty mutable ``batch_sizes`` list; no external side effects.
                """
                self.batch_sizes = []

            def request(self, message):
                """Purpose: Record one Chronos-shaped request and synthesize its response.

                Inputs: A mapping with ``inference_batch_size`` and ``jobs`` entries.
                Outputs: A result mapping with one forecast per job; appends to request history.
                """
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


class OrdinaryManifestTests(unittest.TestCase):
    """Verify normal cluster validation accepts exact dirty source, not unknown code."""

    def test_exact_dirty_manifest_and_mismatch_rejection(self):
        """Retain dependency checks while replacing clean-Git with byte identity."""
        from util.shared_distributed_execution import validate_cluster, EXPECTED_DASK_VERSION

        manifest = {"src/python/00_main.py": "approved", "uv.lock": "locked"}
        report = {
            "git_commit": "revision", "git_dirty": True, "source_manifest": manifest,
            "python_version": "3.12.14", "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION, "configuration_hash": "config",
            "gift_eval_revision": "gift", "resources": {},
            "r_packages": {"R": "4.6.1", "renv": "1.2.4", "forecast": "8.24.0", "jsonlite": "2.0.0"},
            "chronos": {"chronos_forecasting": "2.2.2", "checkpoint_revision": "model",
                        "checkpoint_present": True},
        }
        client = MagicMock()
        client.run.return_value = {"worker": report}
        options = dict(expected_workers=1, timeout=1, expected_commit="revision",
                       expected_configuration_hash="config", expected_gift_eval_revision="gift",
                       expected_chronos_revision="model", expected_chronos_version="2.2.2",
                       chronos_repository="repo", chronos_environment="env",
                       gift_eval_source_directory="gift", require_gpu=False,
                       expected_gpu_name=None, expected_manifest=manifest)
        self.assertEqual(validate_cluster(client, **options), {"worker": report})
        for altered in ({**manifest, "unexpected.py": "unknown"},
                        {**manifest, "uv.lock": "stale"}, {}):
            report["source_manifest"] = altered
            with self.assertRaisesRegex(RuntimeError, "source manifest mismatch"):
                validate_cluster(client, **options)
        report["source_manifest"] = manifest
        report["r_packages"]["forecast"] = "wrong"
        with self.assertRaisesRegex(RuntimeError, "R forecast"):
            validate_cluster(client, **options)

    def test_normal_preflight_rejects_profile_topology_mismatch(self):
        """Correct totals alone cannot hide a CPU pool on the wrong host."""
        from util.shared_distributed_execution import validate_cluster, EXPECTED_DASK_VERSION
        import socket

        report = {
            "hostname": socket.gethostname(), "git_commit": "revision", "git_dirty": False,
            "python_version": "3.12.14", "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION, "configuration_hash": "config",
            "gift_eval_revision": "gift", "resources": {"CPU": 1},
            "r_packages": {"R": "4.6.1", "renv": "1.2.4", "forecast": "8.24.0", "jsonlite": "2.0.0"},
            "chronos": {"chronos_forecasting": "2.2.2", "checkpoint_revision": "model",
                        "checkpoint_present": True},
        }
        client = MagicMock()
        client.run.return_value = {"worker": report}
        options = dict(expected_workers=1, timeout=1, expected_commit="revision",
                       expected_configuration_hash="config", expected_gift_eval_revision="gift",
                       expected_chronos_revision="model", expected_chronos_version="2.2.2",
                       chronos_repository="repo", chronos_environment="env",
                       gift_eval_source_directory="gift", require_gpu=False,
                       expected_gpu_name=None, expected_gpu_workers=0,
                       expected_topology={"mac_cpu_workers": 1, "ubuntu_cpu_workers": 0,
                                          "ubuntu_gpu_workers": 0})
        validate_cluster(client, **options)
        report["hostname"] = "wrong-host"
        with self.assertRaisesRegex(RuntimeError, "CPU/GPU topology"):
            validate_cluster(client, **options)
        options["expected_topology"].update(mac_cpu_workers=0, ubuntu_cpu_workers=1)
        with self.assertRaisesRegex(RuntimeError, "AutoARIMA resource routing"):
            validate_cluster(client, **options)
        report["resources"]["AUTOARIMA_R_SLOT"] = 1
        validate_cluster(client, **options)
        report["hostname"] = socket.gethostname()
        options["expected_topology"].update(mac_cpu_workers=1, ubuntu_cpu_workers=0)
        with self.assertRaisesRegex(RuntimeError, "AutoARIMA resource routing"):
            validate_cluster(client, **options)


if __name__ == "__main__":
    unittest.main()
