# ==============================================================================
# test_forecast_safety.py
#
# Purpose: Controlled unit simulations for ordinary forecast provider safeguards.
# Inputs: Profile objects and in-memory native-worker/probe doubles.
# Outputs: Assertions for pressure, release, OOM subdivision, and terminal failure.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_forecast_safety
# ==============================================================================

"""Focused safety tests; simple functions avoid needless stateful test objects."""

from pathlib import Path
import multiprocessing
import sys
import tempfile
import time
import unittest
from unittest.mock import patch, MagicMock

from util import distributed_execution
from util.execution_profiles import PersistentChronosWorker, resolve_execution_profile
from util.forecast_provider import (
    DistributedForecastProvider,
    ForecastSafetyPolicy,
    LocalAutoArimaProvider,
)


# Bounded practical exception: spawn targets must be importable top-level functions;
# tiny functions are clearer than stateful test classes for this process-only fixture.
def _flock_lifecycle_child(lock_path, start, startup_count, startup_max,
                           inference_barrier, inference_overlap, errors):
    """Exercise the real machine flock while replacing only resource telemetry."""
    class SafeMonitor:
        admission = {}

        def sample_once(self):
            return None

        def raise_if_unsafe(self):
            return None

        def raise_if_current_pressure(self):
            return None

    try:
        distributed_execution.CHRONOS_GPU_STARTUP_LOCK = Path(lock_path)
        start.wait(5)
        with distributed_execution.gpu_startup_admission(SafeMonitor(), 3, 0.005):
            with startup_count.get_lock(), startup_max.get_lock():
                startup_count.value += 1
                startup_max.value = max(startup_max.value, startup_count.value)
            time.sleep(0.15)
            with startup_count.get_lock():
                startup_count.value -= 1
        # Reaching a common barrier after releasing the startup lock proves that
        # inference is not accidentally enclosed by the machine-wide flock.
        inference_barrier.wait(3)
        with inference_overlap.get_lock():
            inference_overlap.value += 1
    except BaseException as error:
        errors.put(repr(error))


def _hold_flock_child(lock_path, entered, release):
    """Hold the real startup lock until the parent has checked timeout behavior."""
    import fcntl

    with Path(lock_path).open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        entered.set()
        release.wait(5)


class _ChronosWorker:
    """Return configured in-memory responses through the native worker protocol."""

    def __init__(self, responses):
        """Bind a finite response sequence; no subprocess or GPU is involved."""
        self.responses = iter(responses)
        self.ready = {"type": "ready", "accelerator_backend": "test"}

    def request(self, payload, timeout):
        """Return the next deterministic protocol response with requested identities."""
        response = next(self.responses)
        if response["type"] == "result":
            response = {**response, "results": [{"id": job["id"]} for job in payload["jobs"]]}
        return response


def _result() -> dict:
    """Construct a minimal successful native response for controlled OOM tests."""
    return {"type": "result", "effective_batch_size": 1, "inference_seconds": 0.1,
            "peak_process_memory_bytes": 1, "accelerator_memory": {"available_bytes": 9}}


def _accelerator_safety() -> dict:
    """Return complete positive controls for synthetic protected GPU work."""
    return {"minimum_available_gib": 4, "host_minimum_available_gib": 16,
            "admission_timeout_seconds": 1, "poll_interval_seconds": 0.01,
            "breach_grace_seconds": 0.01, "swap_growth_limit_gib": 0.25}


class ForecastSafetyTests(unittest.TestCase):
    """Exercise provider contracts and safety responses without real pressure."""

    def test_real_flock_serializes_startup_but_not_inference(self) -> None:
        """Spawned workers share the OS startup lock and overlap after releasing it."""
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            lock_path = str(Path(directory) / "chronos.lock")
            start = context.Event()
            startup_count = context.Value("i", 0)
            startup_max = context.Value("i", 0)
            inference_overlap = context.Value("i", 0)
            barrier = context.Barrier(2)
            errors = context.Queue()
            arguments = (lock_path, start, startup_count, startup_max, barrier,
                         inference_overlap, errors)
            processes = [context.Process(target=_flock_lifecycle_child, args=arguments)
                         for _ in range(2)]
            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(8)
                self.assertEqual(process.exitcode, 0)
            self.assertTrue(errors.empty(), errors.get() if not errors.empty() else "")
            self.assertEqual(startup_max.value, 1)
            self.assertEqual(inference_overlap.value, 2)

    def test_real_flock_times_out_and_releases_after_error(self) -> None:
        """Contention times out, and an exception holder releases the same real lock."""
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "chronos.lock"
            entered, release = context.Event(), context.Event()
            holder = context.Process(target=_hold_flock_child,
                                     args=(str(lock_path), entered, release))
            holder.start()
            self.assertTrue(entered.wait(3))
            monitor = MagicMock()
            distributed_execution.CHRONOS_GPU_STARTUP_LOCK = lock_path
            with self.assertRaisesRegex(
                    distributed_execution.ResourceSafetyInterruption, "timed out"):
                with distributed_execution.gpu_startup_admission(monitor, 0.05, 0.005):
                    self.fail("contended lock must not admit")
            release.set()
            holder.join(3)
            self.assertEqual(holder.exitcode, 0)
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                with distributed_execution.gpu_startup_admission(monitor, 1, 0.005):
                    raise RuntimeError("startup failed")
            with distributed_execution.gpu_startup_admission(monitor, 1, 0.005):
                pass

    def test_monitor_pressure_during_startup_and_inference_cancels_owned_child(self):
        """Sustained pressure cancels the registered child in either lifecycle phase."""
        for phase in ("startup", "inference"):
            snapshots = iter([
                {"available_gib": 8, "swap_used_gib": 0,
                 "accelerator_available_gib": 8},
                {"available_gib": 8, "swap_used_gib": 0,
                 "accelerator_available_gib": 1},
                {"available_gib": 8, "swap_used_gib": 0,
                 "accelerator_available_gib": 1},
            ])
            monitor = distributed_execution.TuningMemoryMonitor(
                minimum_available_gib=4, minimum_accelerator_available_gib=4,
                poll_interval_seconds=1, breach_grace_seconds=1,
                swap_growth_limit_gib=1, probe=lambda: next(snapshots),
                clock=iter([0, 2]).__next__)
            child = MagicMock(pid=701 if phase == "startup" else 702)
            child.poll.return_value = None
            monitor.register_process(child)
            with patch.object(distributed_execution.os, "killpg") as kill_group:
                monitor.sample_once()
                monitor.sample_once()
            kill_group.assert_called_once_with(child.pid, 15)
            with self.assertRaises(distributed_execution.ResourceSafetyInterruption):
                monitor.raise_if_unsafe()

    def test_probe_failure_before_registration_cancels_only_later_owned_child(self):
        """The unsafe-before-register race is closed without targeting unrelated PIDs."""
        monitor = distributed_execution.TuningMemoryMonitor(
            minimum_available_gib=4, poll_interval_seconds=1,
            breach_grace_seconds=1, swap_growth_limit_gib=1,
            probe=MagicMock(side_effect=[{"available_gib": 8, "swap_used_gib": 0},
                                         RuntimeError("telemetry lost")]))
        child = MagicMock(pid=811)
        child.poll.return_value = None
        with patch.object(distributed_execution.os, "killpg") as kill_group:
            monitor.sample_once()
            kill_group.assert_not_called()
            monitor.register_process(child)
        kill_group.assert_called_once_with(811, 15)

    def test_native_startup_failure_force_closes_and_clears_monitor(self) -> None:
        """A real malformed native handshake leaves no process registered or alive."""
        script = "import json; print(json.dumps({'type':'failed'}), flush=True)"
        monitor = MagicMock()
        monitor.raise_if_unsafe.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad_worker.py"
            path.write_text(script, encoding="utf-8")
            worker = PersistentChronosWorker(
                [sys.executable, str(path)], startup_timeout=2, memory_monitor=monitor)
            with self.assertRaisesRegex(RuntimeError, "failed to start"):
                worker.start()
            self.assertIsNone(worker.process)
            monitor.register_process.assert_called_once()
            monitor.clear_process.assert_called_once()
            self.assertIsNotNone(monitor.clear_process.call_args.args[0].poll())

    def test_complete_policy_reaches_native_start_through_provider(self) -> None:
        """The actual distributed provider passes complete policy to worker startup."""
        provider = DistributedForecastProvider(
            {}, "r.py", 1, 1,
            {"repository": "repo", "revision": "rev", "dtype": "float32",
             "cross_learning": False, "predict_batches_jointly": False},
            (0.5,), "cuda", 1, ".venv", "worker.py", 1, 1,
            ForecastSafetyPolicy({}, _accelerator_safety()))
        worker = _ChronosWorker([_result()])
        safe = {"available_gib": 32, "swap_used_gib": 0,
                "accelerator_available_gib": 8}
        run_context = MagicMock()
        run_context.task_run.run_count = 1
        with (patch("prefect.context.get_run_context", return_value=run_context),
              patch.object(distributed_execution, "_gpu_host_probe", return_value=safe),
              patch.object(distributed_execution, "_get_chronos",
                           return_value=(worker, 1)) as start,
              patch.object(distributed_execution, "_close_chronos"),
              patch.object(distributed_execution, "_worker_provenance", return_value={})):
            response = provider.forecast("chronos_2", [{"id": "a", "horizon": 1}])
        self.assertEqual(response["results"], [{"id": "a"}])
        self.assertIsInstance(start.call_args.args[-1],
                              distributed_execution.TuningMemoryMonitor)

    def test_policy_is_profile_derived_and_serializable(self) -> None:
        """The approved profile supplies every native forecast safety threshold."""
        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        policy = ForecastSafetyPolicy.from_profile(profile, "mac-host")
        self.assertEqual(policy.autoarima["fit_budget_gib"], 12)
        self.assertEqual(policy.autoarima["ubuntu_minimum_available_gib"], 16)
        self.assertEqual(policy.accelerator["minimum_available_gib"], 4)
        self.assertEqual(policy.accelerator["host_minimum_available_gib"], 16)
        self.assertEqual(policy.accelerator["breach_grace_seconds"], 5)

    def test_local_provider_contract_validates_model(self) -> None:
        """Local providers expose the same forecast(model, batch) operation."""
        provider = LocalAutoArimaProvider(
            lambda payload: {"results": [], "packages": {}}, {}
        )
        with self.assertRaisesRegex(ValueError, "does not support"):
            provider.forecast("chronos_2", [])

    def test_r_pressure_and_failure_release_reservation(self) -> None:
        """Reuse real admission accounting with simulated host memory and native failure."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with (
                patch.object(distributed_execution, "TUNING_RESERVATION_DIRECTORY", path),
                patch("psutil.virtual_memory", return_value=MagicMock(available=1)),
                patch.object(distributed_execution.time, "sleep"),
                patch.object(distributed_execution.time, "monotonic", side_effect=[0, 2]),
                self.assertRaisesRegex(
                    distributed_execution.ResourceSafetyInterruption, "timed out"
                ),
            ):
                with distributed_execution.tuning_memory_reservation(3, 12, timeout_seconds=1):
                    pass
            with (
                patch.object(distributed_execution, "TUNING_RESERVATION_DIRECTORY", path),
                patch("psutil.virtual_memory", return_value=MagicMock(available=100 * 1024**3)),
                patch.object(distributed_execution, "TuningMemoryMonitor") as monitor,
            ):
                with self.assertRaisesRegex(RuntimeError, "native fault"):
                    with distributed_execution.tuning_memory_reservation(3, 12):
                        self.assertEqual(len(list(path.glob("*.json"))), 1)
                        raise RuntimeError("native fault")
                monitor.return_value.stop.assert_called_once()
                self.assertEqual(list(path.glob("*.json")), [])

    def test_gpu_pressure_admission_starts_no_model(self):
        """An initially unsafe shared GPU fails closed before model startup."""
        safety = _accelerator_safety()
        with (patch.object(distributed_execution, "_gpu_host_probe",
                           return_value={"available_gib": 32, "swap_used_gib": 0,
                                         "accelerator_available_gib": 1}),
              patch.object(distributed_execution, "_get_chronos") as start,
              self.assertRaises(distributed_execution.ResourceSafetyInterruption)):
            distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}], "repo", "rev", [0.5], "cuda",
                "float32", False, False, 1, ".venv", "worker.py", 1, 1,
                accelerator_safety=safety,
            )
        start.assert_not_called()

    def test_gpu_probe_failure_fails_closed_before_start(self):
        """Unavailable NVIDIA telemetry is a pressure interruption, not a model retry."""
        safety = _accelerator_safety()
        with (patch.object(distributed_execution, "_gpu_host_probe",
                           side_effect=RuntimeError("probe unavailable")),
              patch.object(distributed_execution, "_get_chronos") as start,
              self.assertRaisesRegex(distributed_execution.ResourceSafetyInterruption,
                                    "probe failed closed")):
            distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}], "repo", "rev", [0.5], "cuda",
                "float32", False, False, 1, ".venv", "worker.py", 1, 1,
                accelerator_safety=safety)
        start.assert_not_called()

    def test_protected_gpu_success_releases_model_and_monitor(self):
        """The startup gate is short lived and protected models close after inference."""
        worker = _ChronosWorker([_result()])
        safe = {"available_gib": 32, "swap_used_gib": 0,
                "accelerator_available_gib": 8}
        with (patch.object(distributed_execution, "_gpu_host_probe", return_value=safe),
              patch.object(distributed_execution, "_get_chronos", return_value=(worker, 1)),
              patch.object(distributed_execution, "_close_chronos") as close,
              patch.object(distributed_execution, "_worker_provenance", return_value={})):
            response = distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}], "repo", "rev", [0.5], "cuda",
                "float32", False, False, 1, ".venv", "worker.py", 1, 1,
                accelerator_safety=_accelerator_safety())
        self.assertEqual(response["results"], [{"id": "a"}])
        close.assert_called_once()
        self.assertEqual(response["worker"]["accelerator_safety"]["unsafe_reason"], None)

    def test_protected_gpu_oom_reacquires_startup_admission(self):
        """Each OOM replacement starts through the gate without adding model retries."""
        oom = {"type": "error", "error_kind": "out_of_memory", "error": "oom"}
        worker = _ChronosWorker([oom, _result(), _result()])
        safe = {"available_gib": 32, "swap_used_gib": 0,
                "accelerator_available_gib": 8}
        with (patch.object(distributed_execution, "_gpu_host_probe", return_value=safe),
              patch.object(distributed_execution, "_get_chronos",
                           return_value=(worker, 1)) as starts,
              patch.object(distributed_execution, "_close_chronos"),
              patch.object(distributed_execution, "_worker_provenance", return_value={})):
            response = distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}, {"id": "b", "horizon": 1}],
                "repo", "rev", [0.5], "cuda", "float32", False, False, 1,
                ".venv", "worker.py", 1, 1, accelerator_safety=_accelerator_safety())
        self.assertEqual(starts.call_count, 3)
        self.assertEqual(response["worker"]["oom_subdivision_depth"], 1)

    def test_distributed_chronos_oom_subdivides_and_singleton_is_terminal(self) -> None:
        """OOM subdivision is bounded by singleton size."""
        oom = {"type": "error", "error_kind": "out_of_memory", "error": "simulated OOM"}
        worker = _ChronosWorker([oom, _result(), _result()])
        with (
            patch.object(distributed_execution, "_get_chronos", return_value=(worker, 1)),
            patch.object(distributed_execution, "_close_chronos"),
            patch.object(distributed_execution, "_worker_provenance", return_value={}),
        ):
            response = distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}, {"id": "b", "horizon": 1}],
                "repo", "rev", [0.5], "cuda", "float32", False, False, 1,
                ".venv", "worker.py", 1, 1,
            )
        self.assertEqual([item["id"] for item in response["results"]], ["a", "b"])
        self.assertEqual(response["worker"]["oom_subdivision_depth"], 1)

        terminal = _ChronosWorker([oom])
        with (
            patch.object(distributed_execution, "_get_chronos", return_value=(terminal, 1)),
            patch.object(distributed_execution, "_close_chronos"),
            self.assertRaisesRegex(RuntimeError, "simulated OOM"),
        ):
            distributed_execution.chronos_batch(
                [{"id": "a", "horizon": 1}], "repo", "rev", [0.5], "cuda",
                "float32", True, True, 1, ".venv", "worker.py", 1, 1,
            )
