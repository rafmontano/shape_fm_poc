"""Non-scientific CLI/provider wiring for the shared six-process entry point.

Mocked execution and rendering verify canonical defaults and JSON contracts;
no scientific process, native calculation or output regeneration is invoked.
"""

import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import experiment
from p02_preprocess.application import resolve_r_period
from util.runtime import CODE_ROOT, FinalResearchSummary


def _calculation_worker(item):
    """Return execution placement without running a scientific provider."""
    from distributed import get_worker
    return {"worker": get_worker().name}


def _completion_worker(item):
    """Coordinate synthetic work with Dask events, without scientific providers."""
    from distributed import Event
    started = time.monotonic()
    operation = item.payload.get("operation")
    released = None
    if operation == "wait":
        released = Event(item.payload["event"]).wait(timeout=10)
    elif operation == "signal":
        Event(item.payload["event"]).set()
    elif operation == "fail":
        raise ValueError("synthetic atomic failure")
    time.sleep(0.1)
    return {"started": started, "finished": time.monotonic(), "released": released}


class ResearcherInterfaceTests(unittest.TestCase):
    """Protect researcher-visible defaults after retiring the old entry points."""

    def test_bounded_refill_and_failure_restart(self):
        """Refill precedes slow completion; failures drain commits, not new work."""
        from distributed import Client, Event
        from util.runtime import ExperimentRequest, WorkItem, open_experiment

        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, \
                    Client(n_workers=4, threads_per_worker=1, processes=False,
                           dashboard_address=None, set_as_default=False) as client:
                root = Path(directory)
                (root / "config").mkdir()
                (root / "config/experiment.json").write_text((CODE_ROOT / "config/experiment.json").read_text())
                context = open_experiment(ExperimentRequest((6,), root, execution_profile="mvp_two_machine"))
                try:
                    context.configuration["execution"]["default"]["work_group_size"] = 2 if failure else 1
                    context.execution.settings = {"max_in_flight": 2,
                        "scheduler_address": client.scheduler.address}
                    event = f"completion-{root.name}"
                    operations = ([None, "fail", "wait", None, None, None] if failure else
                                  ["wait", None, "signal", None, None, None])
                    items = [WorkItem(str(i), {"operation": operation, "event": event})
                             for i, operation in enumerate(operations)]
                    repository = context.repository
                    pending = repository.plan("06.03", items)

                    def accept(rows):
                        repository.accept("06.03", rows)
                        if failure and any(key == "0" for key, _ in rows):
                            Event(event, client=client).set()

                    def execute(work):
                        context.execution.run(subflow_id="06.03", items=work,
                            calculate=_completion_worker, accept=accept, on_progress=lambda snapshot: None)

                    if failure:
                        with self.assertRaisesRegex(ValueError, "synthetic atomic failure"):
                            execute(pending)
                        before = repository.rows("06.03")
                        self.assertEqual(set(before), {"0", "2", "3"})
                        self.assertTrue(before["2"]["released"])
                        self.assertTrue(items[4].payload)
                        resumed = repository.plan("06.03", [WorkItem(str(i), {}) for i in range(6)])
                        self.assertEqual({item.key for item in resumed}, {"1", "4", "5"})
                        execute(resumed)
                        after = repository.rows("06.03")
                        self.assertEqual({key: after[key] for key in before}, before)
                    else:
                        execute(pending)
                        rows = repository.rows("06.03")
                        self.assertTrue(rows["0"]["released"])
                        self.assertLess(rows["2"]["started"], rows["0"]["finished"])
                        events = [(row["started"], 1) for row in rows.values()]
                        events += [(row["finished"], -1) for row in rows.values()]
                        active = peak = 0
                        for _, change in sorted(events):
                            active += change
                            peak = max(peak, active)
                        self.assertEqual(peak, 2)
                    self.assertEqual(repository.counts("06.03"), (6, 6))
                finally:
                    context.close()

    def test_local_only_cluster_starts_without_a_remote_peer(self):
        """A local scheduler remains reachable without a Mac or LAN endpoint."""
        from urllib.parse import urlsplit
        from util.distributed_cluster import DistributedCluster

        profiles = json.loads((CODE_ROOT / "config/execution_profiles.json").read_text())
        profile = profiles["mvp_ubuntu_only"]
        worker = {**profile["workers"][0], "count": 1}
        inventory = json.loads((CODE_ROOT / "config/machines.json").read_text())
        inventory["services"]["dask_scheduler_port"] = 0
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / "config/machines.json").write_text(json.dumps(inventory))
            with patch("util.distributed_cluster.CODE_ROOT", root):
                cluster = DistributedCluster({**profile, "workers": [worker]})
                try:
                    address = cluster.start()
                    self.assertEqual(urlsplit(address).hostname, "127.0.0.1")
                    workers = cluster.client.scheduler_info()["workers"]
                    self.assertEqual({worker["host"] for worker in workers.values()}, {"127.0.0.1"})
                    self.assertEqual(cluster.client.submit(sum, [2, 5, 11]).result(), 18)
                finally:
                    cluster.close()

    def test_sensitivity_placement_and_accepted_candidate_exclusion(self):
        """Pending bounded groups stay on the coordinator and exclude accepted keys."""
        from distributed import Client, Scheduler, SpecCluster, Worker
        from util.distributed_cluster import DistributedCluster
        from util.runtime import ExperimentRequest, open_experiment

        profile = json.loads((CODE_ROOT / "config/execution_profiles.json").read_text())["mvp_two_machine"]
        for coordinator in ("macbook_pro", "ubuntu_primary"):
            allocations = DistributedCluster({**profile, "coordinator": coordinator}).allocations
            local_cpu = [allocation for allocation in allocations
                         if "COORDINATOR_CPU=1" in allocation["resources"]]
            self.assertEqual({allocation["machine"] for allocation in local_cpu}, {coordinator})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / "config/experiment.json").write_text((CODE_ROOT / "config/experiment.json").read_text())
            context = open_experiment(ExperimentRequest((6,), root, execution_profile="mvp_two_machine"))
            try:
                service = context.evaluate
                planned = service.pending_diagnostic_candidates(None, [])
                stored = [(item.key, {"already": "accepted"}) for item in planned[:-9]]
                service.accept_diagnostic_candidates(stored)
                pending = service.pending_diagnostic_candidates(None, [])
                self.assertEqual(len(pending), 9)
                self.assertFalse({item.key for item in pending} & {key for key, _ in stored})
                with SpecCluster(scheduler={"cls": Scheduler, "options": {"dashboard_address": None}},
                        workers={"mac": {"cls": Worker, "options": {"resources": {"CPU": 1}}},
                                 "ubuntu": {"cls": Worker, "options": {"resources": {"CPU": 1, "COORDINATOR_CPU": 1}}}}) as cluster:
                    with Client(cluster):
                        context.execution.settings = {
                            "scheduler_address": cluster.scheduler_address, "subflows": profile["subflows"]}
                        context.execution.run(subflow_id="06.03", items=pending,
                            calculate=_calculation_worker, accept=service.accept_diagnostic_candidates,
                            on_progress=lambda snapshot: None)
                rows = context.repository.rows("06.03")
                self.assertEqual({rows[item.key]["worker"] for item in pending}, {"ubuntu"})
                self.assertTrue(all(rows[key] == result for key, result in stored))
                self.assertTrue(all(not item.payload for item in pending))
            finally:
                context.close()

    def test_normal_run_uses_canonical_repository_root(self):
        """The ordinary command must not select a review/test experiment package."""
        output = io.StringIO()
        with patch.object(experiment, "run_experiment", return_value="stored-id") as execute:
            with redirect_stdout(output):
                self.assertEqual(experiment.main(["run", "--processes", "5-6"]), 0)
        request = execute.call_args.args[0]
        self.assertEqual(request.root, CODE_ROOT)
        self.assertEqual(request.processes, (5, 6))
        self.assertEqual(json.loads(output.getvalue()), "stored-id")

    def test_export_returns_serializable_summary_from_publication(self):
        """Export exposes metrics and declared paths, not an unserializable object."""
        summary = FinalResearchSummary({"model.da": 0.7}, ("results/tables/table_1.csv",))
        output = io.StringIO()
        with patch("p06_evaluate.publication.publish_stored_outputs", return_value=summary):
            with redirect_stdout(output):
                self.assertEqual(experiment.main(["export"]), 0)
        self.assertEqual(json.loads(output.getvalue()),
                         {"metrics": {"model.da": 0.7}, "artifacts": ["results/tables/table_1.csv"]})

    def test_failed_r_request_identifies_large_integer_without_concealing_error(self):
        """A malformed nested field remains rejected and is locatable without its decimal value."""
        from util.providers import run_r

        value = 1 << 16000
        payload = {"series": [{"context": [1.25, value]}]}
        with self.assertRaisesRegex(ValueError, "integer string conversion") as failure:
            run_r("unused.R", payload, timeout=1, threads=1)
        notes = "\n".join(failure.exception.__notes__)
        self.assertIn("$['series'][0]['context'][1]", notes)
        self.assertIn("type=int", notes)
        self.assertIn("size_bits=16001", notes)
        self.assertIs(payload["series"][0]["context"][1], value)

    def test_period_transport_uses_existing_process_owned_cli(self):
        """The pinned-environment request preserves frequency and optional override."""
        configuration = json.loads((CODE_ROOT / "config/experiment.json").read_text())
        for override in (None, 7):
            with self.subTest(override=override):
                configuration["pipeline"]["r_period_override"] = override
                response = {"frequency": "W", "r_period": override or 52}
                with patch("p02_preprocess.application.subprocess.run", return_value=subprocess.CompletedProcess(
                        [], 0, json.dumps(response), "")) as invoke:
                    self.assertEqual(resolve_r_period(configuration, "W"), response)
                command = invoke.call_args.args[0]
                self.assertEqual(command[0], str(CODE_ROOT / "environments/gift-eval/.venv/bin/python"))
                script = CODE_ROOT / "src/python/p02_preprocess/period.py"
                self.assertTrue(script.is_file())
                self.assertEqual(command[1:], [str(script), "--frequency", "W"] +
                                 ([] if override is None else ["--override", "7"]))

    def test_model_publication_uses_semantic_paths_without_changing_bytes(self):
        """Already-produced bytes publish by model/frequency/horizon, not run ID."""
        from p04_forecast.application import Application

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accepted = {}
            context = SimpleNamespace(request=SimpleNamespace(root=root), configuration={},
                repository=SimpleNamespace(accept=lambda subflow, rows: accepted.update(rows)))
            content = b"opaque provider bytes; no model is fitted or deserialized"
            cases = [("directional_dtw", None, "model.pickle"),
                     ("directional_mantis_rf", 1, "horizon_01.pickle"),
                     ("directional_mantis_rf", 14, "horizon_14.pickle")]
            for model, horizon, filename in cases:
                key = f"D/{model}/{horizon or 'all'}"
                source = root / "temporary.pickle"
                source.write_bytes(content)
                Application(context).accept_decision_models([(key, {"model": model, "horizon": horizon,
                    "temporary_path": str(source), "host": socket.gethostname()})])
                relative = f"models/{model}/daily/{filename}"
                self.assertEqual(accepted[key]["path"], relative)
                self.assertEqual((root / relative).read_bytes(), content)
                self.assertFalse(source.exists())
            self.assertEqual(len(list((root / "models").rglob("*.pickle"))), 3)
            self.assertEqual(list(root.rglob("*.partial")), [])

    def test_native_session_keeps_concurrent_responses_with_their_requests(self):
        """Concurrent callers cannot consume another request's provider response."""
        from concurrent.futures import ThreadPoolExecutor
        from p04_forecast.providers import NativeSession

        code = ("import json,sys; print(json.dumps({'type':'ready'}),flush=True); "
                "exec(\"for line in sys.stdin:\\n r=json.loads(line)\\n "
                "if r.get('command')=='shutdown': break\\n print(json.dumps(r),flush=True)\")")
        session = NativeSession([sys.executable, "-u", "-c", code], 10)
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda i: session.request({"value": i}), range(20)))
            self.assertEqual(results, [{"value": i} for i in range(20)])
        finally:
            session.close()

    def test_researcher_import_sets_canonical_prefect_home_before_settings_load(self):
        """A fresh entry-point import resolves Prefect settings inside hidden runtime."""
        response = subprocess.run([sys.executable, "-B", "-c",
            "import experiment; from prefect.settings import get_current_settings; "
            "print(get_current_settings().home)"], cwd=CODE_ROOT, capture_output=True,
            text=True, check=True, env={**os.environ, "PREFECT_HOME": "/unused-prefect-home",
                                      "PYTHONPATH": str(CODE_ROOT / "src/python")})
        self.assertEqual(Path(response.stdout.strip()), CODE_ROOT / ".runtime/prefect")


if __name__ == "__main__":
    unittest.main()
