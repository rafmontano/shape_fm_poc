"""Process 04 provider transport with worker-local foundation-model reuse.

Pure scientific requests enter pinned R/Python environments; native results
return to the coordinator. No provider receives DuckDB writers or MLflow state.
"""

from __future__ import annotations

import atexit
import base64
import hashlib
import json
import os
import select
import subprocess
import threading
from typing import Any

from util.runtime import CODE_ROOT


class NativeSession:
    """Own a persistent native provider; offline assets load once per worker.

    This is synchronous subprocess transport, not another scheduler or retry
    engine. Native failures propagate without device/model fallback.
    """

    def __init__(self, command: list[str], timeout: float):
        """Start the pinned JSON-lines provider and require its ready response."""
        self.timeout = timeout
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            text=True, bufsize=1, cwd=CODE_ROOT, env={**os.environ,
                "PYTHONPATH": str(CODE_ROOT / "src/python"), "HF_HUB_OFFLINE": "1",
                "HF_HUB_CACHE": str(CODE_ROOT / ".cache/huggingface"),
                "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"})
        self.ready = self._receive()
        if self.ready.get("type") != "ready":
            raise RuntimeError(f"native provider did not become ready: {self.ready}")
        self.registered_artifacts: set[str] = set()

    def _receive(self) -> dict[str, Any]:
        if not select.select([self.process.stdout], [], [], self.timeout)[0]:
            raise TimeoutError("native provider response timed out")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"native provider exited: {self.process.poll()}")
        response = json.loads(line)
        if response.get("type") == "error" or "error" in response:
            raise RuntimeError(response["error"])
        return response

    def request(self, request: dict[str, Any], *, artifacts: dict[str, bytes] | None = None) -> dict[str, Any]:
        """Return one native result without changing inputs or retrying failures."""
        for identity in request.get("artifact_ids", []):
            if identity not in self.registered_artifacts:
                content = artifacts[identity]
                actual = hashlib.sha256(content).hexdigest()
                if actual != identity:
                    raise ValueError(f"worker fitted artifact integrity mismatch: expected {identity}, "
                                     f"received {actual}, {len(content)} bytes")
                registration = {"command": "register_artifact", "sha256": identity,
                    "artifact": base64.b64encode(content).decode("ascii")}
                self.process.stdin.write(json.dumps(registration) + "\n")
                self.process.stdin.flush()
                response = self._receive()
                if response != {"type": "registered", "sha256": identity}:
                    raise RuntimeError(f"native artifact registration failed: {response}")
                self.registered_artifacts.add(identity)
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        return self._receive()

    def close(self) -> None:
        """Release the worker's model session after its owning execution exits."""
        if self.process.poll() is None:
            self.process.stdin.write('{"command":"shutdown"}\n')
            self.process.stdin.flush()
            self.process.stdin.close()
            self.process.wait(timeout=30)
        self.process.stdout.close()


_sessions = threading.local()


def native_request(kind: str, settings: dict[str, Any], request: dict[str, Any], *,
                   artifacts: dict[str, bytes] | None = None) -> dict[str, Any]:
    """Reuse the native session owned by this calculation worker and environment."""
    command = [str(CODE_ROOT / settings["environment"] / "bin/python")]
    if kind == "chronos":
        command.extend(["-m", "p04_forecast.chronos", "serve",
            "--model", settings["model"]["repository"], "--revision", settings["model"]["revision"],
            "--dtype", "float32", "--device", settings["device"], "--internal-cpu-threads", "1"])
    else:
        command.extend(["-m", "p04_forecast.native", kind, "--device", settings.get("device", "cpu")])
    if not hasattr(_sessions, "providers"):
        _sessions.providers = {}
    key = tuple(command)
    if key not in _sessions.providers:
        session = NativeSession(command, settings["timeout"])
        _sessions.providers[key] = session
        atexit.register(session.close)
    return _sessions.providers[key].request(request, artifacts=artifacts)
