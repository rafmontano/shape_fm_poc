# ==============================================================================
# shared_model_storage.py
#
# Purpose: Own trusted fitted-meta-learner paths and atomic joblib persistence.
# Inputs: A configured model root, experiment namespace, and logical model scope.
# Outputs: Verified external joblib files and clear load/save lifecycle evidence.
# Run from: Imported by provider workers and metadata-only coordinator operations.
# ==============================================================================

"""Small filesystem authority for trusted project-owned fitted model objects."""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import joblib


class ModelStorageError(RuntimeError):
    """Identify missing, unreadable, or invalid fitted-model storage operations."""


class ModelStorage:
    """Resolve, atomically save, and load one experiment's fitted meta-learners."""

    def __init__(self, root: Path | str, experiment: str):
        """Retain one configured root and validated experiment namespace."""
        self.root = Path(root).expanduser().resolve()
        self.experiment = self._segment(experiment, "experiment")

    @staticmethod
    def _segment(value: Any, field: str) -> str:
        """Reject empty or path-like logical identity segments."""
        if (
            not isinstance(value, str)
            or not value
            or value in {".", ".."}
            or Path(value).name != value
        ):
            raise ValueError(f"{field} must be one safe path segment")
        return value

    @staticmethod
    def _scope(horizon_scope: int | str) -> str:
        """Map the approved multi-horizon or numbered-horizon scope to a filename."""
        if horizon_scope == "all_horizons":
            return "model_all_horizons.joblib"
        if (
            isinstance(horizon_scope, bool)
            or not isinstance(horizon_scope, int)
            or horizon_scope not in range(1, 15)
        ):
            raise ValueError("horizon_scope must be all_horizons or an integer within 1..14")
        return f"model_h{horizon_scope:02d}.joblib"

    def path(self, model: str, frequency: str, horizon_scope: int | str) -> Path:
        """Return the centrally resolved path for one approved logical model scope."""
        model_name = self._segment(model, "model")
        if model_name not in {"directional_dtw", "directional_mantis_rf"}:
            raise ValueError(f"unsupported fitted model: {model_name}")
        frequency_name = self._segment(frequency, "frequency")
        return (
            self.root
            / self.experiment
            / model_name
            / frequency_name
            / self._scope(horizon_scope)
        )

    def exists(self, model: str, frequency: str, horizon_scope: int | str) -> bool:
        """Report actual fitted-file availability without consulting task state."""
        return self.path(model, frequency, horizon_scope).is_file()

    def _evidence(self, path: Path, status: str) -> dict[str, Any]:
        """Describe one verified artifact without creating a separate catalogue."""
        return {
            "status": status,
            "relative_path": str(path.relative_to(self.root)),
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    def inspect(self, model: str, frequency: str, horizon_scope: int | str) -> dict[str, Any]:
        """Read artifact metadata only; provider workers alone deserialize objects."""
        path = self.path(model, frequency, horizon_scope)
        if not path.is_file():
            raise ModelStorageError(
                f"missing fitted model {model}/{frequency}/{horizon_scope}: {path}"
            )
        try:
            evidence = self._evidence(path, "inspected")
            if evidence["size_bytes"] == 0:
                raise ValueError("empty fitted artifact")
            return evidence
        except Exception as exc:
            raise ModelStorageError(
                f"invalid or unreadable fitted model {model}/{frequency}/{horizon_scope}: {path}: {exc}"
            ) from exc

    def save(
        self,
        model_object: Any,
        model: str,
        frequency: str,
        horizon_scope: int | str,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Validate a temporary joblib file, then atomically publish it."""
        path = self.path(model, frequency, horizon_scope)
        if path.is_file() and not overwrite:
            return self._evidence(path, "skipped_existing")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        status = "overwritten" if path.is_file() else "trained"
        try:
            joblib.dump(model_object, temporary)
            joblib.load(temporary)
            os.replace(temporary, path)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            raise ModelStorageError(
                f"failed to save fitted model {model}/{frequency}/{horizon_scope}: {exc}"
            ) from exc
        return self._evidence(path, status)

    def load(self, model: str, frequency: str, horizon_scope: int | str) -> Any:
        """Load one required trusted model or fail with its exact logical scope."""
        path = self.path(model, frequency, horizon_scope)
        if not path.is_file():
            raise ModelStorageError(
                f"missing fitted model {model}/{frequency}/{horizon_scope}: {path}"
            )
        try:
            return joblib.load(path)
        except BaseException as exc:
            raise ModelStorageError(
                f"invalid or unreadable fitted model {model}/{frequency}/{horizon_scope}: {path}: {exc}"
            ) from exc

    def adopt(
        self,
        source: Path | str,
        model: str,
        frequency: str,
        horizon_scope: int | str,
        *,
        size_bytes: int,
        sha256: str,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Verify worker-attested bytes and atomically adopt without deserialization."""
        source_path = Path(source)
        path = self.path(model, frequency, horizon_scope)
        if path.is_file() and not overwrite:
            return self._evidence(path, "skipped_existing")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        status = "overwritten" if path.is_file() else "trained"
        try:
            if (
                source_path.stat().st_size != size_bytes
                or hashlib.sha256(source_path.read_bytes()).hexdigest() != sha256
            ):
                raise ValueError("incoming artifact differs from worker size or SHA-256")
            shutil.copyfile(source_path, temporary)
            if (
                temporary.stat().st_size != size_bytes
                or hashlib.sha256(temporary.read_bytes()).hexdigest() != sha256
            ):
                raise ValueError("copied artifact differs from worker size or SHA-256")
            os.replace(temporary, path)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            raise ModelStorageError(
                f"failed to adopt fitted model {model}/{frequency}/{horizon_scope}: {exc}"
            ) from exc
        return self._evidence(path, status)
