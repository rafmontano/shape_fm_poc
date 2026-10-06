# ==============================================================================
# shared_classification.py
#
# Purpose: Define portable classification contracts and a model-neutral Random Forest adapter.
# Inputs: One immutable feature dataset, horizon labels, identities, and classifier settings.
# Outputs: Typed datasets/jobs/responses and deterministic binary predictions.
# Run from: Imported by the isolated classifier worker and directional composers.
# ==============================================================================

"""Reusable classification contracts with no Mantis or orchestration dependency."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


EXPECTED_SCIKIT_LEARN_VERSION = "1.7.2"
RANDOM_FOREST_TREES = 200
RANDOM_FOREST_SEED = 42
RANDOM_FOREST_THREADS = 1


def canonical_json(value: Any) -> str:
    """Return stable JSON for portable component identities and fingerprints."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def content_fingerprint(value: Any) -> str:
    """Return the SHA-256 digest of canonical JSON-compatible content."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _identity(value: Any, field: str) -> str:
    """Validate and return one nonempty stable identity."""
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a nonempty stable identity")
    return value


def _feature_matrix(
    values: Any, field: str, feature_dimension: int, dtype: str
) -> np.ndarray:
    """Convert one finite two-dimensional feature matrix to the specified dtype."""
    try:
        matrix = np.asarray(values, dtype=np.dtype(dtype))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if (
        matrix.ndim != 2
        or matrix.shape[0] < 1
        or matrix.shape[1] != feature_dimension
        or not np.isfinite(matrix).all()
    ):
        raise ValueError(
            f"{field} must be a nonempty finite matrix with {feature_dimension} columns"
        )
    return matrix


def _binary_labels(values: Any, expected_count: int) -> np.ndarray:
    """Validate one exact-length vector of integer binary labels."""
    labels = np.asarray(values)
    if (
        expected_count < 1
        or labels.ndim != 1
        or labels.shape[0] != expected_count
        or not np.isin(labels, (0, 1)).all()
        or any(isinstance(value, bool) for value in labels.tolist())
    ):
        raise ValueError("training labels must be an exact-length binary integer vector")
    return labels.astype(np.int8)


@dataclass(frozen=True)
class ClassifierSpec:
    """Describe one fully resolved, versioned classifier calculation."""

    classifier_id: str
    implementation: str
    implementation_version: str
    parameters: Mapping[str, Any]
    seed: int
    feature_dimension: int
    feature_dtype: str
    output_contract: str = "binary_integer_v1"

    def __post_init__(self) -> None:
        """Reject incomplete or scientifically incompatible classifier definitions."""
        _identity(self.classifier_id, "classifier_id")
        if self.implementation != "sklearn.ensemble.RandomForestClassifier":
            raise ValueError("unsupported classifier implementation")
        if self.implementation_version != EXPECTED_SCIKIT_LEARN_VERSION:
            raise ValueError("classifier implementation version must be scikit-learn 1.7.2")
        if not isinstance(self.parameters, Mapping) or not self.parameters:
            raise ValueError("classifier parameters must contain the complete resolved mapping")
        if (
            self.parameters.get("n_estimators") != RANDOM_FOREST_TREES
            or self.parameters.get("random_state") != RANDOM_FOREST_SEED
            or self.parameters.get("n_jobs") != RANDOM_FOREST_THREADS
            or self.seed != RANDOM_FOREST_SEED
        ):
            raise ValueError("Random Forest must use 200 trees, seed 42, and one thread")
        if (
            isinstance(self.feature_dimension, bool)
            or not isinstance(self.feature_dimension, int)
            or self.feature_dimension < 1
        ):
            raise ValueError("feature_dimension must be a positive integer")
        if self.feature_dtype not in {"float32", "float64"}:
            raise ValueError("feature_dtype must be float32 or float64")
        if self.output_contract != "binary_integer_v1":
            raise ValueError("unsupported classifier output contract")

    def to_dict(self) -> dict[str, Any]:
        """Return the complete JSON-compatible classifier definition."""
        return {
            "classifier_id": self.classifier_id,
            "implementation": self.implementation,
            "implementation_version": self.implementation_version,
            "parameters": dict(self.parameters),
            "seed": self.seed,
            "feature_dimension": self.feature_dimension,
            "feature_dtype": self.feature_dtype,
            "output_contract": self.output_contract,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ClassifierSpec":
        """Build and validate a specification from a worker-safe mapping."""
        if not isinstance(value, Mapping):
            raise ValueError("classifier specification must be an object")
        try:
            return cls(
                classifier_id=value["classifier_id"],
                implementation=value["implementation"],
                implementation_version=value["implementation_version"],
                parameters=value["parameters"],
                seed=value["seed"],
                feature_dimension=value["feature_dimension"],
                feature_dtype=value["feature_dtype"],
                output_contract=value.get("output_contract", "binary_integer_v1"),
            )
        except KeyError as exc:
            raise ValueError(f"classifier specification is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class ClassificationDataset:
    """Own one immutable model-neutral training/evaluation feature dataset."""

    dataset_id: str
    training_identities: tuple[str, ...]
    training_features: tuple[tuple[float, ...], ...]
    evaluation_identities: tuple[str, ...]
    evaluation_features: tuple[tuple[float, ...], ...]
    feature_dimension: int
    feature_dtype: str
    training_fingerprint: str
    evaluation_fingerprint: str
    dataset_fingerprint: str

    def __post_init__(self) -> None:
        """Validate immutable identities, matrices, and independently supplied hashes."""
        _identity(self.dataset_id, "dataset_id")
        if (
            isinstance(self.feature_dimension, bool)
            or not isinstance(self.feature_dimension, int)
            or self.feature_dimension < 1
        ):
            raise ValueError("feature_dimension must be a positive integer")
        if self.feature_dtype not in {"float32", "float64"}:
            raise ValueError("feature_dtype must be float32 or float64")
        training_ids = tuple(
            _identity(value, "training identity") for value in self.training_identities
        )
        evaluation_ids = tuple(
            _identity(value, "evaluation identity") for value in self.evaluation_identities
        )
        if (
            not training_ids
            or not evaluation_ids
            or len(training_ids) != len(set(training_ids))
            or len(evaluation_ids) != len(set(evaluation_ids))
        ):
            raise ValueError("classification identities must be nonempty and unique")
        training = _feature_matrix(
            self.training_features,
            "training_features",
            self.feature_dimension,
            self.feature_dtype,
        )
        evaluation = _feature_matrix(
            self.evaluation_features,
            "evaluation_features",
            self.feature_dimension,
            self.feature_dtype,
        )
        if training.shape[0] != len(training_ids) or evaluation.shape[0] != len(evaluation_ids):
            raise ValueError("feature row counts must equal their identity counts")
        expected_training = content_fingerprint(
            {"identities": list(training_ids), "features": training.tolist()}
        )
        expected_evaluation = content_fingerprint(
            {"identities": list(evaluation_ids), "features": evaluation.tolist()}
        )
        identity_content = {
            "training_fingerprint": expected_training,
            "evaluation_fingerprint": expected_evaluation,
            "feature_dimension": self.feature_dimension,
            "feature_dtype": self.feature_dtype,
        }
        expected_dataset = content_fingerprint(identity_content)
        if self.training_fingerprint != expected_training:
            raise ValueError("training features do not match their fingerprint")
        if self.evaluation_fingerprint != expected_evaluation:
            raise ValueError("evaluation features do not match their fingerprint")
        if self.dataset_fingerprint != expected_dataset:
            raise ValueError("classification dataset does not match its fingerprint")
        if self.dataset_id != "classification-dataset/" + expected_dataset[:32]:
            raise ValueError("classification dataset identity does not match its content")

    @classmethod
    def create(
        cls,
        *,
        training_identities: Sequence[str],
        training_features: Sequence[Sequence[float]],
        evaluation_identities: Sequence[str],
        evaluation_features: Sequence[Sequence[float]],
        feature_dimension: int,
        feature_dtype: str,
    ) -> "ClassificationDataset":
        """Construct one immutable dataset and derive every content identity."""
        training = _feature_matrix(
            training_features, "training_features", feature_dimension, feature_dtype
        )
        evaluation = _feature_matrix(
            evaluation_features, "evaluation_features", feature_dimension, feature_dtype
        )
        training_ids = tuple(training_identities)
        evaluation_ids = tuple(evaluation_identities)
        training_fingerprint = content_fingerprint(
            {"identities": list(training_ids), "features": training.tolist()}
        )
        evaluation_fingerprint = content_fingerprint(
            {"identities": list(evaluation_ids), "features": evaluation.tolist()}
        )
        dataset_fingerprint = content_fingerprint(
            {
                "training_fingerprint": training_fingerprint,
                "evaluation_fingerprint": evaluation_fingerprint,
                "feature_dimension": feature_dimension,
                "feature_dtype": feature_dtype,
            }
        )
        return cls(
            dataset_id="classification-dataset/" + dataset_fingerprint[:32],
            training_identities=training_ids,
            training_features=tuple(tuple(float(item) for item in row) for row in training),
            evaluation_identities=evaluation_ids,
            evaluation_features=tuple(tuple(float(item) for item in row) for row in evaluation),
            feature_dimension=feature_dimension,
            feature_dtype=feature_dtype,
            training_fingerprint=training_fingerprint,
            evaluation_fingerprint=evaluation_fingerprint,
            dataset_fingerprint=dataset_fingerprint,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the dataset once for one portable classifier-worker request."""
        return {
            "dataset_id": self.dataset_id,
            "training_identities": list(self.training_identities),
            "training_features": [list(row) for row in self.training_features],
            "evaluation_identities": list(self.evaluation_identities),
            "evaluation_features": [list(row) for row in self.evaluation_features],
            "feature_dimension": self.feature_dimension,
            "feature_dtype": self.feature_dtype,
            "training_fingerprint": self.training_fingerprint,
            "evaluation_fingerprint": self.evaluation_fingerprint,
            "dataset_fingerprint": self.dataset_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ClassificationDataset":
        """Build and validate one dataset received at a process boundary."""
        if not isinstance(value, Mapping):
            raise ValueError("classification dataset must be an object")
        try:
            return cls(
                dataset_id=value["dataset_id"],
                training_identities=tuple(value["training_identities"]),
                training_features=tuple(tuple(row) for row in value["training_features"]),
                evaluation_identities=tuple(value["evaluation_identities"]),
                evaluation_features=tuple(tuple(row) for row in value["evaluation_features"]),
                feature_dimension=value["feature_dimension"],
                feature_dtype=value["feature_dtype"],
                training_fingerprint=value["training_fingerprint"],
                evaluation_fingerprint=value["evaluation_fingerprint"],
                dataset_fingerprint=value["dataset_fingerprint"],
            )
        except KeyError as exc:
            raise ValueError(f"classification dataset is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class ClassificationJob:
    """Reference one immutable dataset and carry one horizon's labels/specification."""

    job_id: str
    horizon: int
    specification: ClassifierSpec
    dataset_id: str
    training_labels: tuple[int, ...]
    training_dataset_fingerprint: str
    labels_fingerprint: str
    training_fingerprint: str
    evaluation_fingerprint: str

    def __post_init__(self) -> None:
        """Validate the lightweight horizon contract and its fingerprints."""
        _identity(self.job_id, "job_id")
        _identity(self.dataset_id, "dataset_id")
        for value, field in (
            (self.training_dataset_fingerprint, "training_dataset_fingerprint"),
            (self.labels_fingerprint, "labels_fingerprint"),
            (self.training_fingerprint, "training_fingerprint"),
            (self.evaluation_fingerprint, "evaluation_fingerprint"),
        ):
            _identity(value, field)
        if isinstance(self.horizon, bool) or self.horizon not in range(1, 15):
            raise ValueError("classification horizon must be within 1..14")
        labels = _binary_labels(self.training_labels, len(self.training_labels))
        expected_labels = content_fingerprint(labels.tolist())
        expected_training = content_fingerprint(
            {
                "dataset": self.training_dataset_fingerprint,
                "labels": expected_labels,
            }
        )
        if self.labels_fingerprint != expected_labels:
            raise ValueError("training labels do not match their fingerprint")
        if self.training_fingerprint != expected_training:
            raise ValueError("training job does not match its fingerprint")
        identity_content = {
            "classifier": self.specification.classifier_id,
            "dataset": self.dataset_id,
            "horizon": self.horizon,
            "training_fingerprint": expected_training,
            "evaluation_fingerprint": self.evaluation_fingerprint,
        }
        if self.job_id != "classification-job/" + content_fingerprint(identity_content)[:32]:
            raise ValueError("classification job identity does not match its content")

    @classmethod
    def create(
        cls,
        *,
        horizon: int,
        specification: ClassifierSpec,
        dataset: ClassificationDataset,
        training_labels: Sequence[int],
    ) -> "ClassificationJob":
        """Construct one lightweight job referring to an immutable dataset."""
        if (
            specification.feature_dimension != dataset.feature_dimension
            or specification.feature_dtype != dataset.feature_dtype
        ):
            raise ValueError("classifier specification differs from the dataset contract")
        labels = _binary_labels(training_labels, len(dataset.training_identities))
        labels_fingerprint = content_fingerprint(labels.tolist())
        training_fingerprint = content_fingerprint(
            {"dataset": dataset.training_fingerprint, "labels": labels_fingerprint}
        )
        identity_content = {
            "classifier": specification.classifier_id,
            "dataset": dataset.dataset_id,
            "horizon": horizon,
            "training_fingerprint": training_fingerprint,
            "evaluation_fingerprint": dataset.evaluation_fingerprint,
        }
        return cls(
            job_id="classification-job/" + content_fingerprint(identity_content)[:32],
            horizon=horizon,
            specification=specification,
            dataset_id=dataset.dataset_id,
            training_labels=tuple(int(item) for item in labels),
            training_dataset_fingerprint=dataset.training_fingerprint,
            labels_fingerprint=labels_fingerprint,
            training_fingerprint=training_fingerprint,
            evaluation_fingerprint=dataset.evaluation_fingerprint,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a portable job without repeating either feature matrix."""
        return {
            "job_id": self.job_id,
            "horizon": self.horizon,
            "specification": self.specification.to_dict(),
            "dataset_id": self.dataset_id,
            "training_labels": list(self.training_labels),
            "training_dataset_fingerprint": self.training_dataset_fingerprint,
            "labels_fingerprint": self.labels_fingerprint,
            "training_fingerprint": self.training_fingerprint,
            "evaluation_fingerprint": self.evaluation_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ClassificationJob":
        """Build and validate one lightweight classification job."""
        if not isinstance(value, Mapping):
            raise ValueError("classification job must be an object")
        try:
            return cls(
                job_id=value["job_id"],
                horizon=value["horizon"],
                specification=ClassifierSpec.from_dict(value["specification"]),
                dataset_id=value["dataset_id"],
                training_labels=tuple(value["training_labels"]),
                training_dataset_fingerprint=value["training_dataset_fingerprint"],
                labels_fingerprint=value["labels_fingerprint"],
                training_fingerprint=value["training_fingerprint"],
                evaluation_fingerprint=value["evaluation_fingerprint"],
            )
        except KeyError as exc:
            raise ValueError(f"classification job is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class ClassificationResult:
    """Return one horizon's predictions and exact submitted-job lineage."""

    classifier_run_id: str
    job_id: str
    horizon: int
    classifier_id: str
    evaluation_identities: tuple[str, ...]
    predictions: tuple[tuple[str, int], ...]
    resolved_parameters: Mapping[str, Any]
    training_fingerprint: str
    evaluation_fingerprint: str
    output_fingerprint: str

    def __post_init__(self) -> None:
        """Reject malformed prediction identities, values, or output fingerprints."""
        _identity(self.classifier_run_id, "classifier_run_id")
        _identity(self.job_id, "job_id")
        _identity(self.classifier_id, "classifier_id")
        _identity(self.training_fingerprint, "training_fingerprint")
        _identity(self.evaluation_fingerprint, "evaluation_fingerprint")
        if not isinstance(self.resolved_parameters, Mapping) or not self.resolved_parameters:
            raise ValueError("classification result requires resolved parameters")
        if (
            isinstance(self.horizon, bool)
            or self.horizon not in range(1, 15)
            or not self.predictions
        ):
            raise ValueError("classification result requires one horizon and predictions")
        expected_identities = tuple(
            _identity(value, "evaluation identity") for value in self.evaluation_identities
        )
        if not expected_identities or len(expected_identities) != len(set(expected_identities)):
            raise ValueError("evaluation identities must be nonempty and unique")
        identities = []
        normalized = []
        for identity, prediction in self.predictions:
            identities.append(_identity(identity, "prediction identity"))
            if isinstance(prediction, bool) or prediction not in {0, 1}:
                raise ValueError("classification predictions must be binary integers")
            normalized.append({"identity": identity, "prediction": int(prediction)})
        if len(identities) != len(set(identities)):
            raise ValueError("classification prediction identities must be unique")
        if tuple(identities) != expected_identities:
            raise ValueError("classification predictions differ from evaluation identities")
        if self.output_fingerprint != content_fingerprint(normalized):
            raise ValueError("classification output does not match its fingerprint")
        expected_run = content_fingerprint(
            {
                "classifier": self.classifier_id,
                "horizon": self.horizon,
                "training": self.training_fingerprint,
                "evaluation": self.evaluation_fingerprint,
            }
        )
        if self.classifier_run_id != "classifier-run/" + expected_run[:32]:
            raise ValueError("classifier run identity does not match its submitted lineage")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible classifier result."""
        return {
            "classifier_run_id": self.classifier_run_id,
            "job_id": self.job_id,
            "horizon": self.horizon,
            "classifier_id": self.classifier_id,
            "evaluation_identities": list(self.evaluation_identities),
            "predictions": [
                {"identity": identity, "prediction": prediction}
                for identity, prediction in self.predictions
            ],
            "resolved_parameters": dict(self.resolved_parameters),
            "training_fingerprint": self.training_fingerprint,
            "evaluation_fingerprint": self.evaluation_fingerprint,
            "output_fingerprint": self.output_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ClassificationResult":
        """Build and validate a portable worker result."""
        if not isinstance(value, Mapping):
            raise ValueError("classification result must be an object")
        try:
            return cls(
                classifier_run_id=value["classifier_run_id"],
                job_id=value["job_id"],
                horizon=value["horizon"],
                classifier_id=value["classifier_id"],
                evaluation_identities=tuple(value["evaluation_identities"]),
                predictions=tuple(
                    (item["identity"], item["prediction"])
                    for item in value["predictions"]
                ),
                resolved_parameters=value["resolved_parameters"],
                training_fingerprint=value["training_fingerprint"],
                evaluation_fingerprint=value["evaluation_fingerprint"],
                output_fingerprint=value["output_fingerprint"],
            )
        except KeyError as exc:
            raise ValueError(f"classification result is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class ClassificationResponse:
    """Carry results in one validated operational worker-provenance envelope."""

    response_id: str
    dataset_id: str
    results: tuple[ClassificationResult, ...]
    worker_provenance: Mapping[str, Any]
    worker_provenance_fingerprint: str

    def __post_init__(self) -> None:
        """Validate shared provenance and unique result identities."""
        _identity(self.response_id, "response_id")
        _identity(self.dataset_id, "dataset_id")
        if not self.results:
            raise ValueError("classification response requires results")
        job_ids = [result.job_id for result in self.results]
        if len(job_ids) != len(set(job_ids)):
            raise ValueError("classification response job identities must be unique")
        if not isinstance(self.worker_provenance, Mapping) or not self.worker_provenance:
            raise ValueError("classification response requires worker provenance")
        expected_provenance = content_fingerprint(dict(self.worker_provenance))
        if self.worker_provenance_fingerprint != expected_provenance:
            raise ValueError("worker provenance does not match its fingerprint")
        expected_response = content_fingerprint(
            {
                "dataset_id": self.dataset_id,
                "results": [
                    content_fingerprint(result.to_dict()) for result in self.results
                ],
                "worker_provenance_fingerprint": expected_provenance,
            }
        )
        if self.response_id != "classification-response/" + expected_response[:32]:
            raise ValueError("classification response identity does not match its content")

    @classmethod
    def create(
        cls,
        *,
        dataset_id: str,
        results: Sequence[ClassificationResult],
        worker_provenance: Mapping[str, Any],
    ) -> "ClassificationResponse":
        """Construct an operational envelope without copying provenance per result."""
        result_values = tuple(results)
        provenance = dict(worker_provenance)
        provenance_fingerprint = content_fingerprint(provenance)
        response_fingerprint = content_fingerprint(
            {
                "dataset_id": dataset_id,
                "results": [
                    content_fingerprint(result.to_dict()) for result in result_values
                ],
                "worker_provenance_fingerprint": provenance_fingerprint,
            }
        )
        return cls(
            response_id="classification-response/" + response_fingerprint[:32],
            dataset_id=dataset_id,
            results=result_values,
            worker_provenance=provenance,
            worker_provenance_fingerprint=provenance_fingerprint,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return one JSON-compatible response envelope."""
        return {
            "response_id": self.response_id,
            "dataset_id": self.dataset_id,
            "results": [result.to_dict() for result in self.results],
            "worker_provenance": dict(self.worker_provenance),
            "worker_provenance_fingerprint": self.worker_provenance_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ClassificationResponse":
        """Build and validate one classifier worker response envelope."""
        if not isinstance(value, Mapping):
            raise ValueError("classification response must be an object")
        try:
            return cls(
                response_id=value["response_id"],
                dataset_id=value["dataset_id"],
                results=tuple(
                    ClassificationResult.from_dict(result) for result in value["results"]
                ),
                worker_provenance=value["worker_provenance"],
                worker_provenance_fingerprint=value["worker_provenance_fingerprint"],
            )
        except KeyError as exc:
            raise ValueError(f"classification response is missing {exc.args[0]}") from exc


class RandomForestClassifierProvider:
    """Fit and use one model-neutral, single-threaded binary Random Forest."""

    def __init__(self, feature_dimension: int = 256, feature_dtype: str = "float32"):
        """Validate the locked runtime and initialize an unfitted provider."""
        version = importlib.metadata.version("scikit-learn")
        if version != EXPECTED_SCIKIT_LEARN_VERSION:
            raise RuntimeError(
                f"Random Forest requires scikit-learn {EXPECTED_SCIKIT_LEARN_VERSION}; found {version}"
            )
        if (
            isinstance(feature_dimension, bool)
            or not isinstance(feature_dimension, int)
            or feature_dimension < 1
        ):
            raise ValueError("feature_dimension must be a positive integer")
        if feature_dtype not in {"float32", "float64"}:
            raise ValueError("feature_dtype must be float32 or float64")
        self.feature_dimension = feature_dimension
        self.feature_dtype = feature_dtype
        self._model: Any | None = None
        self._fitted_contract: dict[str, Any] | None = None

    @staticmethod
    def _new_model() -> Any:
        """Create the exact approved Random Forest without nested parallelism."""
        from sklearn.ensemble import RandomForestClassifier

        return RandomForestClassifier(
            n_estimators=RANDOM_FOREST_TREES,
            random_state=RANDOM_FOREST_SEED,
            n_jobs=RANDOM_FOREST_THREADS,
        )

    def describe(self) -> ClassifierSpec:
        """Return the complete parameters resolved by the locked scikit-learn runtime."""
        parameters = self._new_model().get_params(deep=False)
        identity_content = {
            "implementation": "sklearn.ensemble.RandomForestClassifier",
            "implementation_version": EXPECTED_SCIKIT_LEARN_VERSION,
            "parameters": parameters,
            "feature_dimension": self.feature_dimension,
            "feature_dtype": self.feature_dtype,
            "output_contract": "binary_integer_v1",
        }
        return ClassifierSpec(
            classifier_id="classifier/random-forest/" + content_fingerprint(identity_content)[:32],
            implementation=identity_content["implementation"],
            implementation_version=EXPECTED_SCIKIT_LEARN_VERSION,
            parameters=parameters,
            seed=RANDOM_FOREST_SEED,
            feature_dimension=self.feature_dimension,
            feature_dtype=self.feature_dtype,
        )

    def fit(self, features: Any, labels: Any) -> "RandomForestClassifierProvider":
        """Fit one native classifier to a finite ordinary feature matrix."""
        matrix = _feature_matrix(
            features, "training_features", self.feature_dimension, self.feature_dtype
        )
        target = _binary_labels(labels, matrix.shape[0])
        self._model = self._new_model()
        self._model.fit(matrix, target)
        return self

    def predict(self, features: Any) -> np.ndarray:
        """Return exact binary integer predictions from the fitted native model."""
        if self._model is None:
            raise RuntimeError("Random Forest provider must be fitted before prediction")
        matrix = _feature_matrix(
            features, "evaluation_features", self.feature_dimension, self.feature_dtype
        )
        predictions = np.asarray(self._model.predict(matrix))
        if predictions.ndim != 1 or not np.isin(predictions, (0, 1)).all():
            raise RuntimeError("Random Forest returned invalid binary predictions")
        return predictions.astype(np.int8)

    @staticmethod
    def classifier_run_id(job: ClassificationJob) -> str:
        """Derive the stable scientific run identity for one submitted horizon."""
        run_identity = {
            "classifier": job.specification.classifier_id,
            "horizon": job.horizon,
            "training": job.training_fingerprint,
            "evaluation": job.evaluation_fingerprint,
        }
        return "classifier-run/" + content_fingerprint(run_identity)[:32]

    def fit_job(
        self, dataset: ClassificationDataset, job: ClassificationJob
    ) -> "RandomForestClassifierProvider":
        """Validate and fit one training job without calculating predictions."""
        if job.specification != self.describe():
            raise ValueError("classification job specification differs from the locked provider")
        if (
            job.dataset_id != dataset.dataset_id
            or job.training_dataset_fingerprint != dataset.training_fingerprint
            or job.evaluation_fingerprint != dataset.evaluation_fingerprint
        ):
            raise ValueError("classification job differs from its referenced dataset")
        self.fit(dataset.training_features, job.training_labels)
        self._fitted_contract = {
            "classifier_id": job.specification.classifier_id,
            "horizon": job.horizon,
            "training_fingerprint": job.training_fingerprint,
            "training_dataset_fingerprint": job.training_dataset_fingerprint,
        }
        return self

    def predict_job(
        self, dataset: ClassificationDataset, job: ClassificationJob
    ) -> ClassificationResult:
        """Predict one validated job from existing fitted state without fitting."""
        if job.specification != self.describe():
            raise ValueError("classification job specification differs from the locked provider")
        if (
            job.dataset_id != dataset.dataset_id
            or job.training_dataset_fingerprint != dataset.training_fingerprint
            or job.evaluation_fingerprint != dataset.evaluation_fingerprint
        ):
            raise ValueError("classification job differs from its referenced dataset")
        expected_fitted_contract = {
            "classifier_id": job.specification.classifier_id,
            "horizon": job.horizon,
            "training_fingerprint": job.training_fingerprint,
            "training_dataset_fingerprint": job.training_dataset_fingerprint,
        }
        if self._fitted_contract != expected_fitted_contract:
            raise ValueError("fitted Random Forest artifact differs from the submitted job")
        predicted = self.predict(dataset.evaluation_features)
        prediction_pairs = tuple(
            (identity, int(prediction))
            for identity, prediction in zip(
                dataset.evaluation_identities, predicted.tolist(), strict=True
            )
        )
        output_content = [
            {"identity": identity, "prediction": prediction}
            for identity, prediction in prediction_pairs
        ]
        return ClassificationResult(
            classifier_run_id=self.classifier_run_id(job),
            job_id=job.job_id,
            horizon=job.horizon,
            classifier_id=job.specification.classifier_id,
            evaluation_identities=dataset.evaluation_identities,
            predictions=prediction_pairs,
            resolved_parameters=dict(job.specification.parameters),
            training_fingerprint=job.training_fingerprint,
            evaluation_fingerprint=job.evaluation_fingerprint,
            output_fingerprint=content_fingerprint(output_content),
        )

    def run(
        self, dataset: ClassificationDataset, job: ClassificationJob
    ) -> ClassificationResult:
        """Fit and predict one referenced dataset while retaining the native model."""
        return self.fit_job(dataset, job).predict_job(dataset, job)
