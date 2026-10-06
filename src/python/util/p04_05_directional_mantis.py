# ==============================================================================
# p04_05_directional_mantis.py
#
# Purpose: Compose prepared directional inputs, Mantis records, and classifier jobs.
# Inputs: Model-neutral prepared arrays, portable representations, and classifier results.
# Outputs: Mantis requests, fourteen independent jobs, and directional predictions.
# Run from: Imported by the Process 04 coordinator; never schedules or writes storage.
# ==============================================================================

"""Typed composition boundary for the directional Mantis/Random-Forest model."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .shared_classification import (
    ClassificationDataset,
    ClassificationJob,
    ClassificationResponse,
    ClassificationResult,
    ClassifierSpec,
    content_fingerprint,
)


INPUT_LENGTH = 64
REPRESENTATION_DIMENSION = 256
HORIZONS = tuple(range(1, 15))
TRAINING_ROLE = "training"
OFFICIAL_ROLE = "official_evaluation"
SCIENTIFIC_PREDICTION_METADATA_FIELDS = {
    "classifier_run_id",
    "training_fingerprint",
    "evaluation_fingerprint",
    "output_fingerprint",
}


def _identity(value: Any, field: str) -> str:
    """Validate and return one nonempty stable identity."""
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a nonempty stable identity")
    return value


def _finite_vector(values: Any, length: int, field: str, dtype: str) -> np.ndarray:
    """Return one finite exact-length vector in the contract dtype."""
    try:
        vector = np.asarray(values, dtype=np.dtype(dtype))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if vector.shape != (length,) or not np.isfinite(vector).all():
        raise ValueError(f"{field} must contain exactly {length} finite {dtype} values")
    return vector


@dataclass(frozen=True)
class PreparedDirectionalInput:
    """Carry one model-neutral prepared training or official directional input."""

    input_id: str
    source_series_id: str
    role: str
    values: tuple[float, ...]
    label_reference: float
    labels: tuple[int, ...] | None
    preparation_definition_id: str
    preparation_fingerprint: str
    membership_fingerprint: str
    input_fingerprint: str

    def __post_init__(self) -> None:
        """Validate length, role, finite values, labels, and content identity."""
        _identity(self.input_id, "input_id")
        _identity(self.source_series_id, "source_series_id")
        _identity(self.preparation_definition_id, "preparation_definition_id")
        _identity(self.preparation_fingerprint, "preparation_fingerprint")
        _identity(self.membership_fingerprint, "membership_fingerprint")
        vector = _finite_vector(self.values, INPUT_LENGTH, "prepared values", "float64")
        if self.role not in {TRAINING_ROLE, OFFICIAL_ROLE}:
            raise ValueError("prepared input role must be training or official_evaluation")
        if not isinstance(self.label_reference, (int, float)) or not math.isfinite(
            float(self.label_reference)
        ):
            raise ValueError("label_reference must be finite")
        if self.role == TRAINING_ROLE:
            if (
                self.labels is None
                or len(self.labels) != len(HORIZONS)
                or any(
                    isinstance(value, bool) or value not in {0, 1}
                    for value in self.labels
                )
            ):
                raise ValueError("training inputs require fourteen binary labels")
        elif self.labels is not None:
            raise ValueError("official future labels cannot enter Process 04 inputs")
        expected = content_fingerprint(vector.tolist())
        if self.input_fingerprint != expected:
            raise ValueError("prepared input values do not match their fingerprint")

    @classmethod
    def create(
        cls,
        *,
        input_id: str,
        source_series_id: str,
        role: str,
        values: Sequence[float],
        label_reference: float,
        labels: Sequence[int] | None,
        preparation_definition_id: str,
        preparation_fingerprint: str,
        membership_fingerprint: str,
    ) -> "PreparedDirectionalInput":
        """Construct a prepared input while deriving its value fingerprint."""
        vector = _finite_vector(values, INPUT_LENGTH, "prepared values", "float64")
        return cls(
            input_id=input_id,
            source_series_id=source_series_id,
            role=role,
            values=tuple(float(value) for value in vector),
            label_reference=float(label_reference),
            labels=None if labels is None else tuple(labels),
            preparation_definition_id=preparation_definition_id,
            preparation_fingerprint=preparation_fingerprint,
            membership_fingerprint=membership_fingerprint,
            input_fingerprint=content_fingerprint(vector.tolist()),
        )


@dataclass(frozen=True)
class RepresentationRecord:
    """Carry one portable finite Mantis embedding and complete source lineage."""

    representation_id: str
    input_id: str
    source_series_id: str
    role: str
    definition_id: str
    values: tuple[float, ...]
    dtype: str
    dimension: int
    input_fingerprint: str
    preparation_definition_id: str
    preparation_fingerprint: str
    membership_fingerprint: str
    representation_fingerprint: str
    worker_provenance: Mapping[str, Any]

    def __post_init__(self) -> None:
        """Validate representation shape, dtype, identities, and fingerprint."""
        for value, field in (
            (self.representation_id, "representation_id"),
            (self.input_id, "input_id"),
            (self.source_series_id, "source_series_id"),
            (self.definition_id, "definition_id"),
            (self.input_fingerprint, "input_fingerprint"),
            (self.preparation_definition_id, "preparation_definition_id"),
            (self.preparation_fingerprint, "preparation_fingerprint"),
            (self.membership_fingerprint, "membership_fingerprint"),
        ):
            _identity(value, field)
        if self.role not in {TRAINING_ROLE, OFFICIAL_ROLE}:
            raise ValueError("representation role is invalid")
        if self.dtype != "float32" or self.dimension != REPRESENTATION_DIMENSION:
            raise ValueError("Mantis representations must be float32 with dimension 256")
        vector = _finite_vector(
            self.values, REPRESENTATION_DIMENSION, "representation", "float32"
        )
        if self.representation_fingerprint != content_fingerprint(vector.tolist()):
            raise ValueError("representation values do not match their fingerprint")
        if not isinstance(self.worker_provenance, Mapping):
            raise ValueError("worker_provenance must be an object")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RepresentationRecord":
        """Build and validate one portable Mantis worker record."""
        if not isinstance(value, Mapping):
            raise ValueError("representation record must be an object")
        try:
            return cls(
                representation_id=value["representation_id"],
                input_id=value["input_id"],
                source_series_id=value["source_series_id"],
                role=value["role"],
                definition_id=value["definition_id"],
                values=tuple(value["values"]),
                dtype=value["dtype"],
                dimension=value["dimension"],
                input_fingerprint=value["input_fingerprint"],
                preparation_definition_id=value["preparation_definition_id"],
                preparation_fingerprint=value["preparation_fingerprint"],
                membership_fingerprint=value["membership_fingerprint"],
                representation_fingerprint=value["representation_fingerprint"],
                worker_provenance=value["worker_provenance"],
            )
        except KeyError as exc:
            raise ValueError(f"representation record is missing {exc.args[0]}") from exc


@dataclass(frozen=True)
class DirectionalPrediction:
    """Expose one binary prediction with model-neutral and provider lineage."""

    prediction_id: str
    experiment_id: str
    model_definition_id: str
    evaluation_input_id: str
    horizon: int
    prediction: int
    scientific_metadata: Mapping[str, Any]
    content_hash: str

    def __post_init__(self) -> None:
        """Validate stable scientific metadata, identity, and content hash."""
        for value, field in (
            (self.prediction_id, "prediction_id"),
            (self.experiment_id, "experiment_id"),
            (self.model_definition_id, "model_definition_id"),
            (self.evaluation_input_id, "evaluation_input_id"),
        ):
            _identity(value, field)
        if self.horizon not in HORIZONS:
            raise ValueError("prediction horizon must be within 1..14")
        if isinstance(self.prediction, bool) or self.prediction not in {0, 1}:
            raise ValueError("directional prediction must be a binary integer")
        if (
            not isinstance(self.scientific_metadata, Mapping)
            or set(self.scientific_metadata) != SCIENTIFIC_PREDICTION_METADATA_FIELDS
        ):
            raise ValueError(
                "scientific_metadata must contain only stable classifier lineage"
            )
        for field in SCIENTIFIC_PREDICTION_METADATA_FIELDS:
            _identity(self.scientific_metadata[field], field)
        content = {
            "experiment_id": self.experiment_id,
            "model_definition_id": self.model_definition_id,
            "evaluation_input_id": self.evaluation_input_id,
            "horizon": self.horizon,
            "prediction": self.prediction,
            **dict(self.scientific_metadata),
        }
        logical_identity = {
            "model_definition_id": self.model_definition_id,
            "evaluation_input_id": self.evaluation_input_id,
            "horizon": self.horizon,
        }
        expected_hash = content_fingerprint(content)
        if self.prediction_id != (
            "directional-prediction/" + content_fingerprint(logical_identity)[:32]
        ):
            raise ValueError("directional prediction identity does not match its coordinates")
        if self.content_hash != expected_hash:
            raise ValueError("directional prediction does not match its content hash")


class DirectionalMantisComposer:
    """Compose portable Mantis representations and independent horizon jobs."""

    @staticmethod
    def representation_requests(
        inputs: Sequence[PreparedDirectionalInput], definition_id: str
    ) -> list[dict[str, Any]]:
        """Build label-free worker requests, calculating each selected source once."""
        _identity(definition_id, "definition_id")
        if not inputs:
            raise ValueError("at least one prepared input is required")
        identities = [item.input_id for item in inputs]
        if len(identities) != len(set(identities)):
            raise ValueError("prepared input identities must be unique")
        return [
            {
                "representation_id": "representation/"
                + content_fingerprint(
                    {"definition": definition_id, "input": item.input_id}
                )[:32],
                "input_id": item.input_id,
                "source_series_id": item.source_series_id,
                "role": item.role,
                "definition_id": definition_id,
                "values": list(item.values),
                "input_fingerprint": item.input_fingerprint,
                "preparation_definition_id": item.preparation_definition_id,
                "preparation_fingerprint": item.preparation_fingerprint,
                "membership_fingerprint": item.membership_fingerprint,
            }
            for item in inputs
        ]

    @staticmethod
    def classification_dataset(
        inputs: Sequence[PreparedDirectionalInput],
        representations: Sequence[RepresentationRecord],
        definition_id: str,
    ) -> ClassificationDataset:
        """Validate requested representation lineage and build one immutable dataset."""
        _identity(definition_id, "definition_id")
        prepared = {item.input_id: item for item in inputs}
        represented = {item.input_id: item for item in representations}
        if len(prepared) != len(inputs) or len(represented) != len(representations):
            raise ValueError("input and representation identities must be unique")
        representation_ids = [item.representation_id for item in representations]
        if len(representation_ids) != len(set(representation_ids)):
            raise ValueError("representation identities must be unique")
        if set(prepared) != set(represented):
            raise ValueError("representation set must exactly match prepared inputs")
        for input_id, record in represented.items():
            source = prepared[input_id]
            expected_representation_id = "representation/" + content_fingerprint(
                {"definition": definition_id, "input": source.input_id}
            )[:32]
            if (
                record.definition_id != definition_id
                or record.representation_id != expected_representation_id
                or record.source_series_id != source.source_series_id
                or record.role != source.role
                or record.input_fingerprint != source.input_fingerprint
                or record.preparation_definition_id != source.preparation_definition_id
                or record.preparation_fingerprint != source.preparation_fingerprint
                or record.membership_fingerprint != source.membership_fingerprint
            ):
                raise ValueError("representation differs from its explicit request lineage")
        training = sorted(
            (record for record in representations if record.role == TRAINING_ROLE),
            key=lambda item: item.representation_id,
        )
        official = sorted(
            (record for record in representations if record.role == OFFICIAL_ROLE),
            key=lambda item: item.representation_id,
        )
        if not training or not official:
            raise ValueError("composition requires training and official representations")
        return ClassificationDataset.create(
            training_identities=[item.representation_id for item in training],
            training_features=[item.values for item in training],
            evaluation_identities=[item.representation_id for item in official],
            evaluation_features=[item.values for item in official],
            feature_dimension=REPRESENTATION_DIMENSION,
            feature_dtype="float32",
        )

    @staticmethod
    def classification_jobs(
        inputs: Sequence[PreparedDirectionalInput],
        representations: Sequence[RepresentationRecord],
        dataset: ClassificationDataset,
        specification: ClassifierSpec,
    ) -> tuple[ClassificationJob, ...]:
        """Build fourteen lightweight label jobs referencing one immutable dataset."""
        prepared = {item.input_id: item for item in inputs}
        represented = {item.representation_id: item for item in representations}
        if (
            dataset.training_identities
            != tuple(sorted(
                (
                    item.representation_id
                    for item in representations
                    if item.role == TRAINING_ROLE
                )
            ))
            or dataset.evaluation_identities
            != tuple(sorted(
                (
                    item.representation_id
                    for item in representations
                    if item.role == OFFICIAL_ROLE
                )
            ))
        ):
            raise ValueError("classification dataset identities differ from representations")
        if (
            tuple(dataset.training_features)
            != tuple(represented[identity].values for identity in dataset.training_identities)
            or tuple(dataset.evaluation_features)
            != tuple(represented[identity].values for identity in dataset.evaluation_identities)
        ):
            raise ValueError("classification dataset values differ from representations")
        jobs = []
        for horizon in HORIZONS:
            labels = [
                prepared[represented[identity].input_id].labels[horizon - 1]
                for identity in dataset.training_identities
            ]
            jobs.append(
                ClassificationJob.create(
                    horizon=horizon,
                    specification=specification,
                    dataset=dataset,
                    training_labels=labels,
                )
            )
        return tuple(jobs)

    @staticmethod
    def directional_predictions(
        dataset: ClassificationDataset,
        jobs: Sequence[ClassificationJob],
        responses: Sequence[ClassificationResponse],
        representations: Sequence[RepresentationRecord],
        *,
        experiment_id: str,
        model_definition_id: str,
    ) -> tuple[DirectionalPrediction, ...]:
        """Reconcile one or more worker envelopes and produce common outputs."""
        _identity(experiment_id, "experiment_id")
        _identity(model_definition_id, "model_definition_id")
        response_values = tuple(responses)
        if not response_values or any(
            response.dataset_id != dataset.dataset_id for response in response_values
        ):
            raise ValueError("classifier responses must reference the submitted dataset")
        expected_jobs = {job.job_id: job for job in jobs}
        flattened = [result for response in response_values for result in response.results]
        returned_results = {result.job_id: result for result in flattened}
        if (
            not jobs
            or len(expected_jobs) != len(jobs)
            or len(returned_results) != len(flattened)
            or set(returned_results) != set(expected_jobs)
        ):
            raise ValueError("classifier results do not match the complete submitted job set")
        represented = {record.representation_id: record for record in representations}
        if not set(dataset.evaluation_identities).issubset(represented):
            raise ValueError("evaluation representations are missing from composition")
        for job_id, job in expected_jobs.items():
            result = returned_results[job_id]
            if (
                result.horizon != job.horizon
                or result.classifier_id != job.specification.classifier_id
                or dict(result.resolved_parameters) != dict(job.specification.parameters)
                or result.training_fingerprint != job.training_fingerprint
                or result.evaluation_fingerprint != job.evaluation_fingerprint
                or result.evaluation_identities != dataset.evaluation_identities
                or len(result.predictions) != len(dataset.evaluation_identities)
            ):
                raise ValueError("classifier result differs from its submitted job")
        predictions = []
        for result in sorted(flattened, key=lambda item: item.horizon):
            for representation_id, prediction in result.predictions:
                input_id = represented[representation_id].input_id
                metadata = {
                    "classifier_run_id": result.classifier_run_id,
                    "training_fingerprint": result.training_fingerprint,
                    "evaluation_fingerprint": result.evaluation_fingerprint,
                    "output_fingerprint": result.output_fingerprint,
                }
                content = {
                    "experiment_id": experiment_id,
                    "model_definition_id": model_definition_id,
                    "evaluation_input_id": input_id,
                    "horizon": result.horizon,
                    "prediction": prediction,
                    **metadata,
                }
                logical_identity = {
                    "model_definition_id": model_definition_id,
                    "evaluation_input_id": input_id,
                    "horizon": result.horizon,
                }
                predictions.append(
                    DirectionalPrediction(
                        prediction_id="directional-prediction/"
                        + content_fingerprint(logical_identity)[:32],
                        experiment_id=experiment_id,
                        model_definition_id=model_definition_id,
                        evaluation_input_id=input_id,
                        horizon=result.horizon,
                        prediction=prediction,
                        scientific_metadata=metadata,
                        content_hash=content_fingerprint(content),
                    )
                )
        return tuple(predictions)
