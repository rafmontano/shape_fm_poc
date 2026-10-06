# ==============================================================================
# test_directional_mantis_components.py
#
# Purpose: Verify the isolated Mantis, classification, and composition contracts.
# Inputs: Locked local worker environments, fixed arrays, labels, and identities.
# Outputs: Bounded unittest evidence; no durable research or model artifacts.
# Run from: Imported by unittest discovery; not run directly in production.
# ==============================================================================

"""Focused native-worker and composition tests for IDs 027 and 028."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any

import numpy as np

from util.p04_05_directional_mantis import (
    DirectionalMantisComposer,
    PreparedDirectionalInput,
    RepresentationRecord,
)
from util.shared_classification import (
    ClassificationDataset,
    ClassificationJob,
    ClassificationResponse,
    ClassificationResult,
    ClassifierSpec,
    RandomForestClassifierProvider,
    content_fingerprint,
)


# Test-only paths select the existing locked native environments.
ROOT = Path(__file__).resolve().parents[3]
MANTIS_PYTHON = ROOT / "environments/mantis/.venv/bin/python"
CLASSIFIER_PYTHON = ROOT / "environments/classifiers/.venv/bin/python"
MANTIS_WORKER = ROOT / "src/python/04_05_encode_mantis.py"
CLASSIFIER_TRAINING_WORKER = ROOT / "src/python/04_06_train_random_forest.py"
CLASSIFIER_PREDICTION_WORKER = ROOT / "src/python/04_08_predict_random_forest.py"
FIXED_DEFINITION_ID = "representation-definition/fixed"


def run_json(python: Path, arguments: list[str], request: dict | None = None) -> dict:
    """Run one isolated native worker call and decode its sole stdout value."""
    completed = subprocess.run(
        [str(python), *arguments],
        cwd=ROOT,
        input=None if request is None else json.dumps(request),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout)


def prepared(
    identity: str,
    role: str,
    offset: float,
    labels: tuple[int, ...] | None,
) -> PreparedDirectionalInput:
    """Create one asymmetric common Process 03 input for contract tests."""
    values = np.linspace(-2.5 + offset, 4.75 + offset, 64, dtype=np.float64)
    values[7] += 0.375
    return PreparedDirectionalInput.create(
        input_id=identity,
        source_series_id=f"source/{identity}",
        role=role,
        values=values,
        label_reference=float(values[-1]),
        labels=labels,
        preparation_definition_id="preparation/standardise-sample-v1",
        preparation_fingerprint="preparation-fingerprint",
        membership_fingerprint="membership/s1/fingerprint",
    )


def representation(source: PreparedDirectionalInput, value: float) -> RepresentationRecord:
    """Create one valid portable fixed representation with common-input lineage."""
    values = np.linspace(value, value + 1.0, 256, dtype=np.float32).tolist()
    representation_id = "representation/" + content_fingerprint(
        {"definition": FIXED_DEFINITION_ID, "input": source.input_id}
    )[:32]
    return RepresentationRecord(
        representation_id=representation_id,
        input_id=source.input_id,
        source_series_id=source.source_series_id,
        role=source.role,
        definition_id=FIXED_DEFINITION_ID,
        values=tuple(values),
        dtype="float32",
        dimension=256,
        input_fingerprint=source.input_fingerprint,
        preparation_definition_id=source.preparation_definition_id,
        preparation_fingerprint=source.preparation_fingerprint,
        membership_fingerprint=source.membership_fingerprint,
        representation_fingerprint=content_fingerprint(values),
        worker_provenance={"fixture": "fixed"},
    )


def classification_result(
    job: ClassificationJob,
    dataset: ClassificationDataset,
    *,
    job_id: str | None = None,
    horizon: int | None = None,
    classifier_id: str | None = None,
    parameters: dict[str, Any] | None = None,
    training_fingerprint: str | None = None,
    evaluation_fingerprint: str | None = None,
    evaluation_identities: tuple[str, ...] | None = None,
) -> ClassificationResult:
    """Build one internally valid result, including deliberately unrelated variants."""
    result_horizon = job.horizon if horizon is None else horizon
    result_classifier = job.specification.classifier_id if classifier_id is None else classifier_id
    result_training = (
        job.training_fingerprint if training_fingerprint is None else training_fingerprint
    )
    result_evaluation = (
        job.evaluation_fingerprint
        if evaluation_fingerprint is None
        else evaluation_fingerprint
    )
    identities = (
        dataset.evaluation_identities
        if evaluation_identities is None
        else evaluation_identities
    )
    predictions = tuple(
        (identity, (index + result_horizon) % 2)
        for index, identity in enumerate(identities)
    )
    output = [
        {"identity": identity, "prediction": prediction}
        for identity, prediction in predictions
    ]
    run_content = {
        "classifier": result_classifier,
        "horizon": result_horizon,
        "training": result_training,
        "evaluation": result_evaluation,
    }
    return ClassificationResult(
        classifier_run_id="classifier-run/" + content_fingerprint(run_content)[:32],
        job_id=job.job_id if job_id is None else job_id,
        horizon=result_horizon,
        classifier_id=result_classifier,
        evaluation_identities=identities,
        predictions=predictions,
        resolved_parameters=(
            dict(job.specification.parameters) if parameters is None else parameters
        ),
        training_fingerprint=result_training,
        evaluation_fingerprint=result_evaluation,
        output_fingerprint=content_fingerprint(output),
    )


class NativeMantisWorkerTests(unittest.TestCase):
    """Prove the pinned native encoder preserves the historical scientific path."""

    @classmethod
    def setUpClass(cls):
        """Load Mantis once and collect bounded parity and rejection evidence."""
        if not MANTIS_PYTHON.is_file():
            raise unittest.SkipTest("locked Mantis environment is unavailable")
        program = textwrap.dedent(
            f"""
            import importlib.util
            import sys
            import numpy as np
            import torch

            path = {str(MANTIS_WORKER)!r}
            spec = importlib.util.spec_from_file_location("mantis_worker_test", path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)

            provider = module.MantisRepresentationProvider(device="cpu", batch_size=2)
            try:
                description = provider.describe()
                inputs = np.stack([
                    np.linspace(-3.25, 8.5, 64, dtype=np.float64),
                    np.sin(np.linspace(0.0, 5.0, 64, dtype=np.float64)),
                ])
                inputs[0, 11] += 0.625

                # Independent copy of the historical mantis_models.py resize path.
                historical = torch.nn.functional.interpolate(
                    torch.tensor(inputs[:, np.newaxis, :], dtype=torch.float32),
                    size=512,
                    mode="linear",
                    align_corners=False,
                ).cpu().numpy().astype(np.float32)
                resized = module.resize_for_mantis(inputs)
                embeddings = provider.transform(inputs)
                historical_embeddings = np.asarray(
                    provider._trainer.transform(historical, batch_size=2),
                    dtype=np.float32,
                )
                with torch.no_grad():
                    final_layer_cls = provider._network(
                        torch.tensor(historical, dtype=torch.float32).to(provider.device)
                    ).cpu().numpy().astype(np.float32)

                values = inputs[0].tolist()
                request = {{
                    "representation_id": "representation/asymmetric",
                    "input_id": "input/asymmetric",
                    "source_series_id": "source/asymmetric",
                    "role": "training",
                    "definition_id": description["definition_id"],
                    "values": values,
                    "input_fingerprint": module.content_fingerprint(values),
                    "preparation_definition_id": "preparation/standardise-sample-v1",
                    "preparation_fingerprint": "preparation-fingerprint",
                    "membership_fingerprint": "membership/s1/fingerprint",
                }}
                record = provider.encode([request])[0]

                rejected = {{}}
                for name, changed in (
                    ("definition", {{**request, "definition_id": "representation-definition/other"}}),
                    ("labels", {{**request, "labels": [0] * 14}}),
                ):
                    try:
                        provider.encode([changed])
                    except ValueError:
                        rejected[name] = True
                nonfinite = values.copy()
                nonfinite[5] = float("nan")
                try:
                    provider.encode([{{
                        **request,
                        "values": nonfinite,
                        "input_fingerprint": module.content_fingerprint(nonfinite),
                    }}])
                except ValueError:
                    rejected["nonfinite"] = True
                try:
                    module.validate_device("cuda")
                except RuntimeError:
                    rejected["device"] = True

                print(module.canonical_json({{
                    "description": description,
                    "resize_equal": bool(np.array_equal(resized, historical)),
                    "trainer_equal": bool(np.array_equal(embeddings, historical_embeddings)),
                    "final_cls_equal": bool(np.array_equal(embeddings, final_layer_cls)),
                    "shape": list(embeddings.shape),
                    "dtype": str(embeddings.dtype),
                    "finite": bool(np.isfinite(embeddings).all()),
                    "request": request,
                    "record": record,
                    "rejected": rejected,
                }}))
            finally:
                provider.close()
            """
        )
        completed = subprocess.run(
            [str(MANTIS_PYTHON), "-c", program],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        cls.evidence = json.loads(completed.stdout)

    def test_historical_resize_and_legacy_final_cls_are_exact(self):
        """The approved adapter is exactly equal to the historical native operations."""
        self.assertTrue(self.evidence["resize_equal"])
        self.assertTrue(self.evidence["trainer_equal"])
        self.assertTrue(self.evidence["final_cls_equal"])

    def test_representation_contract_preserves_common_input_lineage(self):
        """The worker returns finite float32×256 values with unchanged input lineage."""
        record = self.evidence["record"]
        request = self.evidence["request"]
        self.assertEqual(self.evidence["shape"], [2, 256])
        self.assertEqual(self.evidence["dtype"], "float32")
        self.assertTrue(self.evidence["finite"])
        self.assertEqual(record["dimension"], 256)
        self.assertEqual(record["dtype"], "float32")
        for field in (
            "representation_id",
            "input_id",
            "source_series_id",
            "role",
            "definition_id",
            "input_fingerprint",
            "preparation_definition_id",
            "preparation_fingerprint",
            "membership_fingerprint",
        ):
            self.assertEqual(record[field], request[field])

    def test_worker_rejects_invalid_contracts_without_device_fallback(self):
        """Labels, non-finite values, wrong definitions, and unavailable devices fail."""
        self.assertEqual(
            self.evidence["rejected"],
            {"definition": True, "labels": True, "nonfinite": True, "device": True},
        )

    def test_describe_preflight_does_not_load_a_second_network(self):
        """Preflight verifies runtime/checkpoint bytes; only encoding loads Mantis."""
        completed = subprocess.run(
            [str(MANTIS_PYTHON), str(MANTIS_WORKER), "describe", "--device", "cpu"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        description = json.loads(completed.stdout)
        self.assertEqual(
            description["definition_id"], self.evidence["description"]["definition_id"]
        )
        self.assertNotIn("Loading weights", completed.stderr)


class RandomForestWorkerTests(unittest.TestCase):
    """Exercise the generic classifier solely in the locked classifiers environment."""

    @classmethod
    def setUpClass(cls):
        """Load the authoritative classifier definition once for focused tests."""
        if not CLASSIFIER_PYTHON.is_file():
            raise unittest.SkipTest("locked classifiers environment is unavailable")
        cls.description = run_json(
            CLASSIFIER_PYTHON, [str(CLASSIFIER_TRAINING_WORKER), "describe"]
        )["runtime"]
        cls.specification = ClassifierSpec.from_dict(cls.description["classifier"])

    def fixed_dataset(self) -> ClassificationDataset:
        """Return one asymmetric fixed matrix with independently predictable classes."""
        training = np.zeros((8, 256), dtype=np.float32)
        training[:, 0] = np.array([-4, -3, -2, -1, 1, 2, 3, 4], dtype=np.float32)
        evaluation = np.zeros((2, 256), dtype=np.float32)
        evaluation[:, 0] = [-2.5, 2.5]
        return ClassificationDataset.create(
            training_identities=[f"representation/train-{index}" for index in range(8)],
            training_features=training,
            evaluation_identities=("representation/eval-negative", "representation/eval-positive"),
            evaluation_features=evaluation,
            feature_dimension=256,
            feature_dtype="float32",
        )

    def test_classifier_environment_has_no_mantis_or_torch(self):
        """The RF process neither provides nor imports Mantis/Torch dependencies."""
        self.assertEqual(self.description["environment"], "environments/classifiers")
        self.assertEqual(self.description["scikit-learn"], "1.7.2")
        self.assertFalse(self.description["mantis_available"])
        self.assertFalse(self.description["torch_available"])
        self.assertEqual(self.description["forbidden_modules_loaded"], [])

    def test_one_dataset_is_serialized_once_for_lightweight_horizon_jobs(self):
        """Fourteen jobs contain no repeated feature matrices or row identities."""
        dataset = self.fixed_dataset()
        jobs = tuple(
            ClassificationJob.create(
                horizon=horizon,
                specification=self.specification,
                dataset=dataset,
                training_labels=[0, 0, 0, 0, 1, 1, 1, 1],
            )
            for horizon in range(1, 15)
        )
        request = {
            "dataset": dataset.to_dict(),
            "jobs": [job.to_dict() for job in jobs],
        }
        encoded = json.dumps(request)
        self.assertEqual(encoded.count('"training_features"'), 1)
        self.assertEqual(encoded.count('"evaluation_features"'), 1)
        self.assertTrue(all("training_features" not in job.to_dict() for job in jobs))
        self.assertTrue(all(job.dataset_id == dataset.dataset_id for job in jobs))

    def test_historical_parallel_configuration_matches_one_thread_provider(self):
        """Historical n_jobs=-1 and approved n_jobs=1 produce equal predictions."""
        program = textwrap.dedent(
            """
            import json
            import numpy as np
            from sklearn.ensemble import RandomForestClassifier
            from util.shared_classification import RandomForestClassifierProvider

            training = np.zeros((12, 256), dtype=np.float32)
            training[:, 0] = np.array([-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6])
            training[:, 7] = np.linspace(0.25, 2.75, 12, dtype=np.float32)
            labels = np.array([0] * 6 + [1] * 6, dtype=np.int8)
            evaluation = np.zeros((5, 256), dtype=np.float32)
            evaluation[:, 0] = [-4.5, -1.5, 0.0, 1.5, 4.5]
            evaluation[:, 7] = [0.4, 0.9, 1.5, 2.1, 2.6]

            historical = RandomForestClassifier(
                n_estimators=200,
                random_state=42,
                n_jobs=-1,
            ).fit(training, labels).predict(evaluation).astype(int)
            first = RandomForestClassifierProvider().fit(training, labels).predict(evaluation)
            second = RandomForestClassifierProvider().fit(training, labels).predict(evaluation)
            print(json.dumps({
                "historical": historical.tolist(),
                "first": first.astype(int).tolist(),
                "second": second.astype(int).tolist(),
            }, sort_keys=True))
            """
        )
        completed = subprocess.run(
            [str(CLASSIFIER_PYTHON), "-c", program],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        evidence = json.loads(completed.stdout)
        self.assertEqual(evidence["historical"], evidence["first"])
        self.assertEqual(evidence["first"], evidence["second"])

    def test_worker_response_has_shared_provenance_and_deterministic_results(self):
        """Worker results repeat exactly while provenance appears once in the envelope."""
        dataset = self.fixed_dataset()
        job = ClassificationJob.create(
            horizon=3,
            specification=self.specification,
            dataset=dataset,
            training_labels=[0, 0, 0, 0, 1, 1, 1, 1],
        )
        request = {"dataset": dataset.to_dict(), "jobs": [job.to_dict()]}
        with tempfile.TemporaryDirectory() as directory:
            scope = ["--model-root", directory, "--experiment", "fixture",
                     "--frequency", "D"]
            trained = run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_TRAINING_WORKER), "train", *scope],
                request,
            )
            self.assertEqual(trained["results"][0]["artifact"]["status"], "trained")
            skipped = run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_TRAINING_WORKER), "train", *scope],
                request,
            )
            self.assertEqual(skipped["results"][0]["artifact"]["status"],
                             "skipped_existing")
            first_value = run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_PREDICTION_WORKER), "predict", *scope],
                request,
            )["response"]
            second_value = run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_PREDICTION_WORKER), "predict", *scope],
                request,
            )["response"]
        first = ClassificationResponse.from_dict(first_value)
        second = ClassificationResponse.from_dict(second_value)
        self.assertEqual(first.results, second.results)
        self.assertEqual(
            [prediction for _, prediction in first.results[0].predictions], [0, 1]
        )
        self.assertIn("worker_provenance", first_value)
        self.assertNotIn("worker_provenance", first_value["results"][0])

    def test_missing_one_horizon_retrains_only_that_random_forest(self):
        """An absent fitted file does not fit or replace another completed horizon."""
        dataset = self.fixed_dataset()
        jobs = [
            ClassificationJob.create(
                horizon=horizon,
                specification=self.specification,
                dataset=dataset,
                training_labels=[0, 0, 0, 0, 1, 1, 1, 1],
            )
            for horizon in (3, 4)
        ]
        request = {
            "dataset": dataset.to_dict(),
            "jobs": [job.to_dict() for job in jobs],
        }
        with tempfile.TemporaryDirectory() as directory:
            scope = ["--model-root", directory, "--experiment", "fixture",
                     "--frequency", "D"]
            run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_TRAINING_WORKER), "train", *scope],
                request,
            )
            horizon_3 = Path(directory) / (
                "fixture/directional_mantis_rf/D/model_h03.joblib"
            )
            horizon_4 = Path(directory) / (
                "fixture/directional_mantis_rf/D/model_h04.joblib"
            )
            horizon_3_hash = hashlib.sha256(horizon_3.read_bytes()).hexdigest()
            horizon_4.unlink()
            retrained = run_json(
                CLASSIFIER_PYTHON,
                [str(CLASSIFIER_TRAINING_WORKER), "train", *scope],
                request,
            )
            statuses = {
                int(result["horizon"]): result["artifact"]["status"]
                for result in retrained["results"]
            }
            self.assertEqual(statuses, {3: "skipped_existing", 4: "trained"})
            self.assertEqual(
                hashlib.sha256(horizon_3.read_bytes()).hexdigest(), horizon_3_hash
            )
            self.assertTrue(horizon_4.is_file())

    def test_prediction_worker_has_no_training_call(self):
        """The prediction executable cannot invoke either generic or job fitting."""
        tree = ast.parse(CLASSIFIER_PREDICTION_WORKER.read_text(encoding="utf-8"))
        called_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertTrue({"fit", "fit_job"}.isdisjoint(called_attributes))

    def test_fitted_random_forest_rejects_another_horizon_job(self):
        """A readable provider cannot masquerade as a different fitted contract."""
        dataset = self.fixed_dataset()
        labels = [0, 0, 0, 0, 1, 1, 1, 1]
        fitted_job = ClassificationJob.create(
            horizon=3,
            specification=self.specification,
            dataset=dataset,
            training_labels=labels,
        )
        unrelated_job = ClassificationJob.create(
            horizon=4,
            specification=self.specification,
            dataset=dataset,
            training_labels=labels,
        )
        provider = RandomForestClassifierProvider().fit_job(dataset, fitted_job)
        with self.assertRaisesRegex(ValueError, "artifact differs"):
            provider.predict_job(dataset, unrelated_job)

    def test_complete_parameters_and_invalid_dataset_values(self):
        """The provider resolves every parameter and rejects malformed feature data."""
        parameters = self.specification.parameters
        self.assertEqual(parameters["n_estimators"], 200)
        self.assertEqual(parameters["random_state"], 42)
        self.assertEqual(parameters["n_jobs"], 1)
        self.assertEqual(len(parameters), 19)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            ClassifierSpec(
                **{**self.specification.to_dict(), "feature_dimension": "256"}
            )
        features = np.zeros((2, 256), dtype=np.float32)
        features[0, 4] = math.nan
        with self.assertRaisesRegex(ValueError, "finite matrix"):
            ClassificationDataset.create(
                training_identities=("representation/a", "representation/b"),
                training_features=features,
                evaluation_identities=("representation/evaluation",),
                evaluation_features=np.zeros((1, 256), dtype=np.float32),
                feature_dimension=256,
                feature_dtype="float32",
            )


class DirectionalMantisCompositionTests(unittest.TestCase):
    """Verify common-input lineage, exact reconciliation, and external composition."""

    @classmethod
    def setUpClass(cls):
        """Load the classifier specification used by every composition fixture."""
        description = run_json(
            CLASSIFIER_PYTHON, [str(CLASSIFIER_TRAINING_WORKER), "describe"]
        )["runtime"]
        cls.specification = ClassifierSpec.from_dict(description["classifier"])

    def setUp(self):
        """Create two training and two official common inputs and representations."""
        self.train_a = prepared(
            "input/train-a", "training", 0.0, tuple(index % 2 for index in range(14))
        )
        self.train_b = prepared(
            "input/train-b", "training", 1.0, tuple((index + 1) % 2 for index in range(14))
        )
        self.official_a = prepared("input/official-a", "official_evaluation", 2.0, None)
        self.official_b = prepared("input/official-b", "official_evaluation", 3.0, None)
        self.inputs = (
            self.train_a,
            self.train_b,
            self.official_a,
            self.official_b,
        )
        self.records = tuple(
            representation(source, value)
            for source, value in zip(
                self.inputs, (-1.0, 1.0, 0.25, 0.75), strict=True
            )
        )
        self.dataset = DirectionalMantisComposer.classification_dataset(
            self.inputs, self.records, FIXED_DEFINITION_ID
        )
        self.jobs = DirectionalMantisComposer.classification_jobs(
            self.inputs, self.records, self.dataset, self.specification
        )

    def valid_response(
        self, worker_provenance: dict[str, Any] | None = None
    ) -> ClassificationResponse:
        """Return one complete internally valid response for the submitted jobs."""
        return ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=[classification_result(job, self.dataset) for job in self.jobs],
            worker_provenance=worker_provenance
            or {"host": "fixture", "runtime": {"scikit-learn": "1.7.2"}},
        )

    def test_common_input_boundary_is_preserved_before_mantis_conversion(self):
        """Requests retain Process 03 values/identities/fingerprints and exclude labels."""
        requests = DirectionalMantisComposer.representation_requests(
            self.inputs, FIXED_DEFINITION_ID
        )
        for source, request in zip(self.inputs, requests, strict=True):
            self.assertEqual(request["input_id"], source.input_id)
            self.assertEqual(request["source_series_id"], source.source_series_id)
            self.assertEqual(request["values"], list(source.values))
            self.assertEqual(request["input_fingerprint"], source.input_fingerprint)
            self.assertEqual(
                request["preparation_fingerprint"], source.preparation_fingerprint
            )
            self.assertEqual(
                request["membership_fingerprint"], source.membership_fingerprint
            )
            self.assertEqual(len(request["values"]), 64)
            self.assertNotIn("labels", request)
            self.assertNotIn("label_reference", request)
        with self.assertRaisesRegex(ValueError, "official future labels"):
            prepared("input/official-invalid", "official_evaluation", 0.0, (0,) * 14)

    def test_jobs_reference_one_dataset_and_select_independent_label_columns(self):
        """Fourteen jobs share one dataset and carry only their matching label column."""
        self.assertEqual(len(self.jobs), 14)
        for horizon, job in enumerate(self.jobs, start=1):
            expected = tuple(
                source.labels[horizon - 1] for source in (self.train_a, self.train_b)
            )
            self.assertEqual(job.training_labels, expected)
            self.assertEqual(job.dataset_id, self.dataset.dataset_id)
            self.assertFalse(hasattr(job, "training_features"))
            self.assertFalse(hasattr(job, "evaluation_features"))

    def test_fixed_records_compose_model_neutral_predictions(self):
        """Validated classifier results map representation IDs back to common input IDs."""
        response = self.valid_response()
        predictions = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        self.assertEqual(len(predictions), 28)
        self.assertEqual({item.horizon for item in predictions}, set(range(1, 15)))
        self.assertEqual(
            {item.evaluation_input_id for item in predictions},
            {self.official_a.input_id, self.official_b.input_id},
        )
        self.assertTrue(
            all(
                set(item.scientific_metadata)
                == {
                    "classifier_run_id",
                    "training_fingerprint",
                    "evaluation_fingerprint",
                    "output_fingerprint",
                }
                for item in predictions
            )
        )
        self.assertTrue(
            all("nearest_distance" not in item.scientific_metadata for item in predictions)
        )

    def test_operational_response_provenance_does_not_change_scientific_predictions(self):
        """Cross-host retries differ operationally but preserve scientific outputs."""
        mac_response = self.valid_response(
            {
                "hostname": "mac-worker",
                "platform": "macOS-arm64",
                "device": "cpu",
                "runtime": {"python": "3.12.14", "scikit-learn": "1.7.2"},
                "elapsed_seconds": 1.25,
                "worker_address": "tcp://mac:10001",
            }
        )
        ubuntu_response = self.valid_response(
            {
                "hostname": "ubuntu-worker",
                "platform": "linux-x86_64",
                "device": "cpu",
                "runtime": {"python": "3.12.14", "scikit-learn": "1.7.2"},
                "elapsed_seconds": 2.75,
                "worker_address": "tcp://ubuntu:10002",
            }
        )
        self.assertNotEqual(mac_response.response_id, ubuntu_response.response_id)
        self.assertNotEqual(
            mac_response.worker_provenance_fingerprint,
            ubuntu_response.worker_provenance_fingerprint,
        )
        mac_predictions = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (mac_response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        ubuntu_predictions = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (ubuntu_response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        self.assertEqual(mac_predictions, ubuntu_predictions)
        self.assertTrue(
            all(
                "classification_response_id" not in item.scientific_metadata
                and "worker_provenance_fingerprint" not in item.scientific_metadata
                for item in mac_predictions
            )
        )

    def test_scientific_changes_alter_predictions_or_are_rejected(self):
        """Prediction content changes identity; invalid run or fingerprints fail."""
        response = self.valid_response()
        baseline = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        first = response.results[0]
        changed_pairs = (
            (first.predictions[0][0], 1 - first.predictions[0][1]),
            *first.predictions[1:],
        )
        changed_output = [
            {"identity": identity, "prediction": prediction}
            for identity, prediction in changed_pairs
        ]
        changed_result = ClassificationResult(
            **{
                **first.__dict__,
                "predictions": changed_pairs,
                "output_fingerprint": content_fingerprint(changed_output),
            }
        )
        changed_response = ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=(changed_result, *response.results[1:]),
            worker_provenance=response.worker_provenance,
        )
        changed = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (changed_response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        self.assertEqual(baseline[0].prediction_id, changed[0].prediction_id)
        self.assertNotEqual(baseline[0].content_hash, changed[0].content_hash)
        self.assertNotEqual(
            baseline[0].scientific_metadata["output_fingerprint"],
            changed[0].scientific_metadata["output_fingerprint"],
        )
        for field, value in (
            ("classifier_run_id", "classifier-run/unrelated"),
            ("training_fingerprint", "training/unrelated"),
            ("evaluation_fingerprint", "evaluation/unrelated"),
            ("output_fingerprint", "output/unrelated"),
        ):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    ClassificationResult(**{**first.__dict__, field: value})

    def test_representation_provenance_is_operational_only(self):
        """Worker changes preserve representation, dataset, and prediction science."""
        changed_records = tuple(
            RepresentationRecord(
                **{
                    **record.__dict__,
                    "worker_provenance": {
                        "hostname": "other-worker",
                        "platform": "linux-x86_64",
                        "device": "cuda",
                        "elapsed_seconds": 9.5,
                    },
                }
            )
            for record in self.records
        )
        self.assertEqual(
            [record.representation_fingerprint for record in self.records],
            [record.representation_fingerprint for record in changed_records],
        )
        changed_dataset = DirectionalMantisComposer.classification_dataset(
            self.inputs, changed_records, FIXED_DEFINITION_ID
        )
        self.assertEqual(self.dataset, changed_dataset)
        response = self.valid_response()
        original_predictions = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (response,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        changed_predictions = DirectionalMantisComposer.directional_predictions(
            changed_dataset,
            self.jobs,
            (response,),
            changed_records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        self.assertEqual(original_predictions, changed_predictions)

    def test_composition_rejects_every_submitted_job_result_mismatch(self):
        """Missing, extra, stale, swapped, and unrelated valid results all fail."""
        valid = [classification_result(job, self.dataset) for job in self.jobs]
        changed_parameters = dict(self.specification.parameters)
        changed_parameters["max_depth"] = 3
        variants = {
            "missing": valid[:-1],
            "additional": [
                *valid,
                classification_result(
                    self.jobs[0], self.dataset, job_id="classification-job/unrelated"
                ),
            ],
            "swapped horizon": [
                classification_result(self.jobs[0], self.dataset, horizon=2),
                *valid[1:],
            ],
            "unrelated classifier": [
                classification_result(
                    self.jobs[0], self.dataset, classifier_id="classifier/unrelated"
                ),
                *valid[1:],
            ],
            "changed parameters": [
                classification_result(
                    self.jobs[0], self.dataset, parameters=changed_parameters
                ),
                *valid[1:],
            ],
            "stale training": [
                classification_result(
                    self.jobs[0], self.dataset, training_fingerprint="stale-training"
                ),
                *valid[1:],
            ],
            "stale evaluation": [
                classification_result(
                    self.jobs[0], self.dataset, evaluation_fingerprint="stale-evaluation"
                ),
                *valid[1:],
            ],
            "missing evaluation identity": [
                classification_result(
                    self.jobs[0],
                    self.dataset,
                    evaluation_identities=self.dataset.evaluation_identities[:1],
                ),
                *valid[1:],
            ],
            "additional evaluation identity": [
                classification_result(
                    self.jobs[0],
                    self.dataset,
                    evaluation_identities=(
                        *self.dataset.evaluation_identities,
                        "representation/unrelated-evaluation",
                    ),
                ),
                *valid[1:],
            ],
        }
        for name, results in variants.items():
            with self.subTest(name=name):
                response = ClassificationResponse.create(
                    dataset_id=self.dataset.dataset_id,
                    results=results,
                    worker_provenance={"host": "fixture"},
                )
                with self.assertRaisesRegex(
                    ValueError, "submitted job set|submitted job"
                ):
                    DirectionalMantisComposer.directional_predictions(
                        self.dataset,
                        self.jobs,
                        (response,),
                        self.records,
                        experiment_id="experiment/mantis",
                        model_definition_id="directional-model/mantis-rf",
                    )

    def test_multiple_worker_envelopes_equal_single_host_grouping(self):
        """Independent Mac/Ubuntu horizon results retain provenance and equal science."""
        results = [classification_result(job, self.dataset) for job in self.jobs]
        single = ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=results,
            worker_provenance={"hostname": "mac", "elapsed_seconds": 4.0},
        )
        mac = ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=results[:6],
            worker_provenance={"hostname": "mac", "elapsed_seconds": 1.5},
        )
        ubuntu = ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=results[6:],
            worker_provenance={"hostname": "ubuntu", "elapsed_seconds": 2.0},
        )
        expected = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (single,),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        distributed = DirectionalMantisComposer.directional_predictions(
            self.dataset,
            self.jobs,
            (mac, ubuntu),
            self.records,
            experiment_id="experiment/mantis",
            model_definition_id="directional-model/mantis-rf",
        )
        self.assertEqual(distributed, expected)
        self.assertNotEqual(mac.response_id, ubuntu.response_id)

        duplicate = ClassificationResponse.create(
            dataset_id=self.dataset.dataset_id,
            results=(results[0],),
            worker_provenance={"hostname": "ubuntu"},
        )
        with self.assertRaisesRegex(ValueError, "submitted job set"):
            DirectionalMantisComposer.directional_predictions(
                self.dataset,
                self.jobs,
                (single, duplicate),
                self.records,
                experiment_id="experiment/mantis",
                model_definition_id="directional-model/mantis-rf",
            )

    def test_requested_representation_definition_and_lineage_are_authoritative(self):
        """Returned records cannot substitute definitions, IDs, sources, or fingerprints."""
        changes = {
            "definition_id": "representation-definition/other",
            "representation_id": "representation/unrelated",
            "source_series_id": "source/unrelated",
            "role": "official_evaluation",
            "input_fingerprint": "input-fingerprint/unrelated",
            "preparation_fingerprint": "preparation-fingerprint/unrelated",
            "membership_fingerprint": "membership-fingerprint/unrelated",
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                changed = RepresentationRecord(
                    **{**self.records[0].__dict__, field: value}
                )
                with self.assertRaisesRegex(ValueError, "explicit request lineage"):
                    DirectionalMantisComposer.classification_dataset(
                        self.inputs,
                        (changed, *self.records[1:]),
                        FIXED_DEFINITION_ID,
                    )
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            RepresentationRecord(
                **{**self.records[0].__dict__, "representation_fingerprint": "wrong"}
            )
        with self.assertRaisesRegex(ValueError, "256"):
            RepresentationRecord(
                **{**self.records[0].__dict__, "dimension": 255}
            )


if __name__ == "__main__":
    unittest.main()
