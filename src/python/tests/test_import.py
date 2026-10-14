"""Bounded pure canonical conversion and read-only synthetic source tests.

Run: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_import
Fixture functions remain shared with active-path tests; no database execution.
"""

import hashlib
import json
import math
import struct
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

from tests.configuration_fixtures import configuration_path
from util.shared_configuration import (
    ExperimentConfiguration, ImportValidationError, load_experiment_configuration,
)
from util.p01_02_import_execution import convert_series, target_content_hash
from util.p01_03_gift_eval_source import (
    GiftEvalSource, SourceSeries, iter_source_series, source_fingerprint,
)


def config(max_series=10):
    """Return a fresh canonical M4 Daily import-settings fixture."""
    return {
        "schema_version": "1",
        "dataset_name": "m4_daily",
        "source_system": "Salesforce/GiftEval",
        "max_series": max_series,
        "benchmark": {
            "frequency": "D", "term": "short", "prediction_length": 14,
            "evaluation_windows": 1,
            "boundary_convention": "zero-based, end-exclusive",
        },
    }


def write_source(path: Path, targets: list[list[float]]) -> None:
    """Write deterministic GiftEval-compatible Arrow and metadata fixtures."""
    path.mkdir(parents=True)
    table = pa.table({
        "item_id": pa.array([str(i) for i in range(len(targets))]),
        "start": pa.array([datetime(2000, 1, 1)] * len(targets), type=pa.timestamp("s")),
        "freq": pa.array(["D"] * len(targets)),
        "target": pa.array(targets, type=pa.list_(pa.float32())),
    })
    with (path / "data-00000-of-00001.arrow").open("wb") as stream:
        with ipc.new_stream(stream, table.schema) as writer:
            writer.write_table(table)
    (path / "dataset_info.json").write_text(json.dumps({"rows": len(targets)}))
    (path / "state.json").write_text(json.dumps({"format": "arrow"}))


class CanonicalTests(unittest.TestCase):
    """Protect the accepted hash encoding and exact end-exclusive window."""

    def test_conversion_preserves_source_and_exact_window(self):
        record = SourceSeries(7, "7", datetime(2000, 1, 1), "D",
                              tuple(float(value) for value in range(107)))
        result = convert_series(record, config())
        self.assertIs(result.source, record)
        self.assertEqual(result.content_hash, hashlib.sha256(
            struct.pack("<107f", *record.target)).hexdigest())
        self.assertEqual(result.window, {
            "window_id": "short/000", "split_name": "validation_and_test",
            "train_start": 0, "train_end": 79,
            "validation_start": 79, "validation_end": 93,
            "test_start": 93, "test_end": 107, "horizon": 14,
            "boundary_convention": "zero-based, end-exclusive",
        })

    def test_null_and_nan_hash_encoding_is_independently_derived(self):
        target = (1.25, None, float("nan"), -2.5)
        expected = hashlib.sha256(
            b"V" + struct.pack("<f", 1.25) + b"N" + b"A"
            + b"V" + struct.pack("<f", -2.5)
        ).hexdigest()
        self.assertEqual(target_content_hash(target), expected)
        self.assertNotEqual(target_content_hash((None,)), target_content_hash((float("nan"),)))


class SourceTests(unittest.TestCase):
    """Exercise the read-only source boundary without persistence or orchestration."""

    def test_missing_and_nan_positions_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            values = [float(value) for value in range(30)]
            values[3], values[7] = None, float("nan")
            write_source(source, [values])
            record = next(iter_source_series(source, "D", 1))
            canonical = convert_series(record, config(1))
            self.assertIsNone(canonical.source.target[3])
            self.assertTrue(math.isnan(canonical.source.target[7]))
            self.assertEqual(canonical.source.target[2], 2.0)

    def test_infinite_observation_is_rejected_clearly(self):
        with tempfile.TemporaryDirectory() as directory:
            for index, value in enumerate((float("inf"), float("-inf"))):
                with self.subTest(value=value):
                    source = Path(directory) / str(index)
                    write_source(source, [[1.0, value, 3.0]])
                    with self.assertRaisesRegex(ImportValidationError, "cannot be infinite"):
                        list(iter_source_series(source, "D", 1))

    def test_source_exposes_bounded_records_and_validated_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "configured" / "m4_daily"
            write_source(source, [[float(value) for value in range(30)]] * 2)
            reference = load_experiment_configuration(
                configuration_path(version=1, directory=directory))
            resolved = deepcopy(reference.resolved)
            resolved["data"]["source"]["directory"] = "configured"
            resolved["data"]["source"]["files"] = {
                name: value["sha256"]
                for name, value in source_fingerprint(source)["files"].items()}
            resolved["data"]["selection"]["count"] = 1
            configured = GiftEvalSource(
                ExperimentConfiguration(reference.original, resolved), root)
            self.assertEqual(configured.metadata()["dataset_info.json"]["rows"], 2)
            self.assertEqual([record.source_series_id for record in configured.records()], ["0"])
            self.assertRegex(configured.fingerprint()["fingerprint"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
