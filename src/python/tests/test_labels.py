# ==============================================================================
# test_labels.py
#
# Purpose: Verify the shared R/Python directional-label contract and agreement.
# Inputs: Deterministic vectors, matrices, missing values, and explicit references.
# Outputs: unittest assertions; R interoperability uses JSON stdin/stdout only.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_labels
# ==============================================================================

"""Cross-language tests for approved directional labels."""

from __future__ import annotations

import json
import math
import subprocess
import unittest
from pathlib import Path

import numpy as np

from util.shared_labels import directional_accuracy, directional_labels


ROOT = Path(__file__).resolve().parents[3]
R_LABELS = ROOT / "src/r/util/labels.R"
CONFIGURED_HORIZONS = (60, 48, 48, 48, 48, 14, 13, 18, 8, 6)


def _normalise(values):
    """Convert nan-containing NumPy output to JSON-comparable nested lists."""
    if isinstance(values, list):
        return [_normalise(value) for value in values]
    return None if isinstance(values, float) and math.isnan(values) else values


def _r_labels(values, references, *, matrix: bool = False):
    """Run the authoritative R counterpart over one bounded JSON fixture."""
    script = f"""
source("{R_LABELS.as_posix()}")
to_numeric <- function(x) vapply(x, function(value) {{
  if (is.null(value)) NA_real_ else as.numeric(value)
}}, numeric(1))
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
if (isTRUE(payload$matrix)) {{
  values <- do.call(rbind, lapply(payload$values, to_numeric))
}} else {{
  values <- to_numeric(payload$values)
}}
result <- directional_labels(values, to_numeric(payload$references))
cat(jsonlite::toJSON(
  list(values = as.vector(t(result)), dimensions = dim(result)),
  auto_unbox = TRUE, na = "null", null = "null"
))
"""
    completed = subprocess.run(
        ["Rscript", "--vanilla", "-e", script],
        input=json.dumps(
            {"values": values, "references": references, "matrix": matrix}
        ),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=True,
    )
    result = json.loads(completed.stdout)
    if result["dimensions"] is None:
        return result["values"]
    rows, columns = result["dimensions"]
    return [
        result["values"][start : start + columns]
        for start in range(0, rows * columns, columns)
    ]


class DirectionalLabelTests(unittest.TestCase):
    """Exercise strict comparisons, shape rules, missingness, and R agreement."""

    def test_vector_ties_negatives_constants_and_near_ties_are_strict(self) -> None:
        """Only values strictly above the supplied reference receive class one."""
        below = np.nextafter(1.0, 0.0)
        above = np.nextafter(1.0, 2.0)
        cases = (
            ([-3.0, -2.0, -1.0], -2.0, [0.0, 0.0, 1.0]),
            ([5.0, 5.0, 5.0], [5.0], [0.0, 0.0, 0.0]),
            ([below, 1.0, above], 1.0, [0.0, 0.0, 1.0]),
            ([8.76], 8.76, [0.0]),
            ([8.76], 8.759999999999998, [1.0]),
            ([7.0], 6.0, [1.0]),
        )
        for values, reference, expected in cases:
            with self.subTest(values=values):
                self.assertEqual(directional_labels(values, reference).tolist(), expected)

    def test_shared_directional_accuracy_counts_asymmetric_binary_vectors(self) -> None:
        """Process 06 receives independent counts rather than a DTW-owned evaluator."""
        self.assertEqual(
            directional_accuracy([1, 0, 1, 1], [1, 1, 1, 0]),
            {"correct_count": 2, "evaluation_count": 4, "accuracy": 0.5},
        )
        with self.assertRaises(ValueError):
            directional_accuracy([1, 0], [1])
        with self.assertRaises(ValueError):
            directional_accuracy([2], [1])

    def test_matrix_uses_row_specific_references_and_preserves_missing_targets(self) -> None:
        """Rows never share references and unavailable future values remain unavailable."""
        values = [[2.0, 1.0, None], [-1.0, -2.0, -3.0]]
        expected = [[1.0, 0.0, None], [1.0, 0.0, 0.0]]
        observed = directional_labels(values, [1.0, -2.0])
        self.assertEqual(observed.shape, (2, 3))
        self.assertEqual(_normalise(observed.tolist()), expected)

    def test_all_configured_horizons_equal_separate_bounded_vectors(self) -> None:
        """Each configured H agrees between a row batch and independent vectors."""
        for horizon in sorted(set(CONFIGURED_HORIZONS)):
            first = [float(index - 1) for index in range(horizon)]
            second = [float(2 - index) for index in range(horizon)]
            batched = directional_labels([first, second], [0.0, 1.0])
            separate = np.vstack(
                [directional_labels(first, 0.0), directional_labels(second, 1.0)]
            )
            self.assertTrue(np.array_equal(batched, separate))
            self.assertEqual(
                _r_labels([first, second], [0.0, 1.0], matrix=True),
                batched.astype(int).tolist(),
            )

    def test_invalid_shapes_references_and_infinity_are_rejected(self) -> None:
        """No scalar, multidimensional, non-finite, or broadcastable mismatch is accepted."""
        invalid = (
            (1.0, 0.0),
            ([], 0.0),
            ([[[1.0]]], [0.0]),
            ([1.0, 2.0], [0.0, 1.0]),
            ([[1.0, 2.0], [3.0, 4.0]], 0.0),
            ([[1.0, 2.0], [3.0, 4.0]], [0.0]),
            ([1.0], float("nan")),
            ([1.0], float("inf")),
            ([float("inf")], 0.0),
        )
        for values, references in invalid:
            with self.subTest(values=values, references=references), self.assertRaises(ValueError):
                directional_labels(values, references)

    def test_r_and_python_share_vector_matrix_and_missing_expected_cases(self) -> None:
        """The two authoritative implementations agree on shared asymmetric fixtures."""
        vector = [-3.0, -2.0, -1.0, None, 4.0]
        expected_vector = [0, 0, 1, None, 1]
        self.assertEqual(
            _normalise(directional_labels(vector, -2.0).tolist()), expected_vector
        )
        self.assertEqual(_r_labels(vector, [-2.0]), expected_vector)

        near_ties = [np.nextafter(1.0, 0.0), 1.0, np.nextafter(1.0, 2.0)]
        expected_near_ties = [0, 0, 1]
        self.assertEqual(
            directional_labels(near_ties, 1.0).astype(int).tolist(),
            expected_near_ties,
        )
        self.assertEqual(_r_labels(near_ties, [1.0]), expected_near_ties)

        matrix = [[2.0, 1.0, None], [-1.0, -2.0, -3.0]]
        expected_matrix = [[1, 0, None], [1, 0, 0]]
        self.assertEqual(
            _normalise(directional_labels(matrix, [1.0, -2.0]).tolist()),
            expected_matrix,
        )
        self.assertEqual(_r_labels(matrix, [1.0, -2.0], matrix=True), expected_matrix)


if __name__ == "__main__":
    unittest.main()
