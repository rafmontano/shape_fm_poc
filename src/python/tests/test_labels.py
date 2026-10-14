"""Process 03 strict label contract on asymmetric hand-derived fixtures.

Future observations and explicit references produce labels/missingness assertions;
no cross-language, hardware or historical comparison is performed.
"""

import unittest

import numpy as np

from p03_transform.labels import directional_labels


class DirectionalLabelTests(unittest.TestCase):
    """Protect strict comparisons and row-specific reference meaning."""

    def test_vector_ties_negatives_constants_and_near_ties_are_strict(self):
        """Only values strictly above the supplied reference receive class one."""
        cases = (
            ([-3.0, -2.0, -1.0], -2.0, [0.0, 0.0, 1.0]),
            ([5.0, 5.0, 5.0], [5.0], [0.0, 0.0, 0.0]),
            ([np.nextafter(1.0, 0.0), 1.0, np.nextafter(1.0, 2.0)], 1.0, [0.0, 0.0, 1.0]),
        )
        for values, reference, expected in cases:
            with self.subTest(values=values):
                self.assertEqual(directional_labels(values, reference).tolist(), expected)

    def test_matrix_preserves_missing_targets_and_row_references(self):
        """Rows never share references; missing futures remain unavailable."""
        observed = directional_labels([[2.0, 1.0, None], [-1.0, -2.0, -3.0]], [1.0, -2.0])
        self.assertEqual(observed.shape, (2, 3))
        self.assertEqual(observed[:, :2].tolist(), [[1.0, 0.0], [1.0, 0.0]])
        self.assertTrue(np.isnan(observed[0, 2]))
        self.assertEqual(observed[1, 2], 0.0)

    def test_invalid_shapes_references_and_infinity_are_rejected(self):
        """No broadcastable mismatch or nonfinite reference is accepted."""
        for values, references in (([], 0.0), ([[1.0], [2.0]], 0.0),
                                   ([1.0], float("inf")), ([float("inf")], 0.0)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                directional_labels(values, references)


if __name__ == "__main__":
    unittest.main()
