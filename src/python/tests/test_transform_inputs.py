"""Accepted Process 03 public boundary arithmetic, independent of storage/flows.

Small arrays protect complete non-overlapping blocks and series-level membership;
the public-flow fixture separately covers cleaning, caps, features and restart.
"""

import unittest

from p03_transform.inputs import rolling_window_inputs, s1_partition


class TransformInputTests(unittest.TestCase):
    """Protect Process 03 window positions and deterministic cohort membership."""

    def test_complete_blocks_drop_partial_tail_without_padding(self):
        """Short/exact/remainder histories expose only complete W+H blocks."""
        for length, count in ((5, 0), (6, 1), (11, 1), (12, 2), (15, 2)):
            with self.subTest(length=length):
                values = [float(index) for index in range(length)]
                windows = rolling_window_inputs(values, 4, 2)
                self.assertEqual(len(windows), count)
                for ordinal, window in enumerate(windows):
                    start = ordinal * 6
                    self.assertEqual(window, {
                        "input_start": start, "input_end": start + 4,
                        "future_start": start + 4, "future_end": start + 6,
                        "raw_input": values[start:start + 4],
                        "actual": values[start + 4:start + 6],
                    })

    def test_missing_values_keep_positions_in_input_and_future(self):
        """Missing future observations are not fabricated or shifted into input."""
        values = [1.0, None, 3.0, 4.0, None, 6.0]
        window = rolling_window_inputs(values, 4, 2)[0]
        self.assertEqual(window["raw_input"], values[:4])
        self.assertEqual(window["actual"], values[4:])

    def test_s1_binary64_floor_is_namespaced_and_order_independent(self):
        """Accepted binary64 floor gives 19, not decimal 20, held-out series."""
        identities = [(dataset, str(index)) for dataset in ("a", "b") for index in range(50)]
        membership = s1_partition(identities, 0.8, 1234)
        self.assertEqual(set(membership), set(identities))
        self.assertEqual(sum(value == "test" for value in membership.values()), 19)
        self.assertEqual(sum(value == "train" for value in membership.values()), 81)
        self.assertEqual(membership, s1_partition(list(reversed(identities)), 0.8, 1234))


if __name__ == "__main__":
    unittest.main()
