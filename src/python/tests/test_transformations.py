"""Process 03/04 sample-standardisation contract with hand-derived fixtures.

Past-only histories and fitted portable state produce assertions on scaling,
inversion and invalid inputs; no cross-language comparison is performed.
"""

from __future__ import annotations

import math
import unittest

from util.transformations import (
    STANDARDISATION_RECIPE,
    apply_steps,
    apply_transformation,
    fit_apply_steps,
    fit_transformation,
    inverse,
    inverse_steps,
    inverse_transformation,
    transform,
)


class StandardisationContractTests(unittest.TestCase):
    """Exercise formulas, validation, composition and inversion."""

    def assert_vector_close(self, observed, expected, tolerance=1e-12) -> None:
        """Require equal lengths and elementwise finite agreement at one tolerance."""
        if isinstance(observed, (int, float)):
            observed = [observed]
        if isinstance(expected, (int, float)):
            expected = [expected]
        self.assertEqual(len(observed), len(expected))
        for left, right in zip(observed, expected, strict=True):
            self.assertTrue(math.isclose(left, right, rel_tol=tolerance, abs_tol=tolerance))

    def test_hand_calculated_and_negative_fixtures_use_sample_sd(self) -> None:
        """The independent hand fixtures distinguish sample from population SD."""
        hand = transform([10.0, 20.0, 30.0], STANDARDISATION_RECIPE)
        self.assertEqual(hand.values, (-1.0, 0.0, 1.0))
        self.assertEqual(
            hand.parameters,
            {
                "recipe": STANDARDISATION_RECIPE,
                "version": 1,
                "centre": 20.0,
                "scale": 10.0,
                "count": 3,
                "constant": False,
            },
        )
        negative = transform([-4.0, -2.0, -3.0], STANDARDISATION_RECIPE)
        self.assertEqual(negative.values, (-1.0, 1.0, 0.0))

    def test_constant_singleton_and_near_constant_contracts_are_distinct(self) -> None:
        """Exact constants use scale one while real small variation remains nonconstant."""
        for source in ([5.0, 5.0, 5.0], [5.0], [0.0, 0.0], [-2.0, -2.0]):
            state = fit_transformation(source, STANDARDISATION_RECIPE)
            self.assertTrue(state["constant"])
            self.assertEqual(state["scale"], 1.0)
            self.assertEqual(apply_transformation(source, state), (0.0,) * len(source))
        state = fit_transformation([1.0, 1.0 + 1e-12, 1.0 - 1e-12], STANDARDISATION_RECIPE)
        self.assertFalse(state["constant"])
        self.assertGreater(state["scale"], 0.0)

    def test_constant_state_preserves_additional_values_labels_and_inverse(self) -> None:
        """A constant fit remains affine instead of collapsing new values to history."""
        state = fit_transformation([5.0, 5.0, 5.0], STANDARDISATION_RECIPE)
        self.assertEqual(apply_transformation([7.0], state), (2.0,))
        self.assertEqual(inverse_transformation([2.0], state), (7.0,))
        self.assertEqual(inverse_transformation([1.5], state), (6.5,))
        self.assertEqual(inverse_transformation([-1.0, 0.0, 2.0], state), (4.0, 5.0, 7.0))
        self.assertEqual(int(7.0 > 5.0), 1)
        self.assertEqual(int(5.0 > 5.0), 0)

    def test_fit_apply_inverse_are_separate_and_support_out_of_range_values(self) -> None:
        """Apply and inverse reuse state for held-out values and recover original units."""
        history = [-7.5, 2.0, 11.25, 4.5]
        state = fit_transformation(history, STANDARDISATION_RECIPE)
        held_out = [-100.0, 100.0]
        transformed = apply_transformation(held_out, state)
        self.assert_vector_close(inverse_transformation(transformed, state), held_out)
        self.assertEqual(state["count"], len(history))

    def test_invalid_inputs_and_malformed_states_fail_clearly(self) -> None:
        """Empty/nonfinite values, aliases, versions, and bad fields never fabricate output."""
        for values in ([], [1.0, float("nan")], [1.0, float("inf")]):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "finite|empty"):
                fit_transformation(values, STANDARDISATION_RECIPE)
        valid = fit_transformation([1.0, 2.0], STANDARDISATION_RECIPE)
        invalid_states = [
            {**valid, "version": 2},
            {**valid, "version": 1.0},
            {**valid, "recipe": "standardize_sample_v1"},
            {**valid, "scale": 0.0},
            {key: value for key, value in valid.items() if key != "count"},
            {**valid, "extra": True},
        ]
        for state in invalid_states:
            with self.subTest(state=state), self.assertRaises(ValueError):
                apply_transformation([3.0], state)

    def test_ordered_steps_apply_forward_and_inverse_reverse(self) -> None:
        """The small registered interface composes state without formula strings."""
        transformed, states = fit_apply_steps(
            [10.0, 20.0, 30.0], ["identity", STANDARDISATION_RECIPE]
        )
        self.assertEqual(transformed, (-1.0, 0.0, 1.0))
        self.assertEqual(apply_steps([40.0], states), (2.0,))
        self.assertEqual(inverse_steps([2.0], states), (40.0,))
        with self.assertRaisesRegex(ValueError, "unsupported ordered"):
            fit_apply_steps([1.0, 2.0], ["minmax_then_standardize"])

if __name__ == "__main__":
    unittest.main()
