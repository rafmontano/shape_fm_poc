# ==============================================================================
# test_transformations.py
#
# Purpose: Verify the v4 sample-standardisation, interoperability, and utilities.
# Inputs: Hand-calculated fixtures, Rscript/jsonlite, and committed configurations.
# Outputs: unittest assertions only; temporary JSON is transferred through stdin.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_transformations
# ==============================================================================

"""Verify the approved portable standardisation contract in Python and R."""

from __future__ import annotations

import json
import math
import subprocess
import unittest
from pathlib import Path

from util.configuration import (
    ExperimentConfigurationError,
    load_experiment_configuration,
    resolve_experiment_configuration,
)
from util.seasonal_period_tuning import historical_folds
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
from util.window_preparation import prepare_context


ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_standardised.json"
R_TRANSFORMATIONS = ROOT / "src/r/util/transformations.R"


def _r_json(expression: str, payload: object) -> object:
    """Execute one bounded R interoperability expression using JSON stdin/stdout."""
    script = (
        f'source("{R_TRANSFORMATIONS.as_posix()}"); '
        "payload <- jsonlite::fromJSON(file(\"stdin\"), simplifyVector = FALSE); "
        f"result <- {expression}; "
        "cat(jsonlite::toJSON(result, auto_unbox = TRUE, digits = 17, null = \"null\"))"
    )
    completed = subprocess.run(
        ["Rscript", "--vanilla", "-e", script],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=True,
    )
    return json.loads(completed.stdout)


class StandardisationContractTests(unittest.TestCase):
    """Exercise formulas, validation, composition, compatibility, and R interop."""

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

    def test_r_state_runs_in_python_and_python_state_runs_in_r(self) -> None:
        """Both languages exchange exact state through JSON without reinterpretation."""
        r_result = _r_json(
            "local({state <- fit_transformation(unlist(payload$history), "
            "STANDARDISATION_RECIPE); list(state = state, values = "
            "apply_transformation(unlist(payload$future), state), restored = "
            "inverse_transformation(c(-1.5, 2.5), state))})",
            {"history": [10.0, 20.0, 30.0], "future": [0.0, 40.0]},
        )
        self.assertEqual(set(r_result["state"]), {"recipe", "version", "centre", "scale", "count", "constant"})
        self.assertEqual(apply_transformation([0.0, 40.0], r_result["state"]), (-2.0, 2.0))
        self.assertEqual(inverse_transformation([-1.5, 2.5], r_result["state"]), (5.0, 45.0))

        python_state = fit_transformation([-4.0, -2.0, -3.0], STANDARDISATION_RECIPE)
        r_applied = _r_json(
            "list(values = apply_transformation(unlist(payload$values), payload$state), "
            "restored = inverse_transformation(c(-2, 3), payload$state))",
            {"state": python_state, "values": [-5.0, 0.0]},
        )
        self.assert_vector_close(r_applied["values"], apply_transformation([-5.0, 0.0], python_state))
        self.assert_vector_close(r_applied["restored"], inverse_transformation([-2.0, 3.0], python_state))

    def test_r_and_python_fixture_parameters_and_values_agree(self) -> None:
        """Independent implementations agree over asymmetric and boundary fixtures."""
        fixtures = [
            [10.0, 20.0, 30.0],
            [-8.0, -1.5, -3.0, -11.0],
            [5.0, 5.0, 5.0],
            [7.0],
            [1.0, 1.0 + 1e-12, 1.0 - 1e-12],
        ]
        for source in fixtures:
            with self.subTest(source=source):
                python = transform(source, STANDARDISATION_RECIPE)
                r = _r_json(
                    "local({state <- fit_transformation(unlist(payload), "
                    "STANDARDISATION_RECIPE); list(state = state, values = "
                    "apply_transformation(unlist(payload), state))})",
                    source,
                )
                self.assertEqual(r["state"]["constant"], python.parameters["constant"])
                self.assertEqual(r["state"]["count"], python.parameters["count"])
                self.assertTrue(math.isclose(r["state"]["centre"], python.parameters["centre"], rel_tol=1e-12, abs_tol=1e-12))
                self.assertTrue(math.isclose(r["state"]["scale"], python.parameters["scale"], rel_tol=1e-12, abs_tol=1e-15))
                self.assert_vector_close(r["values"], python.values)

    def test_fold_states_use_only_each_training_slice(self) -> None:
        """Historical tuning folds own distinct training-only fitted state."""
        history = [float(index) for index in range(1, 101)]
        changed_future = history + [100_000.0] * 10
        for fold in historical_folds(100, 10):
            state = fit_transformation(history[: fold.train_end], STANDARDISATION_RECIPE)
            repeated = fit_transformation(changed_future[: fold.train_end], STANDARDISATION_RECIPE)
            self.assertEqual(state, repeated)
            self.assertEqual(state["count"], fold.train_end)

    def test_v4_configuration_separates_recipe_identity_and_validates_context(self) -> None:
        """New experiments select the new recipe while v1-v3 semantics stay readable."""
        configuration = load_experiment_configuration(CONFIGURATION)
        self.assertEqual(configuration.version, 4)
        self.assertEqual(configuration.workflow["transformations"], ["identity", STANDARDISATION_RECIPE])
        self.assertEqual(configuration.workflow["window_preparation"], {"context_length": 64})
        self.assertIsNone(configuration.seasonal_period_tuning)
        self.assertEqual(
            configuration.resolved["execution"]["final_acceptance"],
            {
                "mode": "sequential",
                "processes": [1, 2, 3],
                "workers": {"mac_cpu": 1, "total": 1},
                "system_memory_min_available_gib": 2.0,
            },
        )
        self.assertEqual(prepare_context([1.0, 2.0, 3.0], 5), (1.0, 1.0, 1.0, 2.0, 3.0))
        self.assertEqual(prepare_context(range(100), 64), tuple(float(value) for value in range(36, 100)))

        invalid = json.loads(CONFIGURATION.read_text(encoding="utf-8"))
        invalid["pipeline"]["window_preparation"]["context_length"] = 0
        with self.assertRaisesRegex(ExperimentConfigurationError, "context_length"):
            resolve_experiment_configuration(invalid)

        tuned = json.loads(
            (ROOT / "config/experiments/poc2_m4_daily_100_period_tuning.json").read_text(
                encoding="utf-8"
            )
        )
        tuned["configuration_version"] = 4
        tuned["pipeline"]["transformations"]["methods"] = [
            "identity",
            STANDARDISATION_RECIPE,
        ]
        tuned["pipeline"]["window_preparation"] = {"context_length": 64}
        tuned["evaluation"]["provisional_candidate"]["transformation"] = (
            STANDARDISATION_RECIPE
        )
        tuned_configuration = resolve_experiment_configuration(tuned)
        self.assertEqual(
            tuned_configuration.seasonal_period_tuning["validation_windows"], 3
        )
        self.assertEqual(set(tuned_configuration.workflow["models"]), {"auto_arima", "ets"})
        legacy = transform([5.0, 5.0], "minmax_then_standardize")
        self.assertEqual(inverse([-1.0, 1.0], "minmax_then_standardize", legacy.parameters), (5.0, 5.0))


if __name__ == "__main__":
    unittest.main()
