"""Public configuration contracts for the approved v13 capped fixed DTW policy."""

import json
import unittest
from copy import deepcopy
from pathlib import Path

from util.shared_configuration import (
    ExperimentConfigurationError, json_fingerprint,
    load_experiment_configuration, resolve_experiment_configuration,
)

CONFIG = Path(__file__).resolve().parents[3] / "config/experiments/poc2_m4_daily_full_capped_fixed_dtw.json"


class CappedFixedConfigurationTests(unittest.TestCase):
    """Small public-interface tests need no provider or scientific execution."""

    def test_fresh_contract_and_unchanged_backbone(self):
        raw = json.loads(CONFIG.read_text())
        before = deepcopy(raw)
        config = resolve_experiment_configuration(raw)
        legacy = load_experiment_configuration(CONFIG.with_name("poc2_m4_daily_full_paper_tables.json"))
        self.assertEqual(raw, before)
        self.assertEqual(config.original, before)
        self.assertEqual(config.version, 13)
        self.assertEqual(config.seed, 1234)
        self.assertEqual(config.series_count, 4227)
        self.assertEqual(config.date, "2026-10-12")
        self.assertNotEqual(config.name, legacy.name)
        self.assertEqual(config.model_storage["experiment"], config.name)
        self.assertEqual(config.resolved["derived"], legacy.resolved["derived"])
        self.assertEqual(config.evaluation_options, legacy.evaluation_options)
        self.assertEqual(config.workflow["table_reproduction"], legacy.table_reproduction)
        windows = config.workflow["window_preparation"]
        self.assertEqual(windows["training_cohort"], {
            "cap": 10000, "seed": 1234, "method": "historical_first_horizon_stratified_v1"})
        self.assertEqual(windows["split"], legacy.workflow["window_preparation"]["split"])
        self.assertEqual(windows["frequencies"]["D"]["stride"], 78)
        dtw = config.workflow["models"]["directional_dtw"]
        self.assertEqual(dtw["constraint"], {"name": "sakoe_chiba", "proportion": .10, "effective_width": 6})
        self.assertEqual(dtw["width_selection"], "fixed_configuration")
        self.assertEqual(dtw["reference"], config.workflow["models"]["directional_mantis_rf"]["reference"])
        self.assertEqual(dtw["engine"], legacy.workflow["models"]["directional_dtw"]["engine"])
        self.assertNotIn("calibration_tie_rule", dtw)
        self.assertNotIn("directional_calibration", config.execution["batch_sizes"])
        self.assertNotIn("training_cohort", legacy.original["pipeline"]["window_preparation"])
        for model in config.original["models"]:
            if not model.startswith("directional_"):
                self.assertEqual(config.original["models"][model], legacy.original["models"][model])

    def test_policy_and_backbone_rejections(self):
        raw = json.loads(CONFIG.read_text())
        mutations = [
            (("pipeline", "window_preparation", "training_cohort", "cap"), value)
            for value in (None, True, 10000.0, 9999)
        ] + [
            (("pipeline", "window_preparation", "training_cohort", "seed"), 123),
            (("pipeline", "window_preparation", "training_cohort", "method"), "none"),
            (("models", "directional_dtw", "constraint", "proportion"), .11),
            (("models", "directional_dtw", "constraint", "effective_width"), 6.0),
            (("models", "directional_dtw", "constraint", "candidate_proportions"), [.1]),
            (("models", "directional_dtw", "width_selection"), "one_per_horizon"),
            (("models", "directional_dtw", "calibration_tie_rule"), "smallest_effective_width"),
            (("execution", "default", "batch_sizes", "directional_calibration"), 32),
            (("models", "directional_dtw", "engine", "version"), "1.7.0"),
            (("data", "selection", "expected_source_total"), 100),
        ]
        for model in ("directional_dtw", "directional_mantis_rf"):
            mutations.extend([
                (("models", model, "reference", "cap"), None),
                (("models", model, "reference", "sampling"), "none"),
            ])
        for path, value in mutations:
            with self.subTest(path=path, value=value):
                changed = deepcopy(raw)
                target = changed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.assertRaises(ExperimentConfigurationError):
                    resolve_experiment_configuration(changed)

    def test_scientific_identity_retains_actual_policy(self):
        config = load_experiment_configuration(CONFIG)
        science = config.scientific_configuration
        self.assertEqual(science["configuration_version"], 13)
        self.assertEqual(science["pipeline"], config.resolved["pipeline"])
        self.assertEqual(science["models"], config.resolved["models"])
        self.assertEqual(science["evaluation"]["table_reproduction"], config.table_reproduction)
        for section in ("cohort", "constraint", "reference"):
            changed = deepcopy(science)
            if section == "cohort":
                changed["pipeline"]["window_preparation"]["training_cohort"]["seed"] = 123
            elif section == "constraint":
                changed["models"]["directional_dtw"]["constraint"]["proportion"] = .11
            else:
                changed["models"]["directional_mantis_rf"]["reference"]["cap"] = None
            self.assertNotEqual(json_fingerprint(changed), config.scientific_hash)
        changed = deepcopy(config.original)
        changed["execution"]["default"]["batch_sizes"]["directional_prediction"] = 8
        self.assertEqual(resolve_experiment_configuration(changed).scientific_hash, config.scientific_hash)
