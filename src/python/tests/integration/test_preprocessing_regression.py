"""Regression-check robust preprocessing on the approved 100-series M4 subset."""

from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path

from util.p01_03_gift_eval_source import iter_source_series


class RobustPreprocessingRegressionTests(unittest.TestCase):
    """Compare the worker element-by-element with the legacy tsclean expression."""

    def test_first_100_match_legacy_tsclean(self):
        root = Path(__file__).resolve().parents[4]
        source = root / "data/source/gift_eval/m4_daily"
        contexts = [
            list(series.target[:-14])
            for series in iter_source_series(source, "D", 100)
        ]
        jobs = [
            {
                "id": str(index),
                "context": context,
                "mode": "robust",
                "seasonality": 7,
            }
            for index, context in enumerate(contexts)
        ]
        environment = {**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"}
        worker = subprocess.run(
            ["Rscript", "src/r/02_01_preprocess_series.R"],
            cwd=root,
            input=json.dumps({"action": "preprocess", "jobs": jobs}),
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
            env=environment,
        )
        legacy_code = r'''
          suppressPackageStartupMessages(library(forecast))
          suppressPackageStartupMessages(library(jsonlite))
          payload <- fromJSON(file("stdin"), simplifyVector = FALSE)
          values <- lapply(payload$contexts, function(x) {
            x_ts <- stats::ts(as.numeric(unlist(x)), frequency = payload$seasonality)
            as.numeric(forecast::tsclean(x_ts))
          })
          cat(toJSON(values, digits = 17))
        '''
        legacy = subprocess.run(
            ["Rscript", "-e", legacy_code],
            cwd=root,
            input=json.dumps({"contexts": contexts, "seasonality": 7}),
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
            env=environment,
        )
        observed = [item["values"] for item in json.loads(worker.stdout)["results"]]
        expected = json.loads(legacy.stdout)
        self.assertEqual(len(observed), 100)
        self.assertEqual(observed, expected)
        self.assertTrue(all(len(value) == len(context) for value, context in zip(observed, contexts, strict=True)))


if __name__ == "__main__":
    unittest.main()
