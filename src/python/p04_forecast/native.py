"""Process 04 isolated native directional provider, without storage or scheduling.

JSON-lines requests contain accepted inputs/settings or trusted fitted bytes.
Responses contain frozen Mantis embeddings, fitted artifacts or binary labels.
Provider libraries stay in their pinned native environments.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import json
import pickle
import sys
from typing import Any


def fit_directional(request: dict[str, Any]) -> dict[str, Any]:
    """Fit one reusable DTW library or horizon RF under docs/directional-models.md.

    DTW stores sorted references once with fixed .10 width. RF uses the complete
    configured 200-tree, seed-42, one-thread parameters and float32 embeddings.
    No calibration, fine-tuning or forecast fitting occurs here.
    """
    import numpy as np
    if request["model"] == "directional_dtw":
        references = sorted(request["references"], key=lambda row: row["identity"])
        model = {"identities": [row["identity"] for row in references],
                 "values": np.asarray([row["values"] for row in references], dtype=np.float64),
                 "labels": np.asarray([row["labels"] for row in references], dtype=np.int8),
                 "window": request["window"]}
    elif request["model"] == "directional_mantis_rf":
        from sklearn.ensemble import RandomForestClassifier
        model = RandomForestClassifier(**request["parameters"])
        model.fit(np.asarray(request["values"], dtype=np.float32),
                  np.asarray(request["labels"], dtype=np.int8))
    else:
        raise ValueError(f"unsupported directional model: {request['model']}")
    package = "aeon" if request["model"] == "directional_dtw" else "scikit-learn"
    return {"artifact": base64.b64encode(pickle.dumps(model, protocol=5)).decode("ascii"),
            "provider": {package: importlib.metadata.version(package), "numpy": np.__version__}}


def predict_directional(request: dict[str, Any], models: dict[str, Any]) -> dict[str, Any]:
    """Predict one complete horizon vector from accepted fitted state, never refit.

    Direct aeon squared-Euclidean DTW at .10 selects one nearest reference; sorted
    identity order resolves ties and the neighbour's labels serve all horizons.
    RF predictions use the independently fitted classifier for each horizon.
    """
    import numpy as np
    if request["model"] == "directional_dtw":
        from aeon.distances import dtw_distance
        model = models[request["artifact_ids"][0]]
        query = np.asarray(request["values"], dtype=np.float64)
        distances = [float(dtw_distance(query, reference, window=model["window"]))
                     for reference in model["values"]]
        nearest = int(np.argmin(distances))
        return {"labels": model["labels"][nearest].tolist(),
                "nearest_reference": model["identities"][nearest],
                "distance": distances[nearest], "effective_width": int(model["window"] * 64)}
    if request["model"] != "directional_mantis_rf":
        raise ValueError(f"unsupported directional model: {request['model']}")
    query = np.asarray([request["values"]], dtype=np.float32)
    return {"labels": [int(models[identity].predict(query)[0])
                       for identity in request["artifact_ids"]]}


def serve(kind: str, device: str) -> None:
    """Own one cached native provider for a worker's JSON-lines session.

    Mantis loads the pinned local checkpoint once. Classifier jobs consume only
    coordinator-supplied accepted inputs/artifacts, not database connections.
    """
    provider = None
    models = {}
    if kind == "mantis":
        import torch
        # The directional contract requires float32, not hardware-dependent
        # TF32 convolution/matmul that can change subsequent RF decisions.
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False
        from p04_forecast.mantis import MantisRepresentationProvider
        provider = MantisRepresentationProvider(device=device)
    print(json.dumps({"type": "ready"}), flush=True)
    try:
        for line in sys.stdin:
            request = json.loads(line)
            if request["command"] == "shutdown":
                return
            try:
                if kind == "mantis":
                    response = {"vectors": provider.transform(request["values"]).tolist(),
                                "provider": provider.describe()}
                elif request["command"] == "register_artifact":
                    content = base64.b64decode(request["artifact"], validate=True)
                    if hashlib.sha256(content).hexdigest() != request["sha256"]:
                        raise ValueError("fitted artifact integrity mismatch")
                    models[request["sha256"]] = pickle.loads(content)
                    del content
                    response = {"type": "registered", "sha256": request["sha256"]}
                elif request["command"] == "fit":
                    response = fit_directional(request)
                else:
                    response = predict_directional(request, models)
                print(json.dumps(response), flush=True)
            except Exception as error:
                print(json.dumps({"error": f"{type(error).__name__}: {error}"}), flush=True)
    finally:
        if provider is not None:
            provider.close()


def main() -> None:
    """Dispatch the Process 04 native session in the specified pinned environment."""
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=("mantis", "classifiers"))
    parser.add_argument("--device", default="cpu", choices=("cpu", "mps", "cuda"))
    arguments = parser.parse_args()
    serve(arguments.kind, arguments.device)


if __name__ == "__main__":
    main()
