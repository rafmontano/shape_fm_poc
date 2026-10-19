"""Process 04 isolated native directional provider with local fitted-model files.

JSON-lines requests contain inputs/settings and ordinary local model paths.
Responses contain embeddings, temporary fitted-file references or binary labels.
Provider libraries stay in their pinned native environments.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import pickle
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any

CODE_ROOT = Path(__file__).resolve().parents[3]


def fit_directional(request: dict[str, Any]) -> dict[str, Any]:
    """Fit one reusable DTW library or horizon RF under docs/directional-models.md.

    DTW stores ordered references once with fixed .10 width. RF uses the complete
    configured 200-tree, seed-42, one-thread parameters and float32 embeddings.
    No calibration, fine-tuning or forecast fitting occurs here.
    """
    import numpy as np
    if request["model"] == "directional_dtw":
        references = request["references"]
        model = {"identities": [row["identity"] for row in references],
                 "values": np.asarray([row["values"] for row in references], dtype=np.float64),
                 "labels": np.asarray([row["labels"] for row in references], dtype=np.int8),
                 "window": max(1, round(max(len(row["values"]) for row in references) * request["window"]))}
    elif request["model"] == "directional_mantis_rf":
        from sklearn.ensemble import RandomForestClassifier
        model = RandomForestClassifier(**request["parameters"])
        model.fit(np.asarray(request["values"], dtype=np.float32),
                  np.asarray(request["labels"], dtype=np.int8))
    else:
        raise ValueError(f"unsupported directional model: {request['model']}")
    package = "dtw-python" if request["model"] == "directional_dtw" else "scikit-learn"
    directory = CODE_ROOT / ".runtime/dask/model-fits"
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, suffix=".pickle", delete=False) as stream:
        pickle.dump(model, stream, protocol=5)
        path = stream.name
    return {"temporary_path": path, "host": socket.gethostname(),
            "provider": {package: importlib.metadata.version(package), "numpy": np.__version__}}


def predict_directional(request: dict[str, Any], models: dict[str, Any]) -> dict[str, Any]:
    """Predict one complete horizon vector from accepted fitted state, never refit.

    Euclidean symmetric2 DTW selects one nearest reference; persisted reference
    order resolves ties and the neighbour's labels serve all horizons.
    RF predictions use the independently fitted classifier for each horizon.
    """
    import numpy as np
    for path in request["model_paths"]:
        if path not in models:
            with (CODE_ROOT / path).open("rb") as stream:
                models[path] = pickle.load(stream)
    if request["model"] == "directional_dtw":
        import dtw
        model = models[request["model_paths"][0]]
        query = np.asarray(request["values"], dtype=np.float64)
        distances = []
        widths = []
        for reference in model["values"]:
            reference = np.asarray(reference, dtype=np.float64)
            width = max(1, min(model["window"], len(reference) - 1, len(query) - 1))
            widths.append(width)
            distance = float("inf")
            if len(reference) >= 2 and len(query) >= 2:
                try:
                    distance = float(dtw.dtw(reference, query, dist_method="euclidean",
                        step_pattern="symmetric2", distance_only=True, open_begin=False,
                        open_end=False, window_type="sakoechiba",
                        window_args={"window_size": width}).distance)
                except Exception:
                    # The historical R helper treats an unusable pair as Inf.
                    pass
            distances.append(distance if np.isfinite(distance) else float("inf"))
        if not any(np.isfinite(distance) for distance in distances):
            raise ValueError("DTW has no usable nearest reference")
        nearest = int(np.argmin(distances))
        return {"labels": model["labels"][nearest].tolist(),
                "nearest_reference": model["identities"][nearest],
                "distance": distances[nearest], "effective_width": widths[nearest]}
    if request["model"] != "directional_mantis_rf":
        raise ValueError(f"unsupported directional model: {request['model']}")
    query = np.asarray([request["values"]], dtype=np.float32)
    return {"labels": [int(models[identity].predict(query)[0])
                       for identity in request["model_paths"]]}


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
                elif request["command"] == "fit":
                    response = fit_directional(request)
                else:
                    response = predict_directional(request, models)
                print(json.dumps(response), flush=True)
            except Exception as error:
                print(json.dumps({"error": f"{socket.gethostname()}: {type(error).__name__}: {error}"}), flush=True)
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
