"""
registry.py — which models score live signals.

Two roles, both recorded on every live signal:
  baseline   the pre-registered v2 model (models/orbital_model.pkl). Frozen:
             it scores every live signal forever, so there is always a clean
             out-of-sample comparison, and the v2 forward test in
             docs/PREREGISTRATION.md is judged on ITS decisions.
  champion   the model whose GO decisions are sent. Starts as v2; replaced
             only by challenger.py, at most once a month, when a challenger
             beats it on the most recent held-out month.

State lives in models/registry/ (git-ignored, written on the VM):
  champion.json         {"version", "file", "sha256", "promoted_at", ...}
  <version>.pkl/.json   every challenger ever trained, promoted or not
  comparisons.jsonl     one line per monthly champion/challenger comparison
With no champion.json the champion is the baseline.
"""

import datetime as dt
import hashlib
import json
import os
import pickle
from pathlib import Path

import strategy_config as C

BASE_DIR = Path(__file__).resolve().parent
MODELS = BASE_DIR / "models"
REGISTRY = MODELS / "registry"
BASELINE_FILE = MODELS / "orbital_model.pkl"
BASELINE_META = MODELS / "orbital_model.json"
BASELINE_VERSION = C.MODEL_VERSION            # "v2"
CHAMPION_FILE = REGISTRY / "champion.json"
COMPARISONS = REGISTRY / "comparisons.jsonl"


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Model:
    """A scored model: estimator + its feature list, threshold and identity."""

    def __init__(self, version, path, meta=None):
        bundle = pickle.loads(Path(path).read_bytes())
        self.version = version
        self.path = Path(path)
        self.model = bundle["model"]
        self.features = list(bundle["features"])
        self.threshold = float(bundle["threshold"])
        self.meta = meta or {}
        self.sha256 = sha256(path)

    @property
    def trained_through(self):
        return self.meta.get("trained_through")

    def __repr__(self):
        return f"Model({self.version}, thr {self.threshold:.3f}, trained through {self.trained_through})"


def _read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def baseline():
    return Model(BASELINE_VERSION, BASELINE_FILE, _read_json(BASELINE_META))


def champion():
    """The model whose GO decisions are live (the baseline until one is promoted)."""
    info = _read_json(CHAMPION_FILE)
    if not info:
        return baseline()
    path = REGISTRY / info["file"]
    if sha256(path) != info["sha256"]:
        raise RuntimeError(f"champion {info['version']}: {path} does not match its recorded hash")
    return Model(info["version"], path, _read_json(path.with_suffix(".json")))


def _atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def save_model(version, model, features, threshold, meta):
    """Store a challenger as models/registry/<version>.pkl + .json; return its Model."""
    REGISTRY.mkdir(parents=True, exist_ok=True)
    path = REGISTRY / f"{version}.pkl"
    if path.exists():
        raise FileExistsError(f"{path} exists — model versions are never overwritten")
    path.write_bytes(pickle.dumps({"model": model, "features": list(features), "threshold": float(threshold)}))
    meta = {**meta, "version": version, "sha256": sha256(path), "threshold": float(threshold),
            "features": list(features)}
    _atomic_write(path.with_suffix(".json"), json.dumps(meta, indent=2, default=str))
    return Model(version, path, meta)


def promote(m, reason):
    _atomic_write(CHAMPION_FILE, json.dumps({
        "version": m.version, "file": m.path.name, "sha256": m.sha256,
        "promoted_at": dt.datetime.now().isoformat(timespec="seconds"), "reason": reason,
    }, indent=2))


def log_comparison(record):
    REGISTRY.mkdir(parents=True, exist_ok=True)
    with open(COMPARISONS, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def comparisons():
    if not COMPARISONS.exists():
        return []
    return [json.loads(line) for line in COMPARISONS.read_text().splitlines() if line.strip()]
