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


VARIANTS_DIR = MODELS / "variants"
V2_THRESHOLD_VARIANT = 0.54          # docs/THRESHOLD_PROTOCOL.md, step 6
LATE_CUT = "v2-late-cut"             # docs/LATE_CUT_PROTOCOL.md
LATE_CUTOFF = dt.time(15, 0)         # fixed by that protocol; never re-tuned
LATE_CUT_START = "2026-10-08"        # first session after the protocol commit (1f25674)
V3A = "v3a"                          # docs/V3A_PROTOCOL.md
V3A_START = "2026-10-09"             # first session after the protocol commit (4af13a9)
V3A_SHA256 = "3e6724041b55a530ef1711a36f4de13a160213b8f8402e14e478e7397dffdf63"   # frozen; in the protocol
# every variant a session is expected to log (integrity.py checks the rows against this list;
# a variant that fails to load is skipped by shadow_variants() but still expected here)
EXPECTED_VARIANTS = ("v2.1", "v3a", "v2@0.54", "v2-late-cut")
VARIANTS_EXPECTED_FROM = {"v2.1": "2026-10-07", "v2@0.54": "2026-10-07",      # first live session of each
                          "v2-late-cut": "2026-10-08", "v3a": "2026-10-09"}


def shadow_variants():
    """Variants scored beside the champion on every live signal; they decide nothing
    (docs/V2_1_PROTOCOL.md, docs/THRESHOLD_PROTOCOL.md, docs/LATE_CUT_PROTOCOL.md,
    docs/V3A_PROTOCOL.md). Each is a dict:
    name, version, threshold, and either its own `model` or None (= the frozen v2
    baseline's score, compared with its own threshold)."""
    out = []
    for name, sha in (("v2.1", None), (V3A, V3A_SHA256)):
        v = _model_variant(name, sha)
        if v is not None:
            out.append(v)
    out.append({"name": "v2@0.54", "version": f"{BASELINE_VERSION}@{V2_THRESHOLD_VARIANT}", "model": None,
                "threshold": V2_THRESHOLD_VARIANT})
    # the champion's own GO decisions with entries at or after 15:00 dropped
    # (docs/LATE_CUT_PROTOCOL.md); threshold None = the champion's
    out.append({"name": LATE_CUT, "version": None, "model": "champion", "threshold": None,
                "before": LATE_CUTOFF})
    return out


def _model_variant(name, expected_sha=None):
    """A variant with its own model file, or None. Loading is isolated: a missing, corrupt or
    altered file (hash differs from the one in its protocol) is logged and skipped, and never
    stops the other variants or the session."""
    p = VARIANTS_DIR / f"{name}.pkl"
    if not p.exists():
        return None
    try:
        if expected_sha and sha256(p) != expected_sha:
            raise RuntimeError(f"{p.name} does not match its protocol's SHA-256")
        m = Model(name, p, _read_json(VARIANTS_DIR / f"{name}.json"))
        return {"name": name, "version": name, "model": m, "threshold": m.threshold}
    except Exception as e:                  # noqa: BLE001 — shadow only
        print(f"⚠️ shadow variant {name} not loaded: {type(e).__name__}: {e}")
        return None


def in_late_slice(entry_time):
    """True for an entry at or after 15:00:00 IST (docs/LATE_CUT_PROTOCOL.md: inclusive,
    fixed). entry_time: a datetime.time or "HH:MM[:SS]"."""
    if not isinstance(entry_time, dt.time):
        entry_time = dt.time.fromisoformat(str(entry_time).strip())
    return entry_time >= LATE_CUTOFF


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
    import platform
    # XGBoost is deterministic on one machine but not across CPU architectures
    # (same data, ARM vs x86: ~1% of GO/SKIP decisions differ), so record where.
    meta = {**meta, "version": version, "sha256": sha256(path), "threshold": float(threshold),
            "features": list(features), "platform": f"{platform.system()}-{platform.machine()}"}
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
