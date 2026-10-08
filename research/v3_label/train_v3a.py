"""
train_v3a.py — trains the v3a shadow variant (docs/V3A_PROTOCOL.md) with the research recipe
itself (v3_label_research._split and _estimator("a", …)): label P&L > 0.10%, 25 inputs, every
labelled signal through v2's trained_through (2026-09-22); the oldest 85% of sessions fit it, the
newest 15% set its threshold (98th percentile). Writes models/variants/v3a.pkl and .json.
Run once; the files are frozen and tracked in git.

    ORBITAL_OFFLINE=1 python research/v3_label/train_v3a.py
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root
_sys.path.insert(0, str(_Path(__file__).resolve().parent))

import datetime as dt
import hashlib
import json
import os
import pickle

os.environ.setdefault("ORBITAL_OFFLINE", "1")

import numpy as np

import registry
import strategy_config as C
import walk_forward as W
from v3_label_research import DROP25, _estimator, _split

ROOT = _Path(__file__).resolve().parents[2]
PROTOCOL = ROOT / "docs" / "V3A_PROTOCOL.md"
OUT = ROOT / "models" / "variants"


def main():
    v2 = registry.baseline()
    cutoff = v2.trained_through
    df, feats = W.load_dataset()
    df = df[df["date"] <= cutoff]
    f25 = [f for f in feats if f not in DROP25]
    fit, cal = _split(df)
    est = _estimator("a", fit, f25)
    cal_scores = est.predict_proba(cal[f25].fillna(0))[:, 1]
    thr = float(np.quantile(cal_scores, C.THRESHOLD_QUANTILE))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "v3a.pkl").write_bytes(pickle.dumps({"model": est, "features": f25, "threshold": thr}))
    meta = {"model_version": "v3a", "role": "shadow variant (decides nothing)",
            "trained_through": cutoff, "trained_from": df["date"].min(), "rows": len(df),
            "fit_rows": len(fit), "cal_rows": len(cal), "calibration_from": cal["date"].min(),
            "threshold": thr, "label": "pnl_% > 0.10 (exit_v1)",
            "positives_fit": int((fit["pnl_%"] > 0.10).sum()), "features": f25,
            "dropped_from_v2": list(DROP25),
            "recipe": "research/v3_label/v3_label_research.py: _split, _estimator('a')",
            "protocol": "docs/V3A_PROTOCOL.md",
            "protocol_sha256": hashlib.sha256(PROTOCOL.read_bytes()).hexdigest(),
            "created": dt.datetime.now().isoformat(timespec="seconds")}
    (OUT / "v3a.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"v3a: {len(df):,} rows (fit {len(fit):,}, calibration {len(cal):,} from {meta['calibration_from']}); "
          f"threshold {thr:.4f}; GO rate on calibration {np.mean(cal_scores >= thr) * 100:.2f}%")
    for f in ("v3a.pkl", "v3a.json"):
        print(f, hashlib.sha256((OUT / f).read_bytes()).hexdigest())


if __name__ == "__main__":
    main()
