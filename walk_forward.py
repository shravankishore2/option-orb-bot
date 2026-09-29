"""
Walk-forward evaluation — walk_forward.py
------------------------------------------
The honest test: before every calendar month, train a fresh model on ALL data
before that month, set the GO threshold from out-of-sample calibration scores,
then score only that month. No model ever sees the month it's judged on.

Per month:
  train     = every labelled signal dated before the month
  fit/cal   = oldest 85% of training SESSIONS fit the model; the newest 15%
              are scored out-of-sample and their 98th percentile is the
              threshold (an absolute number, usable live)
  test      = that month's signals → score → GO if score >= threshold

Outputs:
  data/research/walkforward.csv     every test-month signal with score + decision
  models/walkforward/YYYY-MM.pkl    the model that would have been live that month

    python walk_forward.py                   # evaluate
    python walk_forward.py --train-live      # model for the live bot (all data)
"""

import argparse
import datetime as dt
import json
import pickle
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

import strategy_config as C
from features import FEATURES as CONTEXT_FEATURES, geometry_features

BASE_DIR = Path(__file__).resolve().parent
RESEARCH = BASE_DIR / "data" / "research"
MODELS = BASE_DIR / "models"

SIGNALS = RESEARCH / "signals_orbital.csv"
RESULTS = RESEARCH / "signals_orbital_results.csv"
FEATS = RESEARCH / "features_orbital.csv"
OUT = RESEARCH / "walkforward.csv"                # the live label (strategy_config.LABEL)


def output_for(label):
    """walkforward.csv for the live label, walkforward_<label>.csv for others."""
    return OUT if label == C.LABEL else RESEARCH / f"walkforward_{label}.csv"


def config_hash():
    return hashlib.sha256((BASE_DIR / "strategy_config.py").read_bytes()).hexdigest()


def load_dataset(signals=SIGNALS, results=RESULTS, feats=FEATS):
    sig = pd.read_csv(signals)
    res = pd.read_csv(results)
    ext = pd.read_csv(feats)
    for f in (sig, res, ext):
        f.columns = [c.lower() for c in f.columns]

    keys = ["date", "symbol", "direction"]
    df = sig.merge(res[keys + ["exit_reason", "pnl_%"]], on=keys, how="inner")
    df = df.merge(ext.drop(columns=["time"], errors="ignore").drop_duplicates(keys),
                  on=keys, how="inner")

    df, base = geometry_features(df)
    df = df.loc[:, ~df.columns.duplicated()]
    features = base + CONTEXT_FEATURES
    df[features] = df[features].apply(pd.to_numeric, errors="coerce")
    # "profit": the trade made money under the actual exit rule (v2 label)
    df["profit"] = (df["pnl_%"] > 0).astype(int)
    df = df.dropna(subset=["hit_1_5r"]).reset_index(drop=True)
    df["hit_1_5r"] = df["hit_1_5r"].astype(int)
    df["month"] = df["date"].str[:7]
    return df, features


def fit_with_threshold(train, features, label=None):
    """Fit on the older sessions, calibrate the threshold on the newer ones."""
    sessions = np.sort(train["date"].unique())
    cut = sessions[int(len(sessions) * (1 - C.CALIBRATION_FRACTION))]
    fit, cal = train[train["date"] < cut], train[train["date"] >= cut]

    y = fit[label or C.LABEL]
    params = dict(C.MODEL_PARAMS)
    params["scale_pos_weight"] = (y == 0).sum() / max((y == 1).sum(), 1)

    model = XGBClassifier(**params)
    model.fit(fit[features].fillna(0), y)

    cal_scores = model.predict_proba(cal[features].fillna(0))[:, 1]
    threshold = float(np.quantile(cal_scores, C.THRESHOLD_QUANTILE))
    return model, threshold, {"fit_rows": len(fit), "cal_rows": len(cal),
                              "calibration_from": str(cut)}


def evaluate(df, features, first_month=None, last_month=None, label=None, save_models=True):
    label = label or C.LABEL
    out_file = output_for(label)
    months = sorted(df["month"].unique())
    if first_month:
        months = [m for m in months if m >= first_month]
    if last_month:
        months = [m for m in months if m <= last_month]

    (MODELS / "walkforward").mkdir(parents=True, exist_ok=True)
    out, log = [], []

    for m in months:
        train = df[df["month"] < m]
        test = df[df["month"] == m].copy()
        if len(train) < C.MIN_TRAIN_SIGNALS or test.empty:
            continue

        model, thr, info = fit_with_threshold(train, features, label)
        test["score"] = model.predict_proba(test[features].fillna(0))[:, 1]
        test["threshold"] = thr
        test["go"] = test["score"] >= thr
        out.append(test)

        if save_models:     # the models the paper test replays — live label only
            with open(MODELS / "walkforward" / f"{m}.pkl", "wb") as f:
                pickle.dump({"model": model, "features": features, "threshold": thr}, f)

        go = test[test["go"]]
        log.append((m, len(train), thr, len(test), len(go), go["pnl_%"].mean() if len(go) else np.nan))
        print(f"  {m}  train {len(train):>7,}  thr {thr:.3f}  signals {len(test):>5,}  "
              f"GO {len(go):>4}  PnL/trade {go['pnl_%'].mean() if len(go) else float('nan'):+.3f}%",
              flush=True)

    result = pd.concat(out, ignore_index=True)
    keep = ["date", "time", "symbol", "direction", "entry_price", "orh", "orl", "prev_close",
            "month", "score", "threshold", "go", "exit_reason", "pnl_%", "hit_1_5r", "profit", "mfe_pct"]
    result[keep].to_csv(out_file, index=False)
    print(f"💾 {out_file} (label {label}, {len(result):,} scored signals)")
    return result


def train_live(df, features):
    model, thr, info = fit_with_threshold(df, features)
    MODELS.mkdir(exist_ok=True)
    with open(MODELS / "orbital_model.pkl", "wb") as f:
        pickle.dump({"model": model, "features": features, "threshold": thr}, f)
    meta = {
        "trained_through": df["date"].max(),
        "trained_from": df["date"].min(),
        "rows": len(df), **info,
        "threshold": thr, "label": C.LABEL,
        "model_version": C.MODEL_VERSION,
        "features": features,
        "config_sha256": config_hash(),
        "created": dt.datetime.now().isoformat(timespec="seconds"),
    }
    (MODELS / "orbital_model.json").write_text(json.dumps(meta, indent=2))
    print(f"💾 models/orbital_model.pkl — trained through {meta['trained_through']}, "
          f"threshold {thr:.3f}, {len(features)} features")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-live", action="store_true")
    ap.add_argument("--first-month", default=None)
    ap.add_argument("--last-month", default=None)
    ap.add_argument("--label", default=None, help="evaluate a different label (no live models saved)")
    a = ap.parse_args()

    df, features = load_dataset()
    print(f"📦 {len(df):,} labelled signals, {df['date'].min()} → {df['date'].max()}, "
          f"{len(features)} features")

    if a.train_live:
        train_live(df, features)
    else:
        label = a.label or C.LABEL
        evaluate(df, features, a.first_month, a.last_month, label=label,
                 save_models=(label == C.LABEL))
