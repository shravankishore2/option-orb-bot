"""
Strategy experiments — experiments.py
--------------------------------------
Answers four questions on a strict forward holdout (train on the oldest 80% of
SESSIONS, test on the newest 20%):

  1. What should the model predict?  "trade was green" is the wrong target for
     an option buyer, who needs a BIG move to clear premium and spread.
  2. How selective can we get?  70 signals/day is the core problem.
  3. Given a filtered set, which stop/target actually works?
  4. What would those underlying moves be worth as bought options?

Usage:  python research/v1_exploration/experiments.py [--exp 1|2|3|4]
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root: the shared modules live there

import argparse
import datetime as dt
from math import log, sqrt, exp, erf

import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score

import train_classifier as T
import sweep_exits
from mine_features import FEATURES as EXTRA

SIGNALS_FILE = "data/research/v1/historical_orb_signals.csv"
RESULTS_FILE = "data/research/v1/historical_orb_results.csv"
FEATURES_FILE = "data/research/v1/historical_orb_features.csv"

CANDIDATE_LABELS = [
    ("profit",   "trade ended green"),
    ("hit_1r",   "reached +1.0 x ORB before -1 x ORB"),
    ("hit_1_5r", "reached +1.5 x ORB before -1 x ORB"),
    ("hit_2r",   "reached +2.0 x ORB before -1 x ORB"),
]


def load():
    sig = pd.read_csv(SIGNALS_FILE); sig.columns = [c.lower() for c in sig.columns]
    res = pd.read_csv(RESULTS_FILE); res.columns = [c.lower() for c in res.columns]
    ext = pd.read_csv(FEATURES_FILE); ext.columns = [c.lower() for c in ext.columns]

    keys = ["date", "symbol", "direction"]
    ext = ext.drop(columns=[c for c in ["time"] if c in ext.columns])
    ext = ext.drop_duplicates(subset=keys)

    df = sig.merge(res[keys + ["pnl_%", "exit_reason"]], on=keys, how="inner")
    df = df.merge(ext, on=keys, how="left")

    df, base = T.engineer_features(df)
    df = df.loc[:, ~df.columns.duplicated()]
    df["profit"] = (df["pnl_%"] > 0).astype(int)

    feats = base + [f for f in EXTRA if f in df.columns]
    df[feats] = df[feats].apply(pd.to_numeric, errors="coerce")

    # Keep only signals that carry the forward labels, so every candidate
    # target is compared on exactly the same sample.
    label_cols = [c for c, _ in CANDIDATE_LABELS if c in df.columns and c != "profit"]
    before = len(df)
    df = df.dropna(subset=label_cols + ["mfe_pct"]).reset_index(drop=True)

    for c in label_cols:
        df[c] = df[c].astype(int)

    if before != len(df):
        print(f"   dropped {before - len(df):,} signals with no cached context")

    return df, feats


def split(df):
    days = np.sort(df["date"].unique())
    cut = days[int(len(days) * 0.8)]
    return df["date"] < cut, df["date"] >= cut, cut


def fit(df, feats, tr, te, label):
    y = df[label].astype(int)
    X = df[feats].fillna(0)

    w = (y[tr] == 0).sum() / max((y[tr] == 1).sum(), 1)

    model = XGBClassifier(n_estimators=400, max_depth=4, learning_rate=0.04,
                          subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
                          scale_pos_weight=w, eval_metric="logloss", random_state=42)
    model.fit(X[tr], y[tr])

    return model, model.predict_proba(X[te])[:, 1], y


# ---------------------------------------------------------------- exp 1
def experiment_labels(df, feats, tr, te, cut):
    print(f"\n{'='*78}\n  EXP 1 — WHAT SHOULD THE MODEL PREDICT?"
          f"   (train < {cut}, test >= {cut})\n{'='*78}")

    ndays = df.loc[te, "date"].nunique()
    pnl = df.loc[te, "pnl_%"].to_numpy()
    mfe = df.loc[te, "mfe_pct"].to_numpy()

    print(f"  baseline: take all {te.sum():,} trades "
          f"({te.sum()/ndays:.0f}/day)  win {(pnl>0).mean()*100:.1f}%  "
          f"PnL {pnl.mean():+.3f}%  mean MFE {np.nanmean(mfe):+.2f}%\n")

    print(f"  {'label':<10} {'AUC':>5} {'sel':>5} {'n':>6} {'/day':>5} "
          f"{'win%':>6} {'PnL':>8} {'MFE':>7} {'MFE>1%':>7}")
    print("  " + "-" * 74)

    out = {}
    for label, _ in CANDIDATE_LABELS:
        if label not in df.columns:
            continue

        model, proba, y = fit(df, feats, tr, te, label)
        auc = roc_auc_score(y[te], proba) if y[te].nunique() > 1 else float("nan")
        out[label] = proba

        for q in (0.90, 0.98):
            keep = proba >= np.quantile(proba, q)
            if keep.sum() < 20:
                continue
            print(f"  {label:<10} {auc:>5.3f} top{int((1-q)*100):>2}% {keep.sum():>6,} "
                  f"{keep.sum()/ndays:>5.1f} {(pnl[keep]>0).mean()*100:>5.1f}% "
                  f"{pnl[keep].mean():>+7.3f}% {np.nanmean(mfe[keep]):>+6.2f}% "
                  f"{np.nanmean(mfe[keep]>1.0)*100:>6.1f}%")
        print()

    return out


# ---------------------------------------------------------------- exp 2
def experiment_selectivity(df, feats, tr, te, proba, label):
    print(f"\n{'='*78}\n  EXP 2 — HOW SELECTIVE CAN WE GET?  (label = {label})\n{'='*78}")

    ndays = df.loc[te, "date"].nunique()
    pnl = df.loc[te, "pnl_%"].to_numpy()
    mfe = df.loc[te, "mfe_pct"].to_numpy()

    print(f"  {'keep':>6} {'n':>6} {'trades/day':>11} {'win%':>7} "
          f"{'PnL':>9} {'mean MFE':>9} {'MFE>1%':>8} {'MFE>2%':>8}")
    print("  " + "-" * 70)

    for q in (0.0, 0.5, 0.9, 0.95, 0.98, 0.99, 0.995):
        keep = proba >= np.quantile(proba, q) if q else np.ones(len(proba), bool)
        if keep.sum() < 15:
            continue
        print(f"  {f'top {int((1-q)*100)}%' if q else 'all':>6} {keep.sum():>6,} "
              f"{keep.sum()/ndays:>11.1f} {(pnl[keep]>0).mean()*100:>6.1f}% "
              f"{pnl[keep].mean():>+8.3f}% {np.nanmean(mfe[keep]):>+8.2f}% "
              f"{np.nanmean(mfe[keep]>1.0)*100:>7.1f}% {np.nanmean(mfe[keep]>2.0)*100:>7.1f}%")


# ---------------------------------------------------------------- exp 3
def experiment_exits(df, te, proba, keep_frac=0.02):
    print(f"\n{'='*78}\n  EXP 3 — TARGET / STOP ON THE FILTERED SET "
          f"(top {int(keep_frac*100)}%)\n{'='*78}")

    keep = proba >= np.quantile(proba, 1 - keep_frac)
    chosen = df.loc[te][keep]

    print(f"  loading paths for {len(chosen):,} filtered signals...")
    paths = sweep_exits.load_paths(
        chosen.rename(columns={"orh": "ORH", "orl": "ORL"}), quiet=True
    )
    ndays = chosen["date"].nunique()
    print(f"  {len(paths):,} paths\n")

    rules = [
        ("CURRENT-equivalent trail 0.25 / tgt 0.75", 0.60, 0.75, 0.25),
        ("stop 0.5 / target 1.0 (1:2 R)",            0.50, 1.00, None),
        ("stop 0.5 / target 1.5 (1:3 R)",            0.50, 1.50, None),
        ("stop 0.5 / target 2.0 (1:4 R)",            0.50, 2.00, None),
        ("stop 1.0 / target 2.0 (1:2 R)",            1.00, 2.00, None),
        ("stop 1.0 / target 3.0 (1:3 R)",            1.00, 3.00, None),
        ("stop 1.0 / trail 1.0 / no target",         1.00, None, 1.00),
        ("stop 1.0 / no target / ride to 15:15",     1.00, None, None),
        ("stop 0.5 / no target / ride to 15:15",     0.50, None, None),
    ]

    print(f"  {'rule':<42} {'win%':>6} {'PnL/trade':>10} {'PnL/day':>9}")
    print("  " + "-" * 70)

    best, best_mean = None, -1e9

    for name, s_m, t_m, tr_m in rules:
        vals = [sweep_exits.simulate(p, s_m, t_m, tr_m) for p in paths]
        vals = np.array([v for v in vals if v is not None])
        if not len(vals):
            continue

        if vals.mean() > best_mean:
            best, best_mean = (name, s_m, t_m, tr_m), vals.mean()

        print(f"  {name:<42} {(vals>0).mean()*100:>5.1f}% "
              f"{vals.mean():>+9.3f}% {vals.mean()*len(vals)/ndays:>+8.3f}%")

    print(f"\n  best: {best[0]}  ({best_mean:+.3f}%/trade)")

    return paths, ndays, best


# ---------------------------------------------------------------- exp 4
def _norm_cdf(x):
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def bs_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return max(S - K, 0.0)
    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    return S * _norm_cdf(d1) - K * exp(-r * T) * _norm_cdf(d2)


def experiment_options(df, te, proba, rule,
                       iv=0.32, days_to_expiry=15, spread_pct=2.0, hold_days=0.6):
    print(f"\n{'='*78}\n  EXP 4 — WHAT ARE THOSE MOVES WORTH AS BOUGHT OPTIONS?\n{'='*78}")

    name, s_m, t_m, tr_m = rule

    r = 0.065
    T0 = days_to_expiry / 365.0
    T1 = max((days_to_expiry - hold_days) / 365.0, 1e-6)
    premium = bs_call(100.0, 100.0, T0, r, iv)

    print(f"  MODEL, not real option prices: ATM option, IV {iv:.0%}, "
          f"{days_to_expiry}d to expiry, held")
    print(f"  {hold_days} day, minus {spread_pct:.1f}% round-trip spread. "
          f"Premium = {premium:.2f}% of spot.")
    print(f"  Exit rule: {name}\n")

    ndays_all = df.loc[te, "date"].nunique()

    print(f"  {'keep':>7} {'/day':>6} {'underlying':>11} {'opt gross':>10} "
          f"{'opt net':>9} {'opt win%':>9} {'median':>8}")
    print("  " + "-" * 68)

    for q in (0.90, 0.95, 0.98, 0.99, 0.995):
        keep = proba >= np.quantile(proba, q)
        chosen = df.loc[te][keep]

        if len(chosen) < 40:
            continue

        paths = sweep_exits.load_paths(
            chosen.rename(columns={"orh": "ORH", "orl": "ORL"}), quiet=True
        )

        moves, nets, grosses = [], [], []
        for p in paths:
            mv = sweep_exits.simulate(p, s_m, t_m, tr_m)
            if mv is None:
                continue
            exit_prem = bs_call(100.0 * (1 + mv / 100.0), 100.0, T1, r, iv)
            g = (exit_prem - premium) / premium * 100
            moves.append(mv); grosses.append(g); nets.append(g - spread_pct)

        if not nets:
            continue

        moves, grosses, nets = np.array(moves), np.array(grosses), np.array(nets)

        print(f"  top {int((1-q)*100) if (1-q)>=0.01 else (1-q)*100:>3}% "
              f"{len(nets)/ndays_all:>6.1f} {moves.mean():>+10.3f}% "
              f"{grosses.mean():>+9.2f}% {nets.mean():>+8.2f}% "
              f"{(nets>0).mean()*100:>8.1f}% {np.median(nets):>+7.2f}%")

    print(f"\n  Break-even underlying move for this option "
          f"(premium {premium:.2f}%, {spread_pct:.1f}% spread):")
    for target_move in (0.5, 1.0, 1.5, 2.0, 3.0):
        ep = bs_call(100.0 * (1 + target_move / 100.0), 100.0, T1, r, iv)
        g = (ep - premium) / premium * 100
        print(f"    underlying {target_move:>4.1f}%  ->  option {g:>+7.1f}% gross, "
              f"{g - spread_pct:>+7.1f}% net")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp", type=int, default=0, help="run only one experiment")
    args = parser.parse_args()

    df, feats = load()
    tr, te, cut = split(df)

    print(f"📦 {len(df):,} labelled signals, {len(feats)} features")
    print(f"   train {tr.sum():,} / test {te.sum():,}")

    probas = experiment_labels(df, feats, tr, te, cut)

    best = "hit_1_5r" if "hit_1_5r" in probas else list(probas)[0]

    if args.exp in (0, 2):
        experiment_selectivity(df, feats, tr, te, probas[best], best)

    if args.exp in (0, 3, 4):
        paths, ndays, rule = experiment_exits(df, te, probas[best], keep_frac=0.02)

        if args.exp in (0, 4):
            experiment_options(df, te, probas[best], rule)


if __name__ == "__main__":
    main()
