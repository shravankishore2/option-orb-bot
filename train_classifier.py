"""
ORB Trade Classifier — train_classifier.py
------------------------------------------
Merges signal CSV + backtest results, engineers features,
trains XGBoost to predict TARGET hit vs trail/loss.
Saves model to orb_classifier.pkl
"""

import argparse
import pandas as pd
import numpy as np
import pickle
import os
from xgboost import XGBClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score, GroupKFold
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.utils.class_weight import compute_sample_weight

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
# Live-accumulated logs (a few hundred labelled trades).
SIGNALS_FILE  = "backtest_opening_range.csv"
RESULTS_FILE  = "archive/data/backtest_results_intraday.csv"

# Replayed history from build_historical_signals.py (tens of thousands).
HIST_SIGNALS_FILE = "data/research/v1/historical_orb_signals.csv"
HIST_RESULTS_FILE = "data/research/v1/historical_orb_results.csv"

# Optional context features from enrich_features.py (volume, VWAP, ATR,
# breadth). Measured on the forward holdout these lift net expectancy at the
# top decile from +0.040% to +0.109% per trade.
HIST_EXTRA_FILE = "data/research/v1/historical_orb_features.csv"

# git-ignored output (the 4-Sep exploration's model; nothing live loads it)
MODEL_OUTPUT  = "research/v1_exploration/output/orb_classifier.pkl"
FEATURES_OUTPUT = "research/v1_exploration/output/orb_features.pkl"

# ---------------------------------------------------
# LOAD
# ---------------------------------------------------
def _read(path):
    frame = pd.read_csv(path)
    frame.columns = [c.strip().lower() for c in frame.columns]
    return frame


def load_data(source="auto"):
    """source: 'live', 'historical', 'both', or 'auto' (historical if built)."""
    have_hist = os.path.exists(HIST_SIGNALS_FILE) and os.path.exists(HIST_RESULTS_FILE)

    if source == "auto":
        source = "historical" if have_hist else "live"

    if source in ("historical", "both") and not have_hist:
        raise FileNotFoundError(
            f"Missing {HIST_SIGNALS_FILE} / {HIST_RESULTS_FILE} — "
            "run build_historical_signals.py first."
        )

    frames = []

    if source in ("live", "both"):
        if not os.path.exists(SIGNALS_FILE) or not os.path.exists(RESULTS_FILE):
            raise FileNotFoundError(f"Missing: {SIGNALS_FILE} / {RESULTS_FILE}")
        frames.append((_read(SIGNALS_FILE), _read(RESULTS_FILE)))

    if source in ("historical", "both"):
        frames.append((_read(HIST_SIGNALS_FILE), _read(HIST_RESULTS_FILE)))

    print(f"📂 Training source: {source}")

    sig = pd.concat([f[0] for f in frames], ignore_index=True)
    res = pd.concat([f[1] for f in frames], ignore_index=True)

    return sig, res


# ---------------------------------------------------
# FEATURE ENGINEERING
# ---------------------------------------------------
def attach_extra_features(merged):
    """Merge in enrich_features.py output. Returns (frame, extra_feature_names)."""
    if not os.path.exists(HIST_EXTRA_FILE):
        return merged, []

    extra = _read(HIST_EXTRA_FILE)

    keys = ["date", "symbol", "direction"]

    # mine_features.py writes forward-path LABELS into the same file (what the
    # trade went on to do). Those must never become features — training on them
    # would be pure lookahead. Only the declared feature list is admissible.
    try:
        from mine_features import FEATURES as ALLOWED
    except Exception:
        ALLOWED = None

    try:
        from mine_features import LABELS as FORWARD_LABELS
    except Exception:
        FORWARD_LABELS = []

    if ALLOWED is not None:
        names = [c for c in extra.columns if c in ALLOWED]
    else:
        names = [c for c in extra.columns if c not in keys + ["time"]]

    # Carry the forward labels through so apply_label() can use them, but keep
    # them OUT of `names` — they must never be fed to the model as inputs.
    carried = [c for c in FORWARD_LABELS if c in extra.columns]
    extra = extra[keys + names + carried]

    extra = extra.drop(columns=[c for c in ["time"] if c in extra.columns])
    extra = extra.drop_duplicates(subset=keys)

    for frame in (merged, extra):
        frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
        frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
        frame["direction"] = frame["direction"].astype(str).str.upper().str.strip()

    merged = merged.merge(extra, on=keys, how="left")

    print(f"➕ Context features attached: {', '.join(names)}")

    return merged, names


def engineer_features(df):
    """The 9 geometry inputs — defined once, in features.py."""
    from features import geometry_features
    return geometry_features(df)


# ---------------------------------------------------
# MERGE + LABEL
# ---------------------------------------------------
def apply_label(merged, label):
    """Attach the training label. Called after context features are merged."""
    if label.startswith("hit_"):
        if label not in merged.columns:
            raise KeyError(f"{label} not in the data — run mine_features.py first")
        merged = merged.dropna(subset=[label]).copy()
        merged["label"] = merged[label].astype(int)

    elif label == "target":
        merged["label"] = (merged["exit_reason"] == "Target (Midpoint)").astype(int)

    else:
        merged["label"] = (merged["pnl_%"] > 0).astype(int)

    print(f"🎯 Label: {label}")
    print(f"✅ Positive (label=1): {merged['label'].sum():,}")
    print(f"❌ Negative (label=0): {(merged['label']==0).sum():,}")
    print(f"Class ratio: {merged['label'].mean()*100:.1f}% positive\n")

    return merged


def prepare_dataset(sig, res):
    """Merge signals with their outcomes. Labelling happens in apply_label()."""
    for frame in (sig, res):
        frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
        frame["symbol"] = frame["symbol"].str.upper().str.strip()
        frame["direction"] = frame["direction"].str.upper().str.strip()

    res_slim = res[["date", "symbol", "direction", "exit_reason", "pnl_%"]].copy()

    # One row per trade — the live and historical logs overlap in time.
    keys = ["date", "symbol", "direction"]
    sig = sig.drop_duplicates(subset=keys, keep="last")
    res_slim = res_slim.drop_duplicates(subset=keys, keep="last")

    merged = pd.merge(sig, res_slim, on=keys, how="inner")

    print(f"\n📦 Merged rows: {len(merged):,}")
    print(f"Exit reason breakdown:\n{merged['exit_reason'].value_counts()}\n")

    return merged


# ---------------------------------------------------
# TRAIN
# ---------------------------------------------------
def train(merged, extra_features=()):

    df_feat, features = engineer_features(merged)

    # Context features are already columns on the frame; just widen the list.
    features = features + [f for f in extra_features if f in df_feat.columns]

    df_feat[features] = df_feat[features].apply(pd.to_numeric, errors="coerce")

    X = df_feat[features].fillna(0)
    y = df_feat["label"]

    # Class imbalance — weight positives higher
    pos = y.sum()
    neg = len(y) - pos
    scale_pos_weight = neg / pos
    print(f"⚖️  scale_pos_weight = {scale_pos_weight:.2f} (handles class imbalance)\n")

    model = XGBClassifier(
        n_estimators=200,
        max_depth=3,           # shallow — prevents overfit on small data
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=42,
    )

    # Group folds by DATE so trades from the same session never straddle a
    # fold boundary — dozens of signals share one day's market regime, and
    # splitting them randomly leaks that regime and inflates the score.
    if df_feat["date"].nunique() >= 10:
        cv = GroupKFold(n_splits=5)
        cv_scores = cross_val_score(model, X, y, cv=cv, groups=df_feat["date"],
                                    scoring="roc_auc")
        label = "Grouped-by-day CV ROC-AUC"
    else:
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
        label = "Stratified CV ROC-AUC"

    print(f"📊 {label}: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"   Per-fold: {[round(s,3) for s in cv_scores]}\n")

    # Hold out the most recent 20% of SESSIONS — the honest test of whether
    # this generalises forward, which cross-validation cannot tell you.
    days = np.sort(df_feat["date"].unique())
    split_day = days[int(len(days) * 0.8)]

    is_train = df_feat["date"] < split_day

    if is_train.sum() and (~is_train).sum() and y[is_train].nunique() > 1:
        holdout = XGBClassifier(**model.get_params())
        holdout.fit(X[is_train], y[is_train])

        from sklearn.metrics import roc_auc_score
        proba = holdout.predict_proba(X[~is_train])[:, 1]

        if y[~is_train].nunique() > 1:
            print(f"⏳ Forward holdout (trained < {split_day}, tested >= {split_day})")
            print(f"   {is_train.sum():,} train / {(~is_train).sum():,} test rows")
            print(f"   ROC-AUC: {roc_auc_score(y[~is_train], proba):.3f}")

            # AUC alone can look great while the filter loses money, so state
            # the only thing that matters: PnL of the trades it would keep.
            if "pnl_%" in df_feat.columns:
                pnl = df_feat.loc[~is_train, "pnl_%"].values

                print(f"\n   Economic value on that holdout:")
                print(f"     take ALL   {len(pnl):>6,} trades  "
                      f"mean PnL {pnl.mean():+.3f}%  win {(pnl > 0).mean()*100:.1f}%")

                for keep_frac in (0.5, 0.3, 0.1):
                    cut = np.quantile(proba, 1 - keep_frac)
                    keep = proba >= cut

                    if keep.sum():
                        print(f"     top {int(keep_frac*100):>2}%   {keep.sum():>6,} trades  "
                              f"mean PnL {pnl[keep].mean():+.3f}%  "
                              f"win {(pnl[keep] > 0).mean()*100:.1f}%   "
                              f"(threshold {cut:.2f})")
                print()

    # Train on full data
    model.fit(X, y)

    # Feature importance
    importance = pd.Series(model.feature_importances_, index=features)
    importance = importance.sort_values(ascending=False)
    print("🔍 Feature Importance:")
    for feat, imp in importance.items():
        bar = "█" * int(imp * 40)
        print(f"  {feat:<25} {bar} {imp:.3f}")

    # Full data report (for reference — not CV)
    y_pred = model.predict(X)
    print(f"\n📋 Classification Report (full training data — not CV):")
    print(classification_report(y, y_pred, target_names=["No Target", "Target"]))

    print("Confusion Matrix:")
    cm = confusion_matrix(y, y_pred)
    print(f"  TN={cm[0,0]}  FP={cm[0,1]}")
    print(f"  FN={cm[1,0]}  TP={cm[1,1]}")

    return model, features


# ---------------------------------------------------
# SAVE
# ---------------------------------------------------
def save_model(model, features):
    os.makedirs(os.path.dirname(MODEL_OUTPUT), exist_ok=True)
    with open(MODEL_OUTPUT, "wb") as f:
        pickle.dump(model, f)
    with open(FEATURES_OUTPUT, "wb") as f:
        pickle.dump(features, f)
    print(f"\n💾 Model saved → {MODEL_OUTPUT}")
    print(f"💾 Features saved → {FEATURES_OUTPUT}")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Train the ORB classifier")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--historical", action="store_const", dest="source",
                       const="historical", help="train on replayed history only")
    group.add_argument("--live", action="store_const", dest="source",
                       const="live", help="train on the live signal log only")
    group.add_argument("--both", action="store_const", dest="source",
                       const="both", help="train on live + historical combined")
    parser.add_argument("--no-context", action="store_true",
                        help="ignore historical_orb_features.csv (geometry only)")
    parser.add_argument("--label",
                        choices=["profit", "target", "hit_1r", "hit_1_5r",
                                 "hit_2r", "hit_3r"],
                        default="profit",
                        help="what to predict: 'profit' = trade made money "
                             "(default, best for cash/futures); 'hit_1_5r' etc. "
                             "= underlying made a big move (use for option "
                             "buying); 'target' = original exit_reason label")
    parser.set_defaults(source="auto")
    args = parser.parse_args()

    print("🚀 ORB Classifier Training\n" + "="*40)

    sig, res = load_data(args.source)
    merged = prepare_dataset(sig, res)

    # Context features AND the hit_* forward labels live in the same file, so
    # attach them before labelling.
    extra = []
    if not args.no_context:
        merged, extra = attach_extra_features(merged)

    merged = apply_label(merged, args.label)

    model, features = train(merged, extra_features=extra)
    save_model(model, features)

    print("\n✅ Done.")