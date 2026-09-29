"""
ORB Trade Predictor — predict.py
---------------------------------
Loads trained model and scores new signals from backtest_opening_range.csv
Outputs: orb_signals_scored.csv with go/no-go + target probability

Usage:
    python predict.py
    python predict.py --threshold 0.35   # adjust confidence threshold
"""

import pandas as pd
import numpy as np
import pickle
import os
import argparse

MODEL_FILE    = "orb_classifier.pkl"
FEATURES_FILE = "orb_features.pkl"
SIGNALS_FILE  = "backtest_opening_range.csv"
OUTPUT_FILE   = "orb_signals_scored.csv"

# Calibrated on the 2026-01-30+ forward holdout of the replayed history
# (see train_classifier.py output). Probabilities are P(trade is profitable):
#   0.48 -> keeps top 50% of signals, +0.03% mean PnL, 48% win rate
#   0.53 -> keeps top 30%,            +0.05% mean PnL, 54% win rate
#   0.65 -> keeps top 10%,            +0.10% mean PnL, 70% win rate
# Those are BEFORE brokerage, STT and slippage — budget ~0.05-0.10% round trip.
DEFAULT_THRESHOLD = 0.53   # lower = more trades, higher = stricter filter


# ---------------------------------------------------
# FEATURE ENGINEERING (must match train_classifier.py exactly)
# ---------------------------------------------------
def engineer_features(df, features):

    df = df.copy()

    orb_range = df["orh"] - df["orl"]

    df["orb_range_pct"] = (orb_range / df["orl"]) * 100

    df["gap_pct"] = ((df["orl"] - df["prev_close"]) / df["prev_close"]) * 100

    def breakout_strength(row, orb_range_val):
        if orb_range_val == 0:
            return 0.0
        if row["direction"] == "BUY":
            return (row["entry_price"] - row["orh"]) / orb_range_val
        else:
            return (row["orl"] - row["entry_price"]) / orb_range_val

    orb_range_vals = orb_range.values
    df["breakout_strength"] = [
        breakout_strength(row, orb_range_vals[i])
        for i, (_, row) in enumerate(df.iterrows())
    ]

    orb_mid = (df["orh"] + df["orl"]) / 2
    df["entry_vs_mid"] = (df["entry_price"] - orb_mid) / orb_range.replace(0, np.nan)

    df["prev_close_vs_orb"] = (df["prev_close"] - df["orl"]) / orb_range.replace(0, np.nan)

    def time_to_minutes(t):
        try:
            t = str(t).split(".")[0]
            parts = t.split(":")
            h, m = int(parts[0]), int(parts[1])
            return max(0, (h * 60 + m) - (9 * 60 + 15))
        except Exception:
            return 0

    df["signal_minutes"] = df["time"].apply(time_to_minutes)
    df["direction_enc"]  = (df["direction"].str.upper() == "BUY").astype(int)
    df["orb_range_abs"]  = orb_range
    df["entry_log"]      = np.log(df["entry_price"])

    return df[features].fillna(0)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def main(threshold=DEFAULT_THRESHOLD):

    # Load model
    if not os.path.exists(MODEL_FILE):
        print(f"❌ Model not found: {MODEL_FILE}")
        print("   Run train_classifier.py first.")
        return

    with open(MODEL_FILE, "rb") as f:
        model = pickle.load(f)

    with open(FEATURES_FILE, "rb") as f:
        features = pickle.load(f)

    # Load signals
    if not os.path.exists(SIGNALS_FILE):
        print(f"❌ Signals file not found: {SIGNALS_FILE}")
        return

    df = pd.read_csv(SIGNALS_FILE)
    df.columns = [c.strip().lower() for c in df.columns]

    print(f"\n📥 Loaded {len(df)} signals from {SIGNALS_FILE}")

    # Engineer features + predict
    X = engineer_features(df, features)
    df["target_prob"] = model.predict_proba(X)[:, 1]
    df["go"] = (df["target_prob"] >= threshold).map({True: "✅ GO", False: "❌ SKIP"})

    # Sort by probability descending
    df = df.sort_values("target_prob", ascending=False).reset_index(drop=True)

    # Print summary
    go_count   = (df["target_prob"] >= threshold).sum()
    skip_count = len(df) - go_count

    print(f"\n🎯 Threshold: {threshold:.2f}")
    print(f"✅ GO:   {go_count} trades")
    print(f"❌ SKIP: {skip_count} trades")
    print(f"\n{'Symbol':<15} {'Dir':<6} {'Prob':>6}  {'Signal'}")
    print("-" * 45)

    for _, row in df.iterrows():
        print(
            f"{str(row['symbol']):<15} "
            f"{str(row['direction']):<6} "
            f"{row['target_prob']:>5.1%}  "
            f"{row['go']}"
        )

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Scored signals saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD,
        help=f"Min probability to take a trade (default: {DEFAULT_THRESHOLD})"
    )
    args = parser.parse_args()
    main(threshold=args.threshold)