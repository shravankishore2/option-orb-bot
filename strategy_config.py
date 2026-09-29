"""
strategy_config.py — the ONE place every strategy parameter lives.

Registration history (docs/PREREGISTRATION.md has the hashes):

  v1  frozen 2026-09-23 01:40 IST, label "hit_1_5r".
      Clean test 2022-01 → 2023-08: FAILED — the model predicted its label
      (AUC 0.64) but that label was anti-correlated with P&L, and its picks were
      no better than random (p = 0.83). Exact file: docs/prereg/strategy_config_v1.py

  v2  frozen 2026-09-23 02:41 IST, label "profit" (made money under the exit
      rule). Chosen AFTER seeing v1 fail, so the 2022-23 period is no longer
      clean for it. Its clean test is every session from 2026-09-23 onward.
      Only LABEL, MODEL_VERSION and the forward period changed from v1.
"""

import datetime as dt

# ---------------------------------------------------------------- periods
# Development period: every design decision (label, features, exit rule,
# threshold method) was made by looking at data inside this window.
DEV_START = dt.date(2023, 9, 5)
DEV_END = dt.date(2026, 9, 4)

# Clean periods for v1 — never looked at while designing it.
CLEAN_BACKWARD = (dt.date(2022, 1, 1), dt.date(2023, 8, 31))   # older history
CLEAN_FORWARD = (dt.date(2026, 9, 5), dt.date(2026, 9, 22))    # sessions after dev

# v2's clean test: sessions that had not happened when v2 was frozen.
V2_FORWARD_START = dt.date(2026, 9, 23)

# History used for training before the backward clean period starts.
HISTORY_START = dt.date(2021, 1, 1)

# ---------------------------------------------------------------- universe
# Point-in-time Nifty 200 proxy (see survivorship.py).
UNIVERSE = "nifty200_point_in_time"

# ---------------------------------------------------------------- signal rules
CANDLE_MINUTES = 5
OR_START = dt.time(9, 20)          # opening range: candles stamped 09:20..09:30
OR_END = dt.time(9, 35)            # exclusive; the 09:30 candle closes at 09:35
OR_REQUIRED_CANDLES = 3            # a range built from fewer candles is rejected

BREAKOUT_BUFFER = 0.001            # close >= ORH * 1.001 / <= ORL * 0.999
PREV_CLOSE_MOVE = 0.018            # close >= prev * 1.018 / <= prev * 0.982
PIVOT_FIB = 0.382                  # R1 = P + 0.382*(H-L), S1 = P - 0.382*(H-L)

# Entry is the close of a COMPLETED candle, observed when that candle ends.
# No new entries once the forced exit is one candle away.
LAST_ENTRY_TIME = dt.time(15, 10)

# ---------------------------------------------------------------- exit
STOP_ORB_MULT = 1.0                # initial stop = entry -/+ 1.0 x ORB range
TRAIL_ORB_MULT = 1.0               # trail = best price -/+ 1.0 x ORB range
TARGET_ORB_MULT = None             # no fixed target
FORCE_EXIT_TIME = dt.time(15, 15)  # flat by the 15:15 candle
# When one candle touches the stop and would also have moved the trail, the
# stop is assumed hit first (pessimistic).

# ---------------------------------------------------------------- model
MODEL_VERSION = "v2"
LABEL = "profit"                   # the trade made money under the exit rule below
                                   # (v1 used "hit_1_5r": +1.5 x ORB before -1 x ORB)
MODEL_PARAMS = dict(
    n_estimators=400, max_depth=4, learning_rate=0.04, subsample=0.8,
    colsample_bytree=0.8, min_child_weight=5, eval_metric="logloss",
    random_state=42,
)
RETRAIN = "monthly"                # retrain before every calendar month
CALIBRATION_FRACTION = 0.15        # last 15% of training sessions set the threshold
THRESHOLD_QUANTILE = 0.98          # GO if score >= 98th pct of calibration scores
MIN_TRAIN_SIGNALS = 8000           # don't trade a month without this much history

# ---------------------------------------------------------------- reporting
N_CONFIGS_TRIED = 200              # rough count of variants explored in development,
                                   # used by the deflated Sharpe ratio
COST_SENSITIVITY = (0.0, 0.05, 0.10)   # round-trip %, reported alongside, not applied
