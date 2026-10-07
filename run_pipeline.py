"""
run_pipeline.py — rebuild every result from scratch, in order.

    python run_pipeline.py                 # everything (~2 h first time, mostly downloads)
    python run_pipeline.py --from signals  # resume from a step
    python run_pipeline.py --only report   # one step

Steps
  daily      daily candles for every NSE equity (survivorship ranking input)
  universe   point-in-time Nifty 200 membership + validation vs the real list
  intraday   5-min candles for every stock that was ever a member, + NIFTY 50
  signals    replay the ORBITAL rules and the plain-ORB baseline
  features   28 model inputs + forward labels for every signal
  walkforward  monthly retrain-and-score, 2021 → today (live label, v2)
  walkforward-v1  the same for the pre-registered v1 label (comparison only)
  live-model   the model the live bot uses (trained on everything)
  report     baselines, statistics, clean-test tables → docs/RESULTS.md
  paper      the live bot replayed cycle by cycle over the forward clean period
  explain    SHAP figures and docs/MODEL_SHAP.md
  model      the model's inputs and importances → docs/MODEL.md
"""

import argparse
import datetime as dt
import os
import subprocess
import sys
import time

import strategy_config as C

PY = sys.executable
TODAY = dt.date.today().isoformat()
START = C.HISTORY_START.isoformat()

STEPS = [
    ("daily", [PY, "survivorship.py", "fetch"]),
    ("universe", [PY, "survivorship.py", "build"]),
    ("intraday", [PY, "survivorship.py", "intraday"]),
    ("signals", [PY, "build_historical_signals.py", "--rules", "orbital", "--universe", "pit",
                 "--start", START, "--end", TODAY, "--out", "data/research/signals_orbital.csv"]),
    ("signals-baseline", [PY, "build_historical_signals.py", "--rules", "plain_orb", "--universe", "pit",
                          "--start", START, "--end", TODAY, "--out", "data/research/signals_plain_orb.csv"]),
    ("features", [PY, "mine_features.py", "--universe", "pit",
                  "--signals", "data/research/signals_orbital.csv",
                  "--out", "data/research/features_orbital.csv"]),
    ("walkforward", [PY, "walk_forward.py"]),
    ("walkforward-v1", [PY, "walk_forward.py", "--label", "hit_1_5r"]),
    ("live-model", [PY, "walk_forward.py", "--train-live"]),
    ("report", [PY, "report.py"]),
    ("paper", [PY, "paper_trade.py", "--start", C.CLEAN_FORWARD[0].isoformat(),
               "--end", C.CLEAN_FORWARD[1].isoformat()]),
    ("explain", [PY, "explain_model.py"]),
    ("model", [PY, "model_report.py"]),
]


def main():
    names = [n for n, _ in STEPS]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", choices=names)
    ap.add_argument("--only", choices=names)
    a = ap.parse_args()

    todo = STEPS
    if a.only:
        todo = [s for s in STEPS if s[0] == a.only]
    elif a.start:
        todo = STEPS[names.index(a.start):]

    download_steps = {"daily", "universe", "intraday"}
    for name, cmd in todo:
        print(f"\n{'=' * 70}\n▶ {name}: {' '.join(cmd[1:])}\n{'=' * 70}", flush=True)
        env = dict(os.environ)
        if name not in download_steps:
            env["ORBITAL_OFFLINE"] = "1"      # research steps read the cache only
        t0 = time.time()
        rc = subprocess.call(cmd, env=env)
        print(f"◀ {name} finished in {(time.time() - t0) / 60:.1f} min (exit {rc})", flush=True)
        if rc != 0:
            print(f"❌ stopped at '{name}'. Fix it, then: python run_pipeline.py --from {name}")
            return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
