"""
Explain the model — explain_model.py
------------------------------------
SHAP values split each prediction into one contribution per input: how much
that input pushed this signal's score up or down from the average. XGBoost
computes them exactly for tree models (pred_contribs=True), so no extra
library is needed.

Writes:
  static/figures/shap_global.png     which inputs matter most (mean |SHAP|)
  static/figures/shap_beeswarm.png   and in which direction
  docs/MODEL.md                      the same, in words, with worked examples

    python explain_model.py
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xgboost as xgb

from live_engine import load_bundle
from walk_forward import load_dataset

BASE_DIR = Path(__file__).resolve().parent
FIG = BASE_DIR / "static" / "figures"

DESCRIPTIONS = {
    "orb_range_pct": "opening-range width, % of price",
    "gap_pct": "overnight gap into the range",
    "breakout_strength": "how far past the range the entry is (in ranges)",
    "entry_vs_mid": "entry vs the range's midpoint (in ranges)",
    "prev_close_vs_orb": "yesterday's close vs the range",
    "signal_minutes": "minutes since 09:15",
    "direction_enc": "BUY (1) or SELL (0)",
    "orb_range_abs": "range width in rupees",
    "entry_log": "price level (log)",
    "nifty_ret": "NIFTY 50 move since the open",
    "nifty_or_pos": "NIFTY vs its own opening range",
    "aligned_with_nifty": "trade in the index's direction",
    "breadth": "share of the index above its open",
    "sector_ret": "sector's move since the open",
    "rel_to_sector": "stock vs its sector",
    "rel_vol_or": "opening-range volume vs its 20-day norm",
    "rel_vol_entry": "breakout candle volume vs the day so far",
    "vol_trend": "last 3 candles' volume vs the day so far",
    "atr_pct": "daily volatility (ATR), % of price",
    "atr_norm_range": "opening range in daily ATRs",
    "range_expansion": "last 3 candles' size vs the day so far",
    "vwap_dist": "distance past VWAP, in the trade's direction",
    "move_from_open": "move since the open, in the trade's direction",
    "pos_in_day_range": "where price sits in the day's range",
    "dist_pdh": "distance from yesterday's high",
    "dist_pdl": "distance from yesterday's low",
    "level_touches": "times price tested the level before breaking",
    "consec_bars": "consecutive candles in the trade's direction",
}


def shap_values(model, X):
    booster = model.get_booster()
    contrib = booster.predict(xgb.DMatrix(X), pred_contribs=True)
    return contrib[:, :-1], contrib[:, -1]          # per-feature, bias


def main(sample=8000, seed=0):
    model, features, threshold, meta = load_bundle()
    df, _ = load_dataset()
    df = df.sample(min(sample, len(df)), random_state=seed).reset_index(drop=True)
    X = df[features].fillna(0)
    phi, bias = shap_values(model, X)
    scores = model.predict_proba(X)[:, 1]

    FIG.mkdir(parents=True, exist_ok=True)
    importance = pd.Series(np.abs(phi).mean(axis=0), index=features).sort_values()

    # ---- global importance
    fig, ax = plt.subplots(figsize=(8, 7))
    ax.barh([f"{f}" for f in importance.index], importance.values, color="#2f9e6e")
    ax.set_xlabel("mean |SHAP value|  (average push on the score, log-odds)")
    ax.set_title("Which inputs the model leans on")
    fig.tight_layout()
    fig.savefig(FIG / "shap_global.png", dpi=140)
    plt.close(fig)

    # ---- beeswarm: direction of effect for the top 12
    top = list(importance.index[-12:])
    fig, ax = plt.subplots(figsize=(8, 7))
    rng = np.random.default_rng(seed)
    for row, f in enumerate(top):
        j = features.index(f)
        v = X[f].to_numpy(float)
        lo, hi = np.nanpercentile(v, 5), np.nanpercentile(v, 95)
        colour = np.clip((v - lo) / (hi - lo if hi > lo else 1), 0, 1)
        ax.scatter(phi[:, j], row + rng.uniform(-0.3, 0.3, len(v)), c=colour, cmap="coolwarm",
                   s=4, alpha=0.5, linewidths=0)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(top)
    ax.axvline(0, color="#888", lw=0.8)
    ax.set_xlabel("SHAP value  (← lowers the score · raises the score →)")
    ax.set_title("Direction of effect  (red = high input value, blue = low)")
    fig.tight_layout()
    fig.savefig(FIG / "shap_beeswarm.png", dpi=140)
    plt.close(fig)

    # ---- words
    lines = ["# How the model decides", "",
             f"Model: XGBoost, trained through **{meta.get('trained_through', '?')}**, "
             f"{len(features)} inputs, GO threshold **{threshold:.3f}**. "
             f"Explained on {len(df):,} historical signals with exact tree SHAP values "
             "(`explain_model.py`).", "",
             "A SHAP value is how far one input moved one prediction away from the model's "
             "average, in log-odds. Averaging their size shows what the model relies on; "
             "comparing them with the input's value shows the direction.", "",
             "![global](../static/figures/shap_global.png)", "",
             "![direction](../static/figures/shap_beeswarm.png)", "",
             "## The ten inputs that matter most", "",
             "| # | Input | Meaning | Mean \\|SHAP\\| | Higher value → |",
             "|---|---|---|---|---|"]
    for rank, f in enumerate(reversed(importance.index[-10:]), 1):
        j = features.index(f)
        v = X[f].to_numpy(float)
        corr = np.corrcoef(v, phi[:, j])[0, 1] if np.std(v) > 0 else 0.0
        effect = "raises the score" if corr > 0.15 else "lowers the score" if corr < -0.15 else "mixed / non-linear"
        lines.append(f"| {rank} | `{f}` | {DESCRIPTIONS.get(f, '')} | {importance[f]:.3f} | {effect} |")

    lines += ["", "## Two decisions, taken apart", ""]
    go_i = int(np.argmax(scores))
    skip_i = int(np.argmin(np.abs(scores - np.median(scores))))
    for label, i in (("A GO decision", go_i), ("A typical SKIP", skip_i)):
        r = df.iloc[i]
        contrib = pd.Series(phi[i], index=features)
        order = contrib.abs().sort_values(ascending=False).index[:6]
        lines += [f"**{label}** — {r['symbol']} {r['direction']} on {r['date']} at {r['time']}, "
                  f"score {scores[i]:.3f} (threshold {threshold:.3f}).", "",
                  "| Input | Value | Push |", "|---|---|---|"]
        for f in order:
            lines.append(f"| `{f}` | {X.iloc[i][f]:.3f} | {contrib[f]:+.3f} |")
        lines.append("")

    lines += ["## Caveats", "",
              "- SHAP explains the model, not the market: a big contribution means the model "
              "relies on that input, not that the input causes returns.",
              "- Correlated inputs (e.g. `breakout_strength`, `entry_vs_mid`, `move_from_open`) "
              "share credit between them, so individual ranks among them are not stable.",
              "- This is the live model. Each walk-forward month has its own model whose "
              "emphasis can differ."]

    (BASE_DIR / "docs").mkdir(exist_ok=True)
    (BASE_DIR / "docs" / "MODEL.md").write_text("\n".join(lines))
    print("\n".join(lines[:40]))


if __name__ == "__main__":
    main()
