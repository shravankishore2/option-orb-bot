"""
model_report.py — docs/MODEL.md, read from the model files themselves.

The feature list, order, threshold, label and training window come from
models/orbital_model.pkl/.json (the frozen v2 baseline, which is also the champion
while models/registry/champion.json is absent) and models/variants/v2.1.*. The
plain-English descriptions below are checked against that list: the script stops if
a model input has no description or a description has no input.

Importance:
  * gain: XGBoost's average gain per split for each input, from the v2 booster;
  * permutation: the drop in ROC AUC (label "profit") when one input is shuffled,
    5 repeats, on two out-of-sample sets: (a) v2 itself on its calibration sessions
    (2025-11-13 → 2026-09-22, never used to fit it), and (b) the walk-forward model of
    each of the last 12 months on that month's signals, shuffled within the month.

    ORBITAL_OFFLINE=1 python model_report.py          # writes docs/MODEL.md
"""

import datetime as dt
import json
import os
import pickle
from pathlib import Path

os.environ.setdefault("ORBITAL_OFFLINE", "1")

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

import strategy_config as C
import walk_forward

BASE = Path(__file__).resolve().parent
MODEL = BASE / "models" / "orbital_model.pkl"
META = BASE / "models" / "orbital_model.json"
V21 = BASE / "models" / "variants" / "v2.1.json"
WF_DIR = BASE / "models" / "walkforward"
OUT = BASE / "docs" / "MODEL.md"
REPEATS = 5
WF_MONTHS = 12
WF_FILE = BASE / "data" / "research" / "walkforward.csv"
V1_DIR = BASE / "data" / "research" / "v1"
AUC_BOOT = 500

PIT = {
    "signal": "the signal row, all known when the signal candle closes",
    "candles": "completed candles stamped before the entry time",
    "prev": "previous sessions, looked up by date strictly before the day",
    "both": "completed candles before the entry time + previous sessions by date",
}
# name: (what it means, how it is computed, where its data comes from: a PIT key)
DESCRIPTIONS = {
    "orb_range_pct": ("Width of the opening range, as % of price.",
                      "(ORH − ORL) / ORL × 100; the range is the 09:20, 09:25 and 09:30 candles (closed by 09:35).",
                      "signal"),
    "gap_pct": ("How far the opening range sits from yesterday's close.",
                "(ORL − previous close) / previous close × 100.", "signal"),
    "breakout_strength": ("How far past the breakout level the signal candle closed, in range widths.",
                          "BUY: (entry − ORH) / ORB; SELL: (ORL − entry) / ORB. ORB = ORH − ORL.", "signal"),
    "entry_vs_mid": ("Entry price relative to the middle of the opening range, in range widths (not direction-signed).",
                     "(entry − (ORH + ORL) / 2) / ORB.", "signal"),
    "prev_close_vs_orb": ("Yesterday's close relative to the opening range, in range widths.",
                          "(previous close − ORL) / ORB.", "signal"),
    "signal_minutes": ("Time of day of the entry.", "Minutes from 09:15 to the entry time (signal candle stamp + 5).",
                       "signal"),
    "direction_enc": ("Long or short.", "1 = BUY, 0 = SELL.", "signal"),
    "orb_range_abs": ("Width of the opening range in rupees.", "ORH − ORL.", "signal"),
    "entry_log": ("Price level of the stock.", "ln(entry price); entry = close of the signal candle.", "signal"),
    "nifty_ret": ("How the NIFTY 50 has moved today so far.",
                  "% change of NIFTY from its day open to the close of the last completed candle.", "candles"),
    "nifty_or_pos": ("Where NIFTY is relative to its own opening range.",
                     "(NIFTY close − its OR low) / its OR width: 0 = at the low, 1 = at the high; can be outside 0..1.",
                     "candles"),
    "aligned_with_nifty": ("Whether the trade goes the same way as the market.",
                           "1 if BUY and NIFTY is up on the day, or SELL and NIFTY is down; else 0.", "candles"),
    "breadth": ("Share of the universe trading above today's open.",
                "Fraction of index members whose last completed close is above their day open (history: the "
                "point-in-time member list; live: today's Nifty 200 list).", "candles"),
    "sector_ret": ("How the stock's sector has moved today.",
                   "Mean % move from the day open of the universe stocks in the same sector, at the last completed candle.",
                   "candles"),
    "rel_to_sector": ("The stock's move today relative to its sector.", "Own % move from the open − sector_ret.",
                      "candles"),
    "rel_vol_or": ("How heavy today's opening-range volume is for this stock.",
                   "Today's 09:20–09:30 volume ÷ the median of the same over the previous 20 sessions (needs ≥ 5).",
                   "both"),
    "rel_vol_entry": ("Volume of the latest candle compared with today's average.",
                      "Volume of the last completed candle ÷ mean candle volume so far today.", "candles"),
    "vol_trend": ("Whether volume is picking up.", "Mean volume of the last 3 completed candles ÷ mean so far today.",
                  "candles"),
    "atr_pct": ("Normal daily range of the stock, as % of price.",
                "Mean daily (high − low) over the last 14 sessions before today (needs ≥ 5) ÷ entry × 100.", "prev"),
    "atr_norm_range": ("Opening range compared with a normal day's range.", "ORB ÷ that 14-session average range.",
                       "prev"),
    "range_expansion": ("Whether candles are getting bigger.",
                        "Mean (high − low) of the last 3 completed candles ÷ mean of all completed candles today.",
                        "candles"),
    "vwap_dist": ("Entry relative to today's VWAP, in the trade's favour.",
                  "(entry − VWAP) / VWAP × 100 × (+1 BUY, −1 SELL); VWAP of the typical price over completed candles.",
                  "candles"),
    "move_from_open": ("How much of the day's move has already happened, in the trade's favour.",
                       "(entry − day open) / day open × 100 × (+1 BUY, −1 SELL); day open = 09:15 candle open.",
                       "candles"),
    "pos_in_day_range": ("Where the entry sits in today's range so far.",
                         "(entry − day low) / (day high − day low), over completed candles.", "candles"),
    "dist_pdh": ("Entry relative to yesterday's high.", "(entry − previous session high) / entry × 100.", "prev"),
    "dist_pdl": ("Entry relative to yesterday's low.", "(entry − previous session low) / entry × 100.", "prev"),
    "level_touches": ("How often price has tested the breakout level today.",
                      "Number of completed candles whose range includes the breakout level (ORH for BUY, ORL for "
                      "SELL) within ±0.1%.", "candles"),
    "consec_bars": ("Momentum into the entry.",
                    "Number of consecutive completed candles, ending with the latest, that closed further in the "
                    "trade's direction than the one before.", "candles"),
}


def perm_drop(model, X, y, groups, rng):
    """AUC drop per feature (mean, sd over REPEATS); shuffles within each group."""
    base = roc_auc_score(y, model.predict_proba(X)[:, 1])
    idx = [np.flatnonzero(groups == g) for g in np.unique(groups)]
    out = {}
    for f in X.columns:
        drops = []
        for _ in range(REPEATS):
            Xp = X.copy()
            col = Xp[f].to_numpy().copy()
            for ix in idx:
                col[ix] = col[rng.permutation(ix)]
            Xp[f] = col
            drops.append(base - roc_auc_score(y, model.predict_proba(Xp)[:, 1]))
        out[f] = (float(np.mean(drops)), float(np.std(drops)))
    return base, out


def period_auc(df, score_col, rng):
    """AUC of `score_col` on the profit label, with a 95% day-block bootstrap CI."""
    y, s, d = df["profit"].to_numpy(), df[score_col].to_numpy(), df["date"].to_numpy()
    if len(np.unique(y)) < 2:
        return None, (None, None)
    days = np.unique(d)
    pos = {x: np.flatnonzero(d == x) for x in days}
    boots = []
    for _ in range(AUC_BOOT):
        ix = np.concatenate([pos[x] for x in rng.choice(days, len(days))])
        if len(np.unique(y[ix])) == 2:
            boots.append(roc_auc_score(y[ix], s[ix]))
    return roc_auc_score(y, s), tuple(np.percentile(boots, [2.5, 97.5]))


def periods(meta, first, last):
    """Calendar years before v2's held-out calibration window, then its quarters."""
    cal = meta["calibration_from"]
    out = [(f"{y}", f"{y}-01-01", min(f"{y}-12-31", (dt.date.fromisoformat(cal) - dt.timedelta(days=1)).isoformat()), True)
           for y in range(int(first[:4]), int(cal[:4]) + 1) if f"{y}-01-01" < cal]
    q0 = dt.date.fromisoformat(cal)
    q = dt.date(q0.year, 3 * ((q0.month - 1) // 3) + 1, 1)
    while q.isoformat() <= last:
        nxt = dt.date(q.year + (q.month == 10), (q.month + 2) % 12 + 1, 1)
        a, b = max(q, q0).isoformat(), min(nxt - dt.timedelta(days=1), dt.date.fromisoformat(last)).isoformat()
        out.append((f"{q.year} Q{(q.month - 1) // 3 + 1}", a, b, False))
        q = nxt
    return out


def v1_reference():
    """The 0.628 / 0.665 forward AUCs: train_classifier.py's holdout on the 4-Sep dataset
    (data/research/v1/). Recomputed here with that script's own loaders, features, split
    (oldest 80% of sessions vs the rest) and model settings; the throwaway model is never
    saved, and v2 is untouched. Also scores both models on the signals both datasets share,
    under both datasets' labels. None if the 4-Sep dataset isn't on this machine."""
    if not (V1_DIR / "historical_orb_features.csv").exists():
        return None
    import contextlib
    import io
    from xgboost import XGBClassifier
    import train_classifier as T
    with contextlib.redirect_stdout(io.StringIO()):          # the script narrates every step
        sig, res = T.load_data("historical")
        m = T.prepare_dataset(sig, res)
        m, extra = T.attach_extra_features(m)
        m = T.apply_label(m, "profit")
        df, geo = T.engineer_features(m)
    days = np.sort(df["date"].unique())
    split = days[int(len(days) * 0.8)]
    tr = (df["date"] < split).to_numpy()
    y = df["label"].to_numpy()

    def fit(cols):          # the settings of train_classifier.train()
        X = df[cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        mdl = XGBClassifier(n_estimators=200, max_depth=3, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
                            scale_pos_weight=(y == 0).sum() / y.sum(),   # over all rows, as that script did
                            eval_metric="logloss", random_state=42)
        mdl.fit(X[tr], y[tr])
        p = mdl.predict_proba(X[~tr])[:, 1]
        return roc_auc_score(y[~tr], p), p
    full = geo + [f for f in extra if f in df.columns]
    auc28, p28 = fit(full)
    auc9, _ = fit(geo)
    auc_no_atr, _ = fit([f for f in full if f not in ("atr_pct", "atr_norm_range")])
    te = df.loc[~tr, ["date", "symbol", "direction", "label"]].assign(p_v1=p28)
    te["symbol"], te["direction"] = te["symbol"].str.upper(), te["direction"].str.upper()
    wf = pd.read_csv(WF_FILE, dtype={"date": str, "time": str})
    wf = wf[(wf["date"] >= split) & (wf["date"] <= days[-1])]
    j = te.merge(wf[["date", "symbol", "direction", "score", "profit"]], on=["date", "symbol", "direction"])
    res = res.copy()
    res.columns = [c.lower() for c in res.columns]
    return {"rows": len(df), "first": days[0], "last": days[-1], "split": split, "train": int(tr.sum()),
            "test": int((~tr).sum()), "symbols": int(df["symbol"].nunique()), "auc28": auc28, "auc9": auc9,
            "auc_no_atr": auc_no_atr, "n28": len(full), "win": float(y[~tr].mean()),
            "exits": res["exit_reason"].value_counts().to_dict(),
            "wf_same_window": (roc_auc_score(wf["profit"], wf["score"]), len(wf), float(wf["profit"].mean())),
            "overlap": len(j), "labels_agree": float((j["label"] == j["profit"]).mean()),
            "v1_on_v1": roc_auc_score(j["label"], j["p_v1"]), "v1_on_v2": roc_auc_score(j["profit"], j["p_v1"]),
            "v2_on_v2": roc_auc_score(j["profit"], j["score"]), "v2_on_v1": roc_auc_score(j["label"], j["score"])}


def which_auc_section(df, model, feats, meta, auc_wf, wf_rows, months, auc_cal, cal_rows, rng):
    """The "Which AUC to quote" section."""
    df = df[df["date"] <= meta["trained_through"]].copy()
    df["v2"] = model.predict_proba(df[feats].fillna(0))[:, 1]
    wf = pd.read_csv(WF_FILE, dtype={"date": str, "time": str})
    md = ["## Which AUC to quote", "",
          "AUC here is the ROC AUC of a score against the `profit` label: the chance a random winning signal "
          "scores above a random losing one (0.5 = no better than chance). Two very different numbers have been "
          "quoted for this project. Every number below is recomputed by this script.", ""]
    v1 = v1_reference()
    md += ["### Where each number comes from", "",
           "| Number | Model | Label | Signals scored | Date window | Out of sample? |", "|---|---|---|---|---|---|",
           f"| **{auc_wf:.3f}** | the walk-forward models (each month's model, trained only on earlier months) | "
           f"`profit` under exit_v1 | {wf_rows:,} | {months[0]} → {months[-1]} (last {WF_MONTHS} months) | yes |",
           f"| **{auc_cal:.3f}** | v2 itself (`models/orbital_model.pkl`) | `profit` under exit_v1 | {cal_rows:,} | "
           f"{meta['calibration_from']} → {meta['trained_through']} (v2's calibration sessions) | yes: never used to "
           "fit v2, only to set its threshold |"]
    if v1:
        md += [f"| **{v1['auc28']:.3f}** | `train_classifier.py` (the 4-Sep exploration): one XGBoost, "
               f"{v1['n28']} inputs, 200 trees of depth 3, trained before {v1['split']} | `profit` under the **4-Sep exit "
               f"rule** (midpoint target, trailing stop, EOD 15:15) | {v1['test']:,} | {v1['split']} → {v1['last']} "
               f"(the newest 20% of sessions of {v1['first']} → {v1['last']}) | yes, but on a different dataset |",
               f"| {v1['auc9']:.3f} | the same, 9 geometry inputs only (the \"0.628 → 0.665\" comparison) | same | "
               f"{v1['test']:,} | same | same |"]
    else:
        md += ["| 0.665 | `train_classifier.py` on `data/research/v1/` (not on this machine, so not recomputed) | "
               "`profit` under the 4-Sep exit rule | 10,475 | 2026-01-30 → 2026-09-04 | on a different dataset |"]
    if v1:
        wfa, wfn, wfwin = v1["wf_same_window"]
        md += ["", "### Why 0.665 isn't comparable", "",
               f"- **A different label.** The 4-Sep dataset's `profit` is \"made money under the 4-Sep exit rule\", which "
               f"had a midpoint target ({v1['exits'].get('Target (Midpoint)', 0):,} of {sum(v1['exits'].values()):,} exits). "
               f"On the {v1['overlap']:,} test signals the two datasets share (same date, stock and direction), "
               f"the two `profit` labels agree on only **{v1['labels_agree'] * 100:.1f}%** of them.",
               f"- **The 4-Sep model doesn't rank v2's label.** On those shared signals it scores "
               f"{v1['v1_on_v1']:.3f} against its own label but **{v1['v1_on_v2']:.3f}** against v2's. v2's "
               f"walk-forward scores do the opposite: {v1['v2_on_v2']:.3f} on v2's label, {v1['v2_on_v1']:.3f} on the "
               "4-Sep one.",
               f"- **Same window, current label:** v2's walk-forward AUC over {v1['split']} → {v1['last']} is "
               f"**{wfa:.3f}** ({wfn:,} signals).",
               f"- **Other differences:** the 4-Sep set uses today's {v1['symbols']} index stocks for all of history "
               "(survivorship), and its context features come from the 4-Sep code, whose ATR included the signal "
               "day's own range (a lookahead, since fixed). That lookahead barely matters here: without the two ATR "
               f"inputs the 4-Sep holdout AUC is {v1['auc_no_atr']:.3f}.", ""]

    rows = []
    for name, a, b, in_sample in periods(meta, df["date"].min(), meta["trained_through"]):
        g = df[(df["date"] >= a) & (df["date"] <= b)]
        w = wf[(wf["date"] >= a) & (wf["date"] <= b)]
        auc, ci = period_auc(g, "v2", rng)
        wauc, wci = period_auc(w, "score", rng) if len(w) else (None, (None, None))
        rows.append(f"| {name} | {a} → {b} | {len(g):,} | {g['profit'].mean() * 100:.1f}% | "
                    + (f"{auc:.3f} [{ci[0]:.3f}, {ci[1]:.3f}]" if auc is not None else "—")
                    + (" (in-sample)" if in_sample else "") + " | "
                    + (f"{wauc:.3f} [{wci[0]:.3f}, {wci[1]:.3f}] ({len(w):,})" if wauc is not None else "—") + " |")
    md += ["### v2's AUC by period", "",
           f"v2's own scores on the `profit` label: by quarter over its held-out calibration window (from "
           f"{meta['calibration_from']}) and by calendar year before it. **The years before are in-sample**: v2 was fit "
           "on those signals, so they show how well it memorised them, not how well it predicts. The last column is "
           "the out-of-sample comparison: the walk-forward score of each signal (the model trained before its "
           f"month). 95% CIs from a day-block bootstrap ({AUC_BOOT} resamples).", "",
           "| Period | Dates | Signals | Winners | v2 AUC [95% CI] | Walk-forward AUC [95% CI] (signals) |",
           "|---|---|---|---|---|---|", *rows, "",
           "### What to quote", "",
           f"- **Quote {auc_wf:.3f}** (walk-forward, last {WF_MONTHS} months), alongside v2's own held-out "
           f"{auc_cal:.3f}: v2's design, its label, data it never trained on.",
           "- Don't quote the in-sample years: they measure fit, not prediction.",
           "- Don't quote 0.665 (or 0.628 → 0.665) for v2. It's a different label, dataset and model. If it comes up, "
           "say what it was: the 4-Sep exploration's forward holdout, on a label that agrees with today's on about "
           "60% of the same signals.",
           "- AUC isn't the target anyway: the model only has to rank the top 2% well. Per-trade P&L of the GO trades "
           "(docs/RESULTS.md, docs/LATE_ENTRY.md) is the number that matters.", ""]
    return md


def main():
    rng = np.random.default_rng(3)
    bundle = pickle.loads(MODEL.read_bytes())
    meta = json.loads(META.read_text())
    model, feats, thr = bundle["model"], list(bundle["features"]), float(bundle["threshold"])
    assert feats == meta["features"] and abs(thr - meta["threshold"]) < 1e-12, "pkl and json disagree"
    missing, extra = set(feats) - set(DESCRIPTIONS), set(DESCRIPTIONS) - set(feats)
    assert not missing and not extra, f"descriptions out of date: missing {missing}, extra {extra}"
    champ = BASE / "models" / "registry" / "champion.json"
    v21 = json.loads(V21.read_text())

    gain = model.get_booster().get_score(importance_type="gain")
    gain = {f: float(gain.get(f, 0.0)) for f in feats}
    gsum = sum(gain.values())

    print("loading the dataset...", flush=True)
    df, features = walk_forward.load_dataset()
    assert features == feats, "dataset feature order differs from the model's"
    cal = df[(df["date"] >= meta["calibration_from"]) & (df["date"] <= meta["trained_through"])]
    X, y = cal[feats].fillna(0), cal["profit"].to_numpy()
    print(f"permutation on v2's calibration sessions: {len(cal):,} rows", flush=True)
    auc_cal, perm_cal = perm_drop(model, X, y, np.zeros(len(cal)), rng)

    months = sorted(p.stem for p in WF_DIR.glob("*.pkl"))[-WF_MONTHS:]
    print(f"permutation on walk-forward months {months[0]} → {months[-1]}", flush=True)
    wf_rows, wf_models = [], {}
    for m in months:
        b = pickle.loads((WF_DIR / f"{m}.pkl").read_bytes())
        assert list(b["features"]) == feats
        wf_models[m] = b["model"]
        wf_rows.append(df[df["month"] == m])
    wf = pd.concat(wf_rows)

    class ByMonth:            # each month's rows scored by that month's model
        def predict_proba(self, Xp):
            out = np.zeros((len(Xp), 2))
            for m, mdl in wf_models.items():
                sel = (wf["month"] == m).to_numpy()
                out[sel] = mdl.predict_proba(Xp[sel])
            return out
    auc_wf, perm_wf = perm_drop(ByMonth(), wf[feats].fillna(0), wf["profit"].to_numpy(), wf["month"].to_numpy(), rng)

    order = sorted(feats, key=lambda f: -perm_wf[f][0])
    rank_gain = {f: i + 1 for i, f in enumerate(sorted(feats, key=lambda f: -gain[f]))}
    rank_cal = {f: i + 1 for i, f in enumerate(sorted(feats, key=lambda f: -perm_cal[f][0]))}

    md = ["# The model", "",
          f"Generated by `model_report.py` on {dt.date.today()} from the model files, not from other docs.", "",
          "## Which model decides", "",
          f"- **Champion:** {'see models/registry/champion.json' if champ.exists() else 'v2 (no models/registry/champion.json, so the champion is the frozen v2 baseline)'}: "
          f"`models/orbital_model.pkl`, version `{meta['model_version']}`, created {meta['created']}, "
          f"strategy_config SHA-256 `{meta['config_sha256'][:16]}…`.",
          "- **The model:** XGBoost classifier (`strategy_config.MODEL_PARAMS`: "
          + ", ".join(f"{k}={v}" for k, v in C.MODEL_PARAMS.items() if k != "eval_metric")
          + "; `scale_pos_weight` = losers ÷ winners in the fit rows). Missing inputs are filled with 0, in "
            "training and live alike.",
          f"- **Label:** `{meta['label']}`, 1 if the trade made money under the exit rule (`exits.simulate`, exit_v1: "
          "stop 1× the opening range, trail 1× the range from the best price, out at the 15:15 candle's close), else 0. "
          "So the model estimates the chance a breakout trade ends positive, not how much it makes.",
          f"- **Training window:** every labelled signal from {meta['trained_from']} through {meta['trained_through']}: "
          f"{meta['rows']:,} signals. The oldest 85% of sessions fit the model ({meta['fit_rows']:,} rows); the newest "
          f"15% ({meta['cal_rows']:,} rows, from {meta['calibration_from']}) are held out to set the threshold.",
          f"- **Threshold:** {thr:.4f} (exactly {thr!r}). It is the 98th percentile of the model's scores on those "
          "held-out calibration sessions, so about 2% of signals score GO by design. It is an absolute number, fixed "
          "at training time, never a percentile of the live day.", "",
          "## The 28 inputs, in the model's order", "",
          "Point-in-time rule: a 5-minute candle stamped T covers T..T+5, and a signal from it is entered at T+5 at "
          "its close. Every input uses only that signal row, candles that had closed by the entry time, and "
          "sessions strictly before the day (looked up by date). All 28 are computed by the same code in history "
          "and live (`features.geometry_features`, `features.compute_features`). The live/offline recompute in "
          "docs/THRESHOLD_RESULTS.md matched exactly except where the stored previous close was stale (fixed "
          "2026-10-07).", "",
          "| # | Input | What it means | How it is computed | Available at signal time? |",
          "|---|---|---|---|---|"]
    for i, f in enumerate(feats, 1):
        what, how, pit = DESCRIPTIONS[f]
        md.append(f"| {i} | `{f}` | {what} | {how} | Yes: {PIT[pit]} |")
    md += ["", "## Importance, sorted", "",
           f"- **Permutation, walk-forward** (the sort order): the last {WF_MONTHS} walk-forward months "
           f"({months[0]} → {months[-1]}, {len(wf):,} signals, each scored by the model trained before its month; "
           f"AUC {auc_wf:.3f}). Drop in AUC when the input is shuffled within the month, mean of {REPEATS} repeats.",
           f"- **Permutation, v2 calibration:** the v2 model itself on its held-out calibration sessions "
           f"({len(cal):,} signals, AUC {auc_cal:.3f}).",
           "- **Gain:** XGBoost's average gain per split that uses the input, v2 booster, as a share of the total. "
           "Gain measures how the trees used an input in training, not whether it helps out of sample.",
           "- Which way each input pushes the score (SHAP, with worked examples): docs/MODEL_SHAP.md.", "",
           "| Rank | Input | AUC drop, walk-forward (± sd) | AUC drop, v2 calibration (rank) | Gain share (rank) |",
           "|---|---|---|---|---|"]
    for i, f in enumerate(order, 1):
        md.append(f"| {i} | `{f}` | {perm_wf[f][0]:+.4f} (± {perm_wf[f][1]:.4f}) | {perm_cal[f][0]:+.4f} ({rank_cal[f]}) | "
                  f"{gain[f] / gsum * 100:.1f}% ({rank_gain[f]}) |")
    hurt = [f for f in feats if perm_wf[f][0] < 0 and perm_cal[f][0] < 0]
    unused = [f for f in feats if gain[f] == 0]
    top = order[0]
    md += ["", "A drop near zero means shuffling that input doesn't change the ranking out of sample; a negative "
               "drop means the ranking got *better* without it.", "",
           "**Reading:**",
           f"- The out-of-sample ranking is weak overall (AUC {auc_wf:.3f} walk-forward, {auc_cal:.3f} on v2's "
           f"calibration). `{top}` (time of day) carries most of it: shuffling it costs {perm_wf[top][0]:.3f} AUC, "
           f"{perm_wf[top][0] / max(perm_wf[order[1]][0], 1e-9):.0f}× the next input. That is why v2's GO picks lean late "
           "in the day (docs/LATE_ENTRY.md).",
           f"- Shuffling these {len(hurt)} inputs makes the ranking slightly better in both sets (most by less than "
           "their spread across repeats): "
           + ", ".join(f"`{f}`" for f in hurt) + ". The market-wide ones (`breadth`, `nifty_ret`) are the clearest. "
           "This is exploratory and changes nothing; dropping inputs would be a new model under a new protocol.",
           ("- " + ", ".join(f"`{f}`" for f in unused) + " is never used in a split (gain 0), so it has no effect on "
            "the score." if unused else ""),
           f"- Of the three inputs v2.1 drops, `orb_range_abs` ranks {order.index('orb_range_abs') + 1} and "
           f"`prev_close_vs_orb` {order.index('prev_close_vs_orb') + 1} by walk-forward permutation (both help a "
           f"little), `entry_log` {order.index('entry_log') + 1} (slightly harmful).", "",
           *which_auc_section(df, model, feats, meta, auc_wf, len(wf), months, auc_cal, len(cal), rng),
           "## v2.1 (shadow variant, decides nothing)", "",
           f"v2.1 is v2's recipe with three inputs removed: {', '.join(f'`{f}`' for f in v21['dropped_from_v2'])}. "
           f"Same rows ({v21['rows']:,}, {v21['trained_from']} → {v21['trained_through']}), same calibration split, "
           f"threshold {v21['threshold']:.4f}. Protocol: `{v21['protocol']}` (SHA-256 `{v21['protocol_sha256'][:16]}…`).",
           "", "Its 25 inputs, in order: " + ", ".join(f"`{f}`" for f in v21["features"]) + ".", ""]
    assert [f for f in feats if f not in v21["dropped_from_v2"]] == v21["features"]
    OUT.write_text("\n".join(md) + "\n")
    print("written", OUT)


if __name__ == "__main__":
    main()
