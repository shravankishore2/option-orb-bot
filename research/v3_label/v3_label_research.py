"""
v3_label_research.py — is v2's filter missing big winners, and would a cost-aware or size-aware
label (plus market alignment) take more profitable trades? Historical, walk-forward only;
nothing live changes. Results: docs/V3_LABEL_RESULTS.md.

Setup (the existing walk-forward): every month from the first with >= MIN_TRAIN_SIGNALS of history
(2021-06) to 2026-09 (through 22 Sep) gets a model trained only on earlier months; the oldest 85%
of its training sessions fit it, the newest 15% (calibration) set its selection threshold. A
"top k%" threshold is the (100 − k)th percentile of the model's calibration scores, so it's chosen
before the test month, exactly how v2's 2% is set. Exit rule exit_v1; costs 0.05% / 0.10% round trip.

Candidates (each with v2's 28 inputs and with the 25 that drop entry_log, orb_range_abs and
prev_close_vs_orb): (a) classifier on P&L > 0.10%; (b) regressor on P&L; (c) the profit classifier
weighted by |P&L|. v2 at the same rates comes from its saved monthly models (models/walkforward/).

    ORBITAL_OFFLINE=1 python research/v3_label/v3_label_research.py score    # ~30-60 min, cached
    ORBITAL_OFFLINE=1 python research/v3_label/v3_label_research.py report   # tables + doc
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root: the shared modules live there

import datetime as dt
import os
import pickle
import sys
import time

os.environ.setdefault("ORBITAL_OFFLINE", "1")

import numpy as np
import pandas as pd

import strategy_config as C
import walk_forward as W

HERE = _Path(__file__).resolve().parent
OUTDIR = HERE / "output"                                   # git-ignored
SCORED = OUTDIR / "scored.pkl"
DOC = _Path("docs/V3_LABEL_RESULTS.md")
RATES = (2, 5, 10, 20)
COSTS = (0.05, 0.10)
DROP25 = ("entry_log", "orb_range_abs", "prev_close_vs_orb")
LABELS = {"a": "P&L > 0.10% (classifier)", "b": "P&L (regression)", "c": "profit, weighted by |P&L|"}
EXCL = "2024-06"
BOOT = 2000
SEED = 7


# ------------------------------------------------------------------ scoring (slow, cached)
def _split(train):
    sessions = np.sort(train["date"].unique())
    cut = sessions[int(len(sessions) * (1 - C.CALIBRATION_FRACTION))]
    return train[train["date"] < cut], train[train["date"] >= cut]


def _model(kind, fit, feats):
    """A scoring function for one candidate (see _estimator)."""
    est = _estimator(kind, fit, feats)
    return (lambda Z: est.predict(Z)) if kind == "b" else (lambda Z: est.predict_proba(Z)[:, 1])


def _estimator(kind, fit, feats):
    """The fitted XGBoost model of one candidate: (a) classifier on P&L > 0.10%, (b) regressor on
    P&L, (c) profit classifier weighted by |P&L|. models/variants/v3a.pkl is _estimator("a", …)."""
    from xgboost import XGBClassifier, XGBRegressor
    params = dict(C.MODEL_PARAMS)
    X = fit[feats].fillna(0)
    if kind == "b":
        params.pop("eval_metric", None)
        m = XGBRegressor(**params, objective="reg:squarederror", eval_metric="rmse")
        m.fit(X, fit["pnl_%"])
        return m
    if kind == "a":
        y = (fit["pnl_%"] > 0.10).astype(int)
        w = None
        params["scale_pos_weight"] = (y == 0).sum() / max((y == 1).sum(), 1)
    else:                                                  # "c": profit, weighted by |P&L|
        y = (fit["pnl_%"] > 0).astype(int)
        w = fit["pnl_%"].abs().to_numpy() + 1e-6
        params["scale_pos_weight"] = w[y.to_numpy() == 0].sum() / max(w[y.to_numpy() == 1].sum(), 1e-9)
    m = XGBClassifier(**params)
    m.fit(X, y, sample_weight=w)
    return m


def score():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    df, feats = W.load_dataset()
    f25 = [f for f in feats if f not in DROP25]
    months = sorted(p.stem for p in _Path("models/walkforward").glob("*.pkl"))
    out = []
    t0 = time.time()
    for n, m in enumerate(months, 1):
        train, test = df[df["month"] < m], df[df["month"] == m].copy()
        if len(train) < C.MIN_TRAIN_SIGNALS or test.empty:
            continue
        fit, cal = _split(train)
        # v2: the saved walk-forward model of this month (no retraining)
        b = pickle.loads((_Path("models/walkforward") / f"{m}.pkl").read_bytes())
        v2 = lambda Z: b["model"].predict_proba(Z)[:, 1]
        cands = {"v2": (v2, feats)}
        for kind in LABELS:
            for fs, fl in (("28", feats), ("25", f25)):
                cands[f"{kind}{fs}"] = (_model(kind, fit, fl), fl)
        for name, (pred, fl) in cands.items():
            test[f"s_{name}"] = pred(test[fl].fillna(0))
            cs = pred(cal[fl].fillna(0))
            for k in RATES:
                test[f"t_{name}_{k}"] = float(np.quantile(cs, 1 - k / 100))
        test["v2_thr_saved"] = b["threshold"]
        out.append(test)
        print(f"  {m}  {len(test):>5,} signals  ({time.time() - t0:.0f}s)", flush=True)
    s = pd.concat(out, ignore_index=True)
    s.to_pickle(SCORED)
    print(f"scored {len(s):,} signals → {SCORED}")


# ------------------------------------------------------------------ statistics
def day_boot_diff(a, b, rng, reps=BOOT):
    """95% day-block bootstrap CI of mean(a) − mean(b); a, b = DataFrames with date, pnl_%."""
    days = np.array(sorted(set(a["date"]) | set(b["date"])))
    ix = {d: i for i, d in enumerate(days)}
    def sums(x):
        s, n = np.zeros(len(days)), np.zeros(len(days))
        np.add.at(s, x["date"].map(ix).to_numpy(), x["pnl_%"].to_numpy())
        np.add.at(n, x["date"].map(ix).to_numpy(), 1)
        return s, n
    sa, na = sums(a)
    sb, nb = sums(b)
    k = rng.integers(0, len(days), (reps, len(days)))
    d = sa[k].sum(1) / np.maximum(na[k].sum(1), 1) - sb[k].sum(1) / np.maximum(nb[k].sum(1), 1)
    return np.percentile(d, [2.5, 97.5])


def metrics(t, sessions):
    x = t["pnl_%"]
    r = {"n": len(t), "per_day": len(t) / sessions}
    if not len(t):
        return r
    r.update(mean05=x.mean() - 0.05, mean10=x.mean() - 0.10, total10=(x - 0.10).sum(), hit=(x > 0).mean() * 100)
    bym = t.groupby(t["date"].str[:7])["pnl_%"]
    r["worst_month10"] = (bym.sum() - 0.10 * bym.count()).min()
    gross = x.sum()
    r["best_month"] = bym.sum().idxmax()
    r["best_share"] = bym.sum().max() / gross * 100 if gross > 0 else np.nan
    ex = t[t["date"].str[:7] != EXCL]["pnl_%"]
    r.update(ex_mean05=ex.mean() - 0.05, ex_total10=(ex - 0.10).sum())
    return r


# ------------------------------------------------------------------ report
def fmt(v, spec="{:+.3f}%"):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else spec.format(v)


def nifty_day_direction():
    """+1 / −1: NIFTY's close vs its open, per day (from the cached NIFTY 5-min candles)."""
    from mine_features import load_nifty
    n = load_nifty(dt.date(2021, 1, 1), dt.date(2026, 9, 22))
    g = n.groupby(n.index.date)
    return {str(d): np.sign(float(x["Close"].iloc[-1]) - float(x["Open"].iloc[0])) for d, x in g}


def report():
    rng = np.random.default_rng(SEED)
    s = pd.read_pickle(SCORED)
    df, feats = W.load_dataset()
    sessions = s["date"].nunique()
    first, last = s["date"].min(), s["date"].max()
    pub = pd.read_csv("data/research/walkforward.csv", dtype={"date": str, "time": str})
    k = ["date", "symbol", "direction"]
    v2go = s[s["s_v2"] >= s["v2_thr_saved"]]
    same = v2go[k].merge(pub[pub["go"].astype(str) == "True"][k], on=k).shape[0]
    check = (f"Integrity check: the saved v2 models at their own thresholds reproduce {same:,} of v2's "
             f"{int((pub['go'].astype(str) == 'True').sum()):,} published walk-forward GO trades "
             f"({len(v2go):,} selected here).")
    print(check)
    md = [f"# v3 labels: missing winners, cost/size-aware labels, market alignment", "",
          f"Generated by `research/v3_label/v3_label_research.py` on {dt.date.today()}. **Historical research on "
          "design-period data, out-of-sample walk-forward only. Nothing is promoted or shadow-tracked.**", "",
          f"- Walk-forward: each month {first[:7]} → {last[:7]} (through {last}) scored by a model trained only on "
          f"earlier months: {len(s):,} signals over {sessions:,} sessions. Thresholds come from each month's "
          "calibration sessions (before the test month).",
          f"- Exit rule exit_v1 (published P&L, gross; costs subtracted as a flat round trip). {check}",
          "- \"Total\" is the sum of per-trade % returns (as if each trade had its own capital).", ""]

    # 1. concentration
    allx = df["pnl_%"].sort_values(ascending=False).to_numpy()
    tot = allx.sum()
    pos = allx[allx > 0].sum()
    md += ["## 1. P&L concentration", "",
           f"All {len(df):,} signals, {df['date'].min()} → {df['date'].max()}; total gross P&L {tot:+.1f} (sum of %), "
           f"sum of winners {pos:+.1f}.", "",
           "| Top share of trades | Trades | Their P&L | Share of total gross P&L | Share of all winners' P&L | Smallest P&L in it |",
           "|---|---|---|---|---|---|"]
    for q in (1, 5, 10):
        n = int(round(len(allx) * q / 100))
        md.append(f"| top {q}% | {n:,} | {allx[:n].sum():+.1f} | {allx[:n].sum() / tot * 100:.0f}% | "
                  f"{allx[:n].sum() / pos * 100:.0f}% | {allx[n - 1]:+.2f}% |")
    # v2 capture vs random of the same size (same count per month), walk-forward period
    cut = {q: np.quantile(s["pnl_%"], 1 - q / 100) for q in (1, 5, 10)}
    go = s[s["s_v2"] >= s["v2_thr_saved"]]
    per_m = go.groupby("month").size()
    groups = {m: g["pnl_%"].to_numpy() for m, g in s.groupby("month")}
    sims = {q: [] for q in (1, 5, 10)}
    sim_mean = []
    for _ in range(BOOT):
        pick = np.concatenate([rng.choice(groups[m], min(c, len(groups[m])), replace=False) for m, c in per_m.items()])
        for q in sims:
            sims[q].append((pick >= cut[q]).sum())
        sim_mean.append(pick.mean())
    md += ["", f"**Does v2 catch the big winners?** v2's {len(go):,} walk-forward GO trades vs {BOOT} random picks of "
           "the same number of signals in each month (the top-q% cut-offs are over the walk-forward signals).", "",
           "| Big winners = top | Cut-off | v2 GO trades among them | Random picks (mean, 95% range) | v2 vs random |",
           "|---|---|---|---|---|"]
    for q in (1, 5, 10):
        got = int((go["pnl_%"] >= cut[q]).sum())
        lo, hi = np.percentile(sims[q], [2.5, 97.5])
        md.append(f"| {q}% | {cut[q]:+.2f}% | {got} ({got / len(go) * 100:.1f}% of GO) | {np.mean(sims[q]):.0f} "
                  f"[{lo:.0f}, {hi:.0f}] | {'more' if got > hi else 'fewer' if got < lo else 'about the same'} |")
    md += ["", f"v2 GO mean {go['pnl_%'].mean():+.3f}% vs random {np.mean(sim_mean):+.3f}% "
               f"[{np.percentile(sim_mean, 2.5):+.3f}, {np.percentile(sim_mean, 97.5):+.3f}]; "
               f"big losers (bottom 10%): v2 {int((go['pnl_%'] <= np.quantile(s['pnl_%'], 0.10)).sum())} of {len(go):,}.", ""]

    # 2. market alignment
    a = df.copy()
    a["year"] = a["date"].str[:4]
    buy = a["direction"].str.upper() == "BUY"
    a["aligned"] = a["aligned_with_nifty"].map({1.0: "aligned", 0.0: "against"}).fillna("no NIFTY data")
    a["dir_breadth"] = np.where(buy, a["breadth"], 1 - a["breadth"])          # share of stocks moving with the trade
    a["breadth_bucket"] = pd.cut(a["dir_breadth"], [-0.01, 0.3, 0.5, 0.7, 1.01],
                                 labels=["<30% with", "30-50%", "50-70%", ">70% with"])
    a["rule"] = (a["aligned"] == "aligned") & (a["dir_breadth"] > 0.5)
    nd = nifty_day_direction()
    a["nifty_close_dir"] = a["date"].map(nd)
    sig = np.sign(a["nifty_ret"])
    a["flip"] = (sig != 0) & a["nifty_close_dir"].notna() & (sig != a["nifty_close_dir"])
    days_all = a["date"].nunique()

    def grp(g):
        x = g["pnl_%"]
        return f"{len(g):,} | {len(g) / days_all:.1f} | {x.mean() - 0.05:+.3f}% | {x.mean() - 0.10:+.3f}% | {(x > 0).mean() * 100:.0f}%"
    md += ["## 2. Market alignment", "",
           f"All {len(a):,} signals ({days_all:,} sessions). \"Aligned\" = the trade's direction matches NIFTY's move from "
           "its open at the signal (feature `aligned_with_nifty`). \"With the trade\" = the share of index stocks "
           "above their open for a BUY (below for a SELL), from `breadth`.", "",
           "| Group | Signals | per day | Mean after 0.05% | after 0.10% | Hit |", "|---|---|---|---|---|---|"]
    for name, g in [("all signals", a)] + [(f"{x}", g) for x, g in a.groupby("aligned")] + \
                   [(f"breadth {x}", g) for x, g in a.groupby("breadth_bucket", observed=True)] + \
                   [("rule: aligned AND >50% with the trade", a[a["rule"]]), ("not the rule", a[~a["rule"]])]:
        md.append(f"| {name} | {grp(g)} |")
    md += ["", "**Aligned × breadth** (mean after 0.10%, signals):", "",
           "| | " + " | ".join(str(c) for c in a["breadth_bucket"].cat.categories) + " |",
           "|---|" + "---|" * len(a["breadth_bucket"].cat.categories)]
    for al in ("aligned", "against"):
        cells = []
        for b in a["breadth_bucket"].cat.categories:
            g = a[(a["aligned"] == al) & (a["breadth_bucket"] == b)]
            cells.append(f"{g['pnl_%'].mean() - 0.10:+.3f}% ({len(g):,})" if len(g) else "—")
        md.append(f"| {al} | " + " | ".join(cells) + " |")
    md += ["", "**Per year** (mean after 0.10%):", "", "| Year | all | aligned | against | rule | not rule |", "|---|---|---|---|---|---|"]
    for y, g in a.groupby("year"):
        cells = [g, g[g["aligned"] == "aligned"], g[g["aligned"] == "against"], g[g["rule"]], g[~g["rule"]]]
        md.append(f"| {y} | " + " | ".join(f"{c['pnl_%'].mean() - 0.10:+.3f}%" for c in cells) + " |")
    known = a[(np.sign(a["nifty_ret"]) != 0) & a["nifty_close_dir"].notna()]
    md += ["", f"**Does NIFTY's direction at the signal hold to the close?** It flips by the close (NIFTY's day "
               f"close vs open has the other sign) for **{known['flip'].mean() * 100:.0f}%** of signals "
               f"({int(known['flip'].sum()):,} of {len(known):,}).", "",
           "| Entry time | Signals | NIFTY flips by the close |", "|---|---|---|"]
    tb = pd.cut(known["signal_minutes"], [0, 45, 105, 225, 300, 400], labels=["by 10:00", "10:00-11:00", "11:00-13:00", "13:00-14:15", "after 14:15"])
    for b, g in known.groupby(tb, observed=True):
        md.append(f"| {b} | {len(g):,} | {g['flip'].mean() * 100:.0f}% |")
    md.append("")

    # 3-5. candidates and baselines
    rows, ci_rows, sel = [], [], {}
    for k in RATES:
        sel[("v2", k)] = s[s["s_v2"] >= s[f"t_v2_{k}"]]
        for kind in LABELS:
            for fs in ("28", "25"):
                sel[(f"{kind}{fs}", k)] = s[s[f"s_{kind}{fs}"] >= s[f"t_{kind}{fs}_{k}"]]
    sel[("take-all", 100)] = s
    sel[("rule", None)] = s[(s["aligned_with_nifty"] == 1) &
                            (np.where(s["direction"].str.upper() == "BUY", s["breadth"], 1 - s["breadth"]) > 0.5)]
    groups_d = {m: g for m, g in s.groupby("month")}
    rand = {}
    for k in RATES:                                         # random, same count per month as v2 at k
        cnt = sel[("v2", k)].groupby("month").size()
        means, totals = [], []
        for _ in range(500):
            pick = pd.concat([groups_d[m].sample(min(c, len(groups_d[m])), random_state=int(rng.integers(1e9)))
                              for m, c in cnt.items()])
            means.append(pick["pnl_%"].mean())
            totals.append((pick["pnl_%"] - 0.10).sum())
        rand[k] = (np.mean(means), np.percentile(means, [2.5, 97.5]), np.mean(totals), len(pick))

    def name_of(key):
        c, k = key
        if c == "v2":
            return f"v2 recipe, top {k}%" + (" (= v2's design)" if k == 2 else "")
        if c in ("take-all", "rule"):
            return {"take-all": "take all signals", "rule": "rule: aligned with NIFTY and breadth agrees"}[c]
        return f"({c[0]}) {LABELS[c[0]]}, {c[1:]} inputs, top {k}%"
    md += ["## 3–5. Candidates vs baselines (walk-forward, out of sample)", "",
           "Each candidate is compared with the v2 recipe at the **same selection rate** (same calibration-based "
           "threshold rule). CI: 95% day-block bootstrap of (candidate − v2 at that rate), mean per trade.", ""]
    for k in RATES:
        v2k = sel[("v2", k)]
        md += [f"### Top {k}%", "",
               "| Candidate | Trades | /day | Mean after 0.05% | after 0.10% | Total after 0.10% | Worst month (after 0.10%) | "
               "Best month's share of gross | Ex-Jun-2024: mean after 0.05% / total after 0.10% | Δ mean vs v2 [95% CI] |",
               "|---|---|---|---|---|---|---|---|---|---|"]
        keys = [("v2", k)] + [(f"{kind}{fs}", k) for kind in LABELS for fs in ("28", "25")]
        for key in keys:
            t = sel[key]
            r = metrics(t, sessions)
            if key[0] == "v2":
                ci = "—"
            else:
                lo, hi = day_boot_diff(t, v2k, rng)
                d = t["pnl_%"].mean() - v2k["pnl_%"].mean()
                ci = f"{d:+.3f}% [{lo:+.3f}, {hi:+.3f}]"
                beats = r["total10"] > metrics(v2k, sessions)["total10"] and lo > 0
                ci_rows.append({"candidate": name_of(key), "k": k, "total10": r["total10"],
                                "v2_total10": metrics(v2k, sessions)["total10"], "diff": d, "lo": lo, "hi": hi, "beats": beats})
            rows.append({"key": key, **r})
            md.append(f"| {name_of(key)} | {r['n']:,} | {r['per_day']:.2f} | {fmt(r.get('mean05'))} | {fmt(r.get('mean10'))} | "
                      f"{r.get('total10', 0):+.1f} | {r.get('worst_month10', np.nan):+.1f} | {fmt(r.get('best_share'), '{:.0f}%')} "
                      f"({r.get('best_month', '')}) | {fmt(r.get('ex_mean05'))} / {r.get('ex_total10', 0):+.1f} | {ci} |")
        rm, (rlo, rhi), rtot, rn = rand[k]
        md += [f"| random picks, same count per month as v2 at {k}% (500 draws) | {rn:,} | {rn / sessions:.2f} | "
               f"{rm - 0.05:+.3f}% [{rlo - 0.05:+.3f}, {rhi - 0.05:+.3f}] | {rm - 0.10:+.3f}% | {rtot:+.1f} | | | | |", ""]
    md += ["### Baselines without a selection rate", "",
           "| Baseline | Trades | /day | Mean after 0.05% | after 0.10% | Total after 0.10% | Worst month | Best month's share | Ex-Jun-2024 |",
           "|---|---|---|---|---|---|---|---|---|"]
    for key in (("take-all", 100), ("rule", None)):
        r = metrics(sel[key], sessions)
        md.append(f"| {name_of(key)} | {r['n']:,} | {r['per_day']:.2f} | {fmt(r['mean05'])} | {fmt(r['mean10'])} | {r['total10']:+.1f} | "
                  f"{r['worst_month10']:+.1f} | {fmt(r['best_share'], '{:.0f}%')} | {fmt(r['ex_mean05'])} / {r['ex_total10']:+.1f} |")
    cr = pd.DataFrame(ci_rows)
    cr.to_csv(OUTDIR / "comparisons.csv", index=False)
    winners = cr[cr["beats"]]
    md += ["", "## 6. Does anything beat v2?", "",
           "Rule (fixed before the run): a candidate beats v2 if, at the same selection rate, its **total P&L after 0.10%** "
           "is higher **and** the 95% CI of its mean-per-trade difference from v2 is entirely above zero.", "",
           f"**{len(winners)} of {len(cr)}** candidate/rate combinations pass." +
           ("" if winners.empty else " They are: " + "; ".join(f"{w.candidate} (Δ {w.diff:+.3f}% [{w.lo:+.3f}, {w.hi:+.3f}], "
                                                               f"total {w.total10:+.1f} vs v2 {w.v2_total10:+.1f})" for w in winners.itertuples()) + "."),
           "", "<!-- recommendation -->", ""]
    keep = ""                                               # the hand-written reading survives a re-run
    if DOC.exists() and "<!-- recommendation -->" in DOC.read_text():
        keep = DOC.read_text().split("<!-- recommendation -->", 1)[1].strip()
    DOC.write_text("\n".join(md) + ("\n" + keep + "\n" if keep else "\n"))
    print(f"written {DOC}; winners: {len(winners)}")


if __name__ == "__main__":
    {"score": score, "report": report}[sys.argv[1] if len(sys.argv) > 1 else "report"]()
