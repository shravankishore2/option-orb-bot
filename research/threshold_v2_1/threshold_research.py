"""
threshold_research.py — docs/THRESHOLD_PROTOCOL.md (steps 4-5) and the history section of
docs/V2_1_PROTOCOL.md. Research only; the champion, the frozen v2 baseline and the live bot
are not touched.

    ORBITAL_OFFLINE=1 python research/threshold_v2_1/threshold_research.py sweep            # step 5 + v2.1 history
    ORBITAL_OFFLINE=1 python research/threshold_v2_1/threshold_research.py drift LIVE_DIR   # step 4 analyses 1-3

Writes docs/THRESHOLD_RESULTS.md (sections) and docs/figures/threshold_sweep.png.
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root: the shared modules live there

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import strategy_config as C
import walk_forward as W

TEST_FROM, TEST_TO = "2022-01-01", "2026-09-22"
PERIODS = {"clean_backward": ("2022-01-01", "2023-08-31"), "development": ("2023-09-05", "2026-09-04"),
           "clean_forward": ("2026-09-05", "2026-09-22"), "combined": (TEST_FROM, TEST_TO)}
TOP_K = (1, 2, 3, 5, 10, 20, 50)
FIXED = (0.50, 0.54, 0.58, 0.60, 0.644)
COSTS = (0.0, 0.05, 0.10)
OUT = Path("docs/THRESHOLD_RESULTS.md")
FIG = Path("docs/figures/threshold_sweep.png")
CAL_FROM, CAL_TO = "2025-11-13", "2026-09-22"            # v2's calibration window (step 4 history)


def _fit(train, features):
    """walk_forward.fit_with_threshold, also returning the calibration scores."""
    from xgboost import XGBClassifier
    sessions = np.sort(train["date"].unique())
    cut = sessions[int(len(sessions) * (1 - C.CALIBRATION_FRACTION))]
    fit, cal = train[train["date"] < cut], train[train["date"] >= cut]
    params = dict(C.MODEL_PARAMS)
    y = fit[C.LABEL]
    params["scale_pos_weight"] = (y == 0).sum() / max((y == 1).sum(), 1)
    m = XGBClassifier(**params)
    m.fit(fit[features].fillna(0), y)
    return m, m.predict_proba(cal[features].fillna(0))[:, 1]


def scored_months(df, features):
    """Every test-month signal with its walk-forward score and that month's top-k thresholds."""
    out = []
    for m in sorted(df.loc[(df["date"] >= TEST_FROM) & (df["date"] <= TEST_TO), "month"].unique()):
        train, test = df[df["month"] < m], df[df["month"] == m].copy()
        if len(train) < C.MIN_TRAIN_SIGNALS or test.empty:
            continue
        model, cal = _fit(train, features)
        test["score"] = model.predict_proba(test[features].fillna(0))[:, 1]
        for k in TOP_K:
            test[f"thr_top{k}"] = float(np.quantile(cal, 1 - k / 100))
        out.append(test)
    return pd.concat(out, ignore_index=True)


def metrics(t, sessions):
    x = t["pnl_%"]
    r = {"trades": len(t), "per_day": len(t) / max(sessions, 1), "hit": (x > 0).mean() * 100 if len(t) else np.nan}
    for c in COSTS:
        r[f"mean_{c}"] = x.mean() - c if len(t) else np.nan
        r[f"total_{c}"] = (x - c).sum() if len(t) else 0.0
    if len(t):
        bym = t.groupby(t["date"].str[:7])["pnl_%"]
        r["worst_month"] = bym.sum().min()
        best = bym.sum().idxmax()
        ex = t[t["date"].str[:7] != best]["pnl_%"]
        r["best_month"] = best
        for c in COSTS:
            r[f"ex_mean_{c}"] = ex.mean() - c if len(ex) else np.nan
            r[f"ex_total_{c}"] = (ex - c).sum()
    return r


def sweep():
    df, feats = W.load_dataset()
    print(f"{len(df):,} signals, {len(feats)} inputs", flush=True)
    s = scored_months(df, feats)
    pub = pd.read_csv("data/research/walkforward.csv", dtype={"date": str})
    pub = pub[(pub["date"] >= TEST_FROM) & (pub["date"] <= TEST_TO)]
    k = ["date", "symbol", "direction"]
    mine = s[s["score"] >= s["thr_top2"]][k]
    theirs = pub[pub["go"].astype(str) == "True"][k]
    same = mine.merge(theirs, on=k).shape[0]
    check = f"Integrity check: top 2% reproduces v2's published walk-forward GO trades: {same:,} of {len(theirs):,} identical, {len(mine):,} in total."
    print(check, flush=True)

    settings = [(f"top {kk}%", s["score"] >= s[f"thr_top{kk}"]) for kk in TOP_K] + \
               [(f"fixed {f:g}", s["score"] >= f) for f in FIXED]
    rows = []
    for name, mask in settings:
        for period, (a, b) in PERIODS.items():
            inper = (s["date"] >= a) & (s["date"] <= b)
            rows.append({"setting": name, "period": period, "share": mask[inper].mean() * 100,
                         **metrics(s[mask & inper], s.loc[inper, "date"].nunique())})
    res = pd.DataFrame(rows)

    # v2.1 history (V2_1_PROTOCOL "History"): same walk-forward, 25 inputs, top 2%
    f21 = [f for f in feats if f not in ("entry_log", "orb_range_abs", "prev_close_vs_orb")]
    s21 = scored_months(df, f21)
    v21 = []
    for name, data in (("v2 (28 inputs)", s), ("v2.1 (25 inputs)", s21)):
        for period, (a, b) in PERIODS.items():
            inper = (data["date"] >= a) & (data["date"] <= b)
            v21.append({"model": name, "period": period,
                        **metrics(data[(data["score"] >= data["thr_top2"]) & inper], data.loc[inper, "date"].nunique())})
    plot(res)
    write_sweep(res, pd.DataFrame(v21), check)


def plot(res):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    FIG.parent.mkdir(parents=True, exist_ok=True)
    comb = res[res["period"] == "combined"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    for ax, kind in zip(axes, ("total", "mean")):
        for c, col in zip(COSTS, ("#2a9d8f", "#e9c46a", "#e76f51")):
            for marker, sel in (("o", comb["setting"].str.startswith("top")), ("s", comb["setting"].str.startswith("fixed"))):
                d = comb[sel].sort_values("share")
                ax.plot(d["share"], d[f"{kind}_{c}"], marker=marker, color=col, lw=1.5 if marker == "o" else 0,
                        label=f"{'top-k%' if marker == 'o' else 'fixed thr.'}, cost {c:.2f}%")
        ax.axhline(0, color="#888", lw=0.8)
        ax.set_xscale("log")
        ax.set_xlabel("share of signals taken (%, log scale)")
        ax.set_ylabel("total P&L, sum of % per trade" if kind == "total" else "mean P&L per trade (%)")
        ax.set_title(("Total" if kind == "total" else "Mean per trade") + " P&L, walk-forward 2022-01 → 2026-09")
        ax.grid(alpha=0.25)
    axes[0].legend(fontsize=7, ncol=2)
    for _, r in comb.iterrows():
        if r["setting"].startswith("fixed"):
            axes[0].annotate(r["setting"].split()[1], (r["share"], r["total_0.05"]), fontsize=7, xytext=(3, 3),
                             textcoords="offset points")
    fig.tight_layout()
    fig.savefig(FIG, dpi=140)


def _f(v, spec="{:+.3f}%"):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else spec.format(v)


def write_sweep(res, v21, check):
    md = ["# GO threshold — results", "",
          "Follows [`THRESHOLD_PROTOCOL.md`](THRESHOLD_PROTOCOL.md) (committed before anything below ran). "
          "**Exploratory:** these months were examined while v1 and v2 were built.", "",
          "## Step 5: threshold sweep (walk-forward, 2022-01 → 2026-09-22)", "", check, "",
          "Top k%: each month's threshold is that month's model's (100 − k)th calibration percentile (data before "
          "the month only). Fixed: GO when that month's model scores at or above the number. P&L in % per trade "
          "(gross; the cost columns subtract a flat round trip); totals are sums over trades.", "",
          f"![threshold sweep]({FIG.relative_to('docs')})", ""]
    for period in PERIODS:
        md += [f"### {period} ({PERIODS[period][0]} → {PERIODS[period][1]})", "",
               "| Setting | Share | Trades | /day | Hit | Mean 0% | Mean 0.05% | Mean 0.10% | Total 0% | Total 0.05% | Total 0.10% | Worst month | Total 0.05% w/o best month | Mean 0.05% w/o best month |",
               "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for _, r in res[res["period"] == period].iterrows():
            md.append(f"| {r['setting']} | {r['share']:.1f}% | {r['trades']:,} | {r['per_day']:.2f} | {_f(r['hit'], '{:.1f}%')} | "
                      f"{_f(r['mean_0.0'])} | {_f(r['mean_0.05'])} | {_f(r['mean_0.1'])} | {_f(r['total_0.0'], '{:+.1f}')} | "
                      f"{_f(r['total_0.05'], '{:+.1f}')} | {_f(r['total_0.1'], '{:+.1f}')} | {_f(r.get('worst_month'), '{:+.1f}')} | "
                      f"{_f(r.get('ex_total_0.05'), '{:+.1f}')} | {_f(r.get('ex_mean_0.05'))} |")
        md.append("")
    comb = res[res["period"] == "combined"].set_index("setting")
    ref = comb.loc["fixed 0.644"]
    lines = []
    for c in (0.05, 0.10):
        better = [s for s in comb.index if comb.loc[s, f"total_{c}"] > ref[f"total_{c}"]
                  and comb.loc[s, f"ex_total_{c}"] > ref[f"ex_total_{c}"] and comb.loc[s, "trades"] > ref["trades"]]
        best = comb[f"total_{c}"].idxmax()
        lines.append(f"- **At {c:.2f}% cost:** the highest combined total is **{best}** ({comb.loc[best, f'total_{c}']:+.1f} "
                     f"over {comb.loc[best, 'trades']:,} trades) against fixed 0.644's {ref[f'total_{c}']:+.1f} over "
                     f"{ref['trades']:,}. Settings that take more trades than 0.644 and have a higher total both with and "
                     f"without their best month: {', '.join(better) or 'none'}.")
    agree = (comb.loc["fixed 0.54", "total_0.05"] > ref["total_0.05"]) and (comb.loc["fixed 0.54", "ex_total_0.05"] > ref["ex_total_0.05"])
    md += ["### Answer to question 2", ""] + lines + [
        f"- **Protocol step 7, condition 4 (does the sweep agree with 0.54 over 0.644 at 0.05%, with and without the "
        f"best month?):** {'yes' if agree else 'no'}.", ""]
    md += ["## v2.1 on history (V2_1_PROTOCOL: exploratory, not decisive)", "",
           "| Model | Period | Trades | Hit | Mean 0% | Mean 0.05% | Mean 0.10% | Total 0.05% | Worst month | Mean 0.05% w/o best month |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for _, r in v21.iterrows():
        md.append(f"| {r['model']} | {r['period']} | {r['trades']:,} | {_f(r['hit'], '{:.1f}%')} | {_f(r['mean_0.0'])} | "
                  f"{_f(r['mean_0.05'])} | {_f(r['mean_0.1'])} | {_f(r['total_0.05'], '{:+.1f}')} | {_f(r.get('worst_month'), '{:+.1f}')} | "
                  f"{_f(r.get('ex_mean_0.05'))} |")
    md.append("")
    existing = OUT.read_text() if OUT.exists() else ""
    tail = existing[existing.index("## Step 4"):] if "## Step 4" in existing else ""
    OUT.write_text("\n".join(md) + "\n" + tail)
    print("\n".join(md))


# ---------------------------------------------------------------- step 4 analyses 1-3
BINS = [(9, 40), (10, 10), (10, 40), (11, 10), (11, 40), (12, 10), (12, 40), (13, 10), (13, 40), (14, 10), (14, 40), (15, 10)]


def _bin(t):
    m = int(t[:2]) * 60 + int(t[3:5])
    edges = [h * 60 + mm for h, mm in BINS]
    return int(np.searchsorted(edges, m, side="right") - 1)


def _psi(ref, live, w_ref=None, bins=10):
    ref, live = np.asarray(ref, float), np.asarray(live, float)
    ok = ~np.isnan(ref)
    edges = np.unique(np.quantile(ref[ok], np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return np.nan
    def share(x, w):
        x = np.asarray(x, float); w = np.ones(len(x)) if w is None else np.asarray(w, float)
        m = ~np.isnan(x)
        idx = np.clip(np.searchsorted(edges[1:-1], x[m], side="right"), 0, len(edges) - 2)
        h = np.bincount(idx, weights=w[m], minlength=len(edges) - 1)
        h = np.append(h, w[~m].sum())
        return h / max(w.sum(), 1e-12)
    p, q = np.clip(share(ref, w_ref), 1e-4, None), np.clip(share(live, None), 1e-4, None)
    return float(np.sum((q - p) * np.log(q / p)))


def drift(live_dir, skew_prefix=None):
    from scipy.stats import binomtest, ks_2samp
    import drift as DR
    import registry
    import shadow
    live_dir = Path(live_dir)
    base = registry.baseline()
    S = shadow._read(live_dir / "data/live/shadow_signals.csv")
    S = S[S["date"] >= "2026-09-23"].drop_duplicates("signal_id").copy()
    D = pd.read_csv(live_dir / "live_decisions.csv", dtype={"date": str})
    D = D[D["date"] >= "2026-09-23"].copy()
    df, feats = W.load_dataset()
    H = df[(df["date"] >= CAL_FROM) & (df["date"] <= CAL_TO)].copy()
    H["score"] = base.model.predict_proba(H[base.features].fillna(0))[:, 1]
    H["go"] = H["score"] >= base.threshold
    for t in (H, S, D):
        t["bin"] = t["time"].astype(str).map(_bin)
    rate = H.groupby("bin")["go"].mean()
    md = ["## Step 4: is the live GO rate wrong?", "",
          f"History: v2's calibration window {CAL_FROM} → {CAL_TO} ({len(H):,} signals, out-of-sample for v2), "
          f"scored by the live v2 model (GO ≥ {base.threshold:.4f}); its GO rate is {H['go'].mean() * 100:.2f}%.", "",
          "### 1. GO rate, expected for the live time-of-day mix", "",
          "| Live set | Sessions | Signals | GO | Observed rate (95% CI) | Expected for this time-of-day mix | Binomial p |",
          "|---|---|---|---|---|---|---|"]
    sets = [("decision log (live bot)", D, D["decision"] == "GO"),
            ("shadow, live sessions", S[S["source"] == "live"], S.loc[S["source"] == "live", "baseline_go"].astype(str).isin(["True", "true"])),
            ("shadow, replayed sessions", S[S["source"] == "replay"], S.loc[S["source"] == "replay", "baseline_go"].astype(str).isin(["True", "true"]))]
    for name, t, go in sets:
        n, k = len(t), int(go.sum())
        exp = float((t["bin"].map(rate).fillna(H["go"].mean())).mean()) if n else np.nan
        lo, hi = DR.wilson(k, n)
        p = binomtest(k, n, exp).pvalue if n else np.nan
        md.append(f"| {name} | {t['date'].nunique()} | {n} | {k} | {k / n * 100:.2f}% ({lo * 100:.2f}–{hi * 100:.2f}%) | "
                  f"{exp * 100:.2f}% ({exp * n:.1f} GO) | {p:.3f} |")
    md += ["", "GO rate in history by entry time (30-minute bins from 09:40) and the live signal mix:", "",
           "| Bin from | History signals | History GO rate | Live signals (decision log) |", "|---|---|---|---|"]
    for b in sorted(H["bin"].unique()):
        h, m = BINS[b] if 0 <= b < len(BINS) else (0, 0)
        md.append(f"| {h:02d}:{m:02d} | {int((H['bin'] == b).sum()):,} | {rate.get(b, np.nan) * 100:.2f}% | {int((D['bin'] == b).sum())} |")
    # 2. scores
    w = H["bin"].map(D["bin"].value_counts(normalize=True)).fillna(0) / H["bin"].map(H["bin"].value_counts(normalize=True))
    ls = pd.to_numeric(D["score"], errors="coerce").dropna()
    ks = ks_2samp(H["score"], ls)
    md += ["", "### 2. Score distribution (decision log vs history)", "",
           f"- Median {ls.median():.3f} live vs {H['score'].median():.3f}; 90th pct {ls.quantile(.9):.3f} vs {H['score'].quantile(.9):.3f}; "
           f"98th pct {ls.quantile(.98):.3f} vs {H['score'].quantile(.98):.3f}; max {ls.max():.3f}.",
           f"- KS {ks.statistic:.3f} (p = {ks.pvalue:.3f}); PSI with history reweighted to the live time-of-day mix "
           f"{_psi(H['score'], ls, w):.3f}."]
    # 3. features
    live_f = S
    sess = live_f["date"].nunique()
    hdays = np.sort(H["date"].unique())
    rng = np.random.default_rng(0)
    rows = []
    for f in [c for c in base.features]:
        lv = pd.to_numeric(live_f[f"x_{f}"], errors="coerce")
        wl = H["bin"].map(live_f["bin"].value_counts(normalize=True)).fillna(0) / H["bin"].map(H["bin"].value_counts(normalize=True))
        p_obs = _psi(H[f], lv, wl)
        null = []
        for _ in range(100):
            a = rng.integers(0, len(hdays) - sess)
            win = H[(H["date"] >= hdays[a]) & (H["date"] <= hdays[a + sess - 1])]
            null.append(_psi(H[f], win[f], wl))
        p95 = float(np.nanquantile(null, 0.95))
        rows.append((f, p_obs, p95, ks_2samp(H[f].dropna(), lv.dropna()).statistic))
    rows.sort(key=lambda r: -(r[1] - r[2]))
    flagged = [r[0] for r in rows if r[1] > r[2]]
    md += ["", f"### 3. Inputs: {len(live_f)} logged signals over {sess} sessions vs history (time-of-day matched)", "",
           f"A feature is flagged if its PSI is above the 95th percentile of PSI for random {sess}-session stretches of the "
           f"history (the same noise floor as drift.py). Flagged: **{', '.join(flagged) or 'none'}**.", "",
           "| Input | PSI | Noise floor (95th pct) | KS |", "|---|---|---|---|"]
    for f, a, b, k in rows[:10]:
        md.append(f"| {f}{' ⚠' if a > b else ''} | {a:.3f} | {b:.3f} | {k:.3f} |")
    if skew_prefix:
        md += skew_section(skew_prefix)
    head = OUT.read_text() if OUT.exists() else "# GO threshold — results\n\n"
    head = head[:head.index("## Step 4")] if "## Step 4" in head else head
    OUT.write_text(head.rstrip("\n") + "\n\n" + "\n".join(md) + "\n")
    print("\n".join(md))


def skew_section(prefix):
    sh = pd.read_csv(prefix + "_shadow.csv", dtype={"date": str})
    dc = pd.read_csv(prefix + "_decisions.csv", dtype={"date": str})
    geo = ["orb_range_pct", "gap_pct", "breakout_strength", "entry_vs_mid", "prev_close_vs_orb", "signal_minutes",
           "direction_enc", "orb_range_abs", "entry_log"]
    from features import FEATURES
    ok = sh[sh["status"] == "ok"]
    md = ["", "### 4. Skew: every logged signal recomputed offline from historical candles", "",
          f"{len(sh)} logged signals (shadow log, 2026-09-23 → {sh['date'].max()}); {len(ok)} re-derived offline by the "
          f"research code with the universe and sector map the bot had on those days; "
          f"{int((ok['same_entry'] == True).sum())} with the same entry time and price.", "",
          "| Input | Compared | Differ (beyond 1e-6 rel.) | Max abs. difference |", "|---|---|---|---|"]
    worst = []
    for f in geo + FEATURES:
        a, b = ok[f"live_{f}"], ok[f"off_{f}"]
        both = a.notna() & b.notna()
        d = (a - b)[both].abs()
        tol = 1e-6 * np.maximum(1, a[both].abs())
        nd = int((d > tol).sum())
        miss = int((a.isna() != b.isna()).sum())
        worst.append((f, int(both.sum()), nd, float(d.max()) if len(d) else 0.0, miss))
    for f, n, nd, mx, miss in sorted(worst, key=lambda r: (-r[2], -r[3])):
        if nd or miss:
            md.append(f"| {f} | {n} | {nd}{f' (+{miss} missing on one side)' if miss else ''} | {mx:.4g} |")
    if not any(r[2] or r[4] for r in worst):
        md.append("| (all 28) | — | 0 | 0 |")
    sd = (ok["offline_score"] - ok["logged_score"]).abs()
    md += ["", f"Scores: max |offline − logged| = {sd.max():.4g} over {len(ok)} signals.", ""]
    for src in ("live", "replay"):
        g = ok[ok["source"] == src]
        if len(g):
            dd = (g["offline_score"] - g["logged_score"]).abs()
            md.append(f"- {src}: {len(g)} signals, max score difference {dd.max():.4g}, same entry {int((g['same_entry'] == True).sum())}.")
    md += ["", "**Decision log vs offline** (scores only; features weren't logged before 2026-10-05):", "",
           "| Day | Live signals | Re-derived offline (same stock & direction) | Same entry time | Max score diff | Only live | Only offline |",
           "|---|---|---|---|---|---|---|"]
    for day, g in dc.groupby("date"):
        live = g[g["live_time"].notna()]
        both = live[live["offline_time"].notna()]
        same_t = int((both["live_time"] == both["offline_time"]).sum())
        mx = (both["live_score"] - both["offline_score"]).abs().max() if len(both) else np.nan
        only_off = int(g["live_time"].isna().sum())
        md.append(f"| {day} | {len(live)} | {len(both)} | {same_t} | {mx:.4g} | {len(live) - len(both)} | {only_off} |")
    return md


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "sweep":
        sweep()
    elif cmd == "drift":
        drift(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
    else:
        sys.exit(__doc__)
