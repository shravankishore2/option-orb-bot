"""
drift.py — weekly training/serving skew check (deploy/orbital-drift.timer, Saturdays).

Compares what the live bot sees with what the model was trained on:

  * every model input: live distribution (all live shadow signals, GO and
    NO-GO, features as of signal time) vs the historical training data, by
    PSI (population stability index, deciles of the training data plus a
    missing-value bin) and the two-sample Kolmogorov-Smirnov distance.
    PSI >= 0.25 = drifted, 0.10-0.25 = moderate (the usual rule of thumb).
  * the score: live v2-baseline scores vs v2's scores on its own calibration
    sessions (the newest 15% of training sessions, out-of-sample for v2 and
    exactly where its GO threshold was set, so the designed GO rate is 2%).
  * the live GO rate of the champion and the v2 baseline vs the designed
    1 - THRESHOLD_QUANTILE = 2%, with a Wilson 95% interval.

Writes data/live/drift.json (the dashboard's scorecard) and the "Live drift"
section of docs/RESULTS.md. Needs at least MIN_LIVE_SESSIONS live sessions
(one full week) — before that it records that it waited.

    python drift.py            # the weekly run
    python drift.py --print    # compute and print, write nothing
"""

import argparse
import datetime as dt
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

import registry
import shadow
import strategy_config as C

BASE_DIR = Path(__file__).resolve().parent
OUT = shadow.LIVE_DIR / "drift.json"
RESULTS = BASE_DIR / "docs" / "RESULTS.md"
START, END = "<!-- drift:start -->", "<!-- drift:end -->"
FEATURES = shadow.FEATURE_COLUMNS
MIN_LIVE_SESSIONS = 5
PSI_DRIFT, PSI_WATCH = 0.25, 0.10
DESIGNED_GO_RATE = 1 - C.THRESHOLD_QUANTILE


def _edges(ref, bins=10):
    ref = pd.Series(ref, dtype=float).dropna()
    return np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1))) if len(ref) else np.array([])


def _shares(x, edges):
    x = pd.Series(x, dtype=float)
    out = []
    if len(edges) > 1:
        idx = np.searchsorted(edges[1:-1], x.dropna().to_numpy(), side="right")
        out = list(np.bincount(idx, minlength=len(edges) - 1))
    out.append(int(x.isna().sum()))                      # missing values are their own bin
    return np.asarray(out, float) / max(len(x), 1)


def psi(ref, live, bins=10, eps=1e-4, edges=None, ref_shares=None):
    """PSI over the reference's quantile bins, with a separate bin for missing values."""
    edges = _edges(ref, bins) if edges is None else edges
    p = _shares(ref, edges) if ref_shares is None else ref_shares
    q = _shares(live, edges)
    p, q = np.clip(p, eps, None), np.clip(q, eps, None)
    return float(np.sum((q - p) * np.log(q / p)))


def null_psi(ref, dates, n_sessions, n_windows=200, seed=0):
    """PSI of `n_windows` random runs of `n_sessions` consecutive historical
    sessions against the whole history: how far a normal stretch of that length
    sits from the reference, by chance alone.

    Needed because several inputs (NIFTY return, breadth, NIFTY's position in
    its range...) are nearly the same for every signal at a given time of day,
    so a week of live data is only ~5 market states; against years of history
    that looks like drift by construction (one live day flagged 20 of 28 inputs
    with a plain PSI >= 0.25 rule)."""
    days = np.sort(pd.unique(dates))
    if len(days) <= n_sessions:
        return np.array([])
    rng = np.random.default_rng(seed)
    edges = _edges(ref)
    base = _shares(ref, edges)
    out = []
    for start in rng.integers(0, len(days) - n_sessions, size=n_windows):
        mask = (dates >= days[start]) & (dates <= days[start + n_sessions - 1])
        out.append(psi(None, ref[mask], edges=edges, ref_shares=base))
    return np.asarray(out)


def ks(ref, live):
    from scipy.stats import ks_2samp
    a, b = pd.Series(ref, dtype=float).dropna(), pd.Series(live, dtype=float).dropna()
    if len(a) < 2 or len(b) < 2:
        return None, None
    r = ks_2samp(a, b)
    return float(r.statistic), float(r.pvalue)


def wilson(k, n, z=1.96):
    if n == 0:
        return None, None
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def go_rate(flags):
    flags = pd.Series(flags).astype(bool)
    n, k = len(flags), int(flags.sum())
    lo, hi = wilson(k, n)
    return {"go": k, "signals": n, "rate": k / n if n else None, "ci95": [lo, hi],
            "designed": DESIGNED_GO_RATE,
            "consistent_with_design": (lo is not None and lo <= DESIGNED_GO_RATE <= hi)}


def live_signals(signals_file=None):
    s = shadow._read(signals_file or shadow.SIGNALS_FILE)
    if s.empty:
        return s
    s = s[s["source"] == "live"].drop_duplicates("signal_id").copy()
    for c in [f"x_{f}" for f in FEATURES] + ["score", "threshold", "baseline_score", "baseline_threshold"]:
        s[c] = pd.to_numeric(s[c], errors="coerce")
    for c in ("model_go", "baseline_go"):
        s[c] = s[c].astype(str).isin(["True", "true", "1"])
    return s


def compute(history, live, baseline):
    """The drift report as a dict (pure: no files)."""
    sessions = int(live["date"].nunique()) if not live.empty else 0
    report = {"run_at": dt.datetime.now().isoformat(timespec="seconds"), "live_sessions": sessions,
              "live_signals": len(live), "reference_rows": len(history),
              "live_from": live["date"].min() if sessions else None,
              "live_to": live["date"].max() if sessions else None}
    if sessions < MIN_LIVE_SESSIONS:
        report["status"] = f"waiting: {sessions} live session(s), need {MIN_LIVE_SESSIONS} (one full week)"
        return report

    feats = []
    dates = history["date"].to_numpy()
    for f in FEATURES:
        p = psi(history[f], live[f"x_{f}"])
        d, pv = ks(history[f], live[f"x_{f}"])
        null = null_psi(history[f].to_numpy(dtype=float), dates, sessions)
        p95 = float(np.quantile(null, 0.95)) if len(null) else None
        unusual = p95 is None or p > p95
        feats.append({"feature": f, "psi": p, "ks": d, "ks_p": pv, "psi_null95": p95,
                      "live_missing": float(live[f"x_{f}"].isna().mean()),
                      "train_missing": float(history[f].isna().mean()),
                      "flag": ("drifted" if p >= PSI_DRIFT and unusual else
                               "moderate" if p >= PSI_WATCH and unusual else "")})
    feats.sort(key=lambda r: -r["psi"])

    cal_from = baseline.meta.get("calibration_from")
    cal = history[(history["date"] >= cal_from) & (history["date"] <= str(baseline.trained_through))] \
        if cal_from else history.iloc[0:0]
    ref_scores = baseline.model.predict_proba(cal[baseline.features].fillna(0))[:, 1] if len(cal) else []
    live_scores = live["baseline_score"].dropna()
    d, pv = ks(ref_scores, live_scores)
    report.update(
        status="ok",
        features=feats,
        drifted=[r["feature"] for r in feats if r["flag"] == "drifted"],
        moderate=[r["feature"] for r in feats if r["flag"] == "moderate"],
        score={"reference": f"v2 on its calibration sessions {cal_from} → {baseline.trained_through} "
                            f"({len(cal):,} signals, out-of-sample for v2)",
               "psi": psi(ref_scores, live_scores) if len(cal) else None, "ks": d, "ks_p": pv,
               "reference_median": float(np.median(ref_scores)) if len(cal) else None,
               "live_median": float(live_scores.median()) if len(live_scores) else None,
               "reference_p98": float(np.quantile(ref_scores, C.THRESHOLD_QUANTILE)) if len(cal) else None,
               "live_p98": float(live_scores.quantile(C.THRESHOLD_QUANTILE)) if len(live_scores) else None,
               "reference_go_rate": float(np.mean(np.asarray(ref_scores) >= baseline.threshold)) if len(cal) else None},
        go_rate={"champion": go_rate(live["model_go"]), "baseline": go_rate(live["baseline_go"])},
    )
    return report


def markdown(r):
    lines = [START, "", "## Live drift — training/serving skew (weekly)", ""]
    if r.get("status") != "ok":
        lines += [f"_{r['run_at'][:10]}: {r.get('status')}._", "", END]
        return "\n".join(lines)
    g, b, s = r["go_rate"]["champion"], r["go_rate"]["baseline"], r["score"]
    pct = lambda v: "—" if v is None else f"{v * 100:.1f}%"
    num = lambda v: "—" if v is None else f"{v:.3f}"
    lines += [
        f"Run {r['run_at'][:10]} on **{r['live_signals']:,} live signals over {r['live_sessions']} sessions** "
        f"({r['live_from']} → {r['live_to']}), against {r['reference_rows']:,} training signals.",
        "",
        f"- **Live GO rate:** champion {g['go']}/{g['signals']} = {pct(g['rate'])} "
        f"(95% CI {pct(g['ci95'][0])}–{pct(g['ci95'][1])}); v2 baseline {pct(b['rate'])}; "
        f"designed {pct(DESIGNED_GO_RATE)}. "
        + ("Consistent with the design." if g["consistent_with_design"] else "**Not consistent with the design.**"),
        f"- **Score distribution:** live median {num(s['live_median'])} vs {num(s['reference_median'])} on "
        f"{s['reference']}; PSI {num(s['psi'])}, KS {num(s['ks'])}. 98th percentile live {num(s['live_p98'])} "
        f"vs {num(s['reference_p98'])}.",
        f"- **Drifted features (PSI ≥ {PSI_DRIFT} and unusual for a window this long):** {', '.join(r['drifted']) or 'none'}. "
        f"Moderate ({PSI_WATCH}–{PSI_DRIFT}): {', '.join(r['moderate']) or 'none'}.",
        "",
        f"A feature is flagged only if its PSI is also above the 95th percentile of PSI for random "
        f"{r['live_sessions']}-session stretches of the training history (`psi_null95`): market-wide inputs "
        f"are nearly constant within a day, so a short live window always looks unlike five years.",
        "",
        "| Feature | PSI | Normal for this window (95th pct) | KS | Missing live / train |",
        "|---|---|---|---|---|",
    ]
    for f in r["features"][:10]:
        lines.append(f"| {f['feature']}{' ⚠' if f['flag'] == 'drifted' else ''} | {num(f['psi'])} | "
                     f"{num(f['psi_null95'])} | {num(f['ks'])} | {f['live_missing']:.0%} / {f['train_missing']:.0%} |")
    lines += ["", "Top 10 by PSI; all 28 in `data/live/drift.json`. Live = every live shadow signal "
              "(GO and NO-GO), features as of signal time.", "", END]
    return "\n".join(lines)


def write_results(md, path=RESULTS):
    text = path.read_text() if path.exists() else "# Results\n"
    if START in text and END in text:
        text = text[:text.index(START)] + md + text[text.index(END) + len(END):]
    else:
        text = text.rstrip("\n") + "\n\n" + md + "\n"
    path.write_text(text)


def run(write=True):
    import challenger
    history = challenger.load_history()
    if history.empty:
        raise SystemExit(f"no training history at {challenger.HISTORY}")
    history[FEATURES] = history[FEATURES].apply(pd.to_numeric, errors="coerce")
    report = compute(history, live_signals(), registry.baseline())
    if write:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(report, indent=2, default=str))
        os.replace(tmp, OUT)
        write_results(markdown(report))
    print(markdown(report))
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true", help="compute and print only")
    a = ap.parse_args()
    run(write=not a.print)
