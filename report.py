"""
report.py — the numbers that go in the thesis.

Reads the walk-forward output and the baseline datasets, and writes:
  docs/RESULTS.md                   tables, per period
  data/research/summary.json        the same numbers, for the dashboard
  docs/summary.json                 a published copy (the deployed dashboard reads it)

Periods (see strategy_config.py / docs/PREREGISTRATION.md):
  clean_backward  2022-01 → 2023-08     never looked at during design
  development     2023-09 → 2026-09-04  design choices were made here (still walk-forward)
  clean_forward   2026-09-05 → 09-22    sessions after development ended
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

import dhan_client as dhan
import history_cache
import strategy_config as C
import survivorship
from baselines import random_entries
from stats import permutation_vs_random, summarize

BASE_DIR = Path(__file__).resolve().parent
R = BASE_DIR / "data" / "research"

import datetime as dt

PERIODS = {
    "clean_backward": C.CLEAN_BACKWARD,
    "development": (C.DEV_START, C.DEV_END),
    "clean_forward": C.CLEAN_FORWARD,
    "v2_forward": (C.V2_FORWARD_START, dt.date.today()),
}

V1 = "Model v1 · hit_1_5r (pre-registered)"
V2 = "Model v2 · profit (live)"
STRATEGY_ORDER = ["Random entry", "Plain ORB (break only)", "ORBITAL rules (no model)", V1, V2]

# How much each model's number in each period can be trusted.
STATUS = {
    ("clean_backward", V1): "pre-registered clean test",
    ("clean_backward", V2): "post-hoc (chosen after v1 failed here)",
    ("development", V1): "design period",
    ("development", V2): "design period",
    ("clean_forward", V1): "pre-registered clean test",
    ("clean_forward", V2): "post-hoc",
    ("v2_forward", V1): "—",
    ("v2_forward", V2): "pre-registered clean test",
}


def sessions_between(start, end):
    n = dhan.regular_session(history_cache._read_cache(history_cache._cache_path("NIFTY_INDEX", "5m")))
    return [d.isoformat() for d in sorted({d for d in n.index.date if start <= d <= end})]


def in_period(df, start, end):
    return df[(df["date"] >= start.isoformat()) & (df["date"] <= end.isoformat())]


def pct(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{v:+.3f}%"


def period_report(name, start, end, wf, wf_v1, plain, table):
    sessions = sessions_between(start, end)
    if not sessions:
        return None, []

    pool = in_period(wf, start, end)
    model = pool[pool["go"]]
    v1 = in_period(wf_v1, start, end) if wf_v1 is not None else pool.iloc[0:0]
    v1 = v1[v1["go"]]

    members = {}
    for d in sessions:
        for s in survivorship.members_on(table, d):
            members.setdefault(s, set()).add(pd.to_datetime(d).date())
    rnd = random_entries(members, n=min(20000, max(2000, len(pool))), seed=3)

    rows = {
        "Random entry": summarize(rnd, sessions),
        "Plain ORB (break only)": summarize(in_period(plain, start, end), sessions),
        "ORBITAL rules (no model)": summarize(pool, sessions),
        V1: summarize(v1, sessions, n_trials=C.N_CONFIGS_TRIED, costs=C.COST_SENSITIVITY),
        V2: summarize(model, sessions, n_trials=C.N_CONFIGS_TRIED + 1, costs=C.COST_SENSITIVITY),
    }
    perm = {V1: permutation_vs_random(v1, pool) if len(v1) else (np.nan, np.nan),
            V2: permutation_vs_random(model, pool) if len(model) else (np.nan, np.nan)}
    for k in (V1, V2):
        rows[k]["status"] = STATUS.get((name, k), "")
        rows[k]["random_selection_mean"], rows[k]["selection_p_value"] = perm[k]
    rand_mean, p_val = perm[V2]

    monthly = (model.groupby("month")["pnl_%"].agg(["count", "mean", "sum"]).reset_index()
               .to_dict("records") if len(model) else [])

    block = {"start": start.isoformat(), "end": end.isoformat(), "sessions": len(sessions),
             "strategies": rows, "random_selection_mean": rand_mean,
             "selection_p_value": p_val, "monthly": monthly}

    title = name.replace("_", " ").title()
    md = [f"## {title} — {start} → {end} ({len(sessions)} sessions)", "",
          "| Strategy | Trades | /day | Win% | Mean/trade | 95% CI (day bootstrap) | "
          "Median | PF | Sharpe | Max DD | Months + |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for label in STRATEGY_ORDER:
        s = rows[label]
        if not s.get("trades"):
            md.append(f"| {label} | 0 | — | — | — | — | — | — | — | — | — |")
            continue
        md.append(
            f"| {label} | {s['trades']:,} | {s['per_day']:.2f} | {s['win_rate']:.1f}% | "
            f"{pct(s['mean'])} | [{pct(s['ci_low'])}, {pct(s['ci_high'])}] | "
            f"{pct(s['median'])} | {s['profit_factor']:.2f} | {s['sharpe']:.2f} | "
            f"{s['max_dd']:.1f}% | {s['months_positive']} |")

    md.append("")
    selected = {V1: v1, V2: model}
    for k in (V1, V2):
        m = rows[k]
        if not m.get("trades"):
            md.append(f"- **{k}** — no trades in this period.")
            continue
        if m.get("best_month") and len(sessions) > 40:
            bm = m["best_month"]
            rest_sel = selected[k][selected[k]["date"].str[:7] != bm]
            rest_pool = pool[pool["date"].str[:7] != bm]
            _, p_rest = permutation_vs_random(rest_sel, rest_pool) if len(rest_sel) else (np.nan, np.nan)
            m["p_ex_best_month"] = p_rest
            m["pool_ex_best_month"] = float(rest_pool["pnl_%"].mean())
        dsr = m.get("deflated_sharpe", np.nan)
        md.append(f"- **{k}** — *{m['status']}*. Random picks from the same daily signal pool, "
                  f"matched to its daily count, average {pct(m['random_selection_mean'])} vs its "
                  f"{pct(m['mean'])} (permutation p = {m['selection_p_value']:.3f}). Deflated Sharpe "
                  f"{'n/a' if np.isnan(dsr) else f'{dsr:.2f}'}. After costs: "
                  + ", ".join(f"{c:.2f}% → {pct(m['mean'] - c)}" for c in C.COST_SENSITIVITY if c) + ".")
        if "p_ex_best_month" in m:
            md.append(f"  - *Concentration:* its best month ({m['best_month']}) is "
                      f"{m['best_month_share']:.0f}% of its total P&L. Without that month it averages "
                      f"{pct(m['mean_ex_best_month'])} vs the pool's {pct(m['pool_ex_best_month'])} "
                      f"(permutation p = {m['p_ex_best_month']:.3f}).")
    md.append("")
    return block, md


def main():
    wf = pd.read_csv(R / "walkforward.csv")
    v1_path = R / "walkforward_hit_1_5r.csv"
    wf_v1 = pd.read_csv(v1_path) if v1_path.exists() else None
    plain = pd.read_csv(R / "signals_plain_orb_results.csv")
    table = survivorship.load_membership()

    import walk_forward
    summary = {"config_sha256": walk_forward.config_hash(), "periods": {}}
    md = ["# Results", "",
          "Generated by `python report.py`. Every strategy uses the same exit rule "
          "(`exits.py`: stop 1.0×ORB, trail 1.0×ORB, no target, flat by 15:15) and the "
          "point-in-time Nifty 200 universe. Model results are walk-forward: each month is "
          "scored by a model trained only on earlier months. **P&L is gross of costs**; the "
          "cost line subtracts a flat round-trip cost for sensitivity only.", "",
          "**Reading guide.** v1 is the pre-registered model; its clean test (2022-01 → 2023-08) "
          "is the headline result and it failed. v2 changes only the label and was chosen after "
          "seeing that failure, so its 2022-23 numbers are post-hoc; its own clean test is every "
          "session from 2026-09-23 (see docs/PREREGISTRATION.md).", ""]

    for name, (start, end) in PERIODS.items():
        block, lines = period_report(name, start, end, wf, wf_v1, plain, table)
        if block:
            summary["periods"][name] = block
            md += lines

    (BASE_DIR / "docs").mkdir(exist_ok=True)
    results = BASE_DIR / "docs" / "RESULTS.md"
    old = results.read_text() if results.exists() else ""
    drift = (old[old.index("<!-- drift:start -->"):old.index("<!-- drift:end -->") + len("<!-- drift:end -->")]
             if "<!-- drift:start -->" in old and "<!-- drift:end -->" in old else "")
    results.write_text("\n".join(md) + ("\n\n" + drift + "\n" if drift else ""))   # keep drift.py's live section
    (R / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    # aggregate numbers only — published with the repo for the deployed dashboard
    (BASE_DIR / "docs" / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print("\n".join(md))


if __name__ == "__main__":
    main()
