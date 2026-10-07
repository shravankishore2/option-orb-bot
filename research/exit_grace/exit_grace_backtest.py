"""
exit_grace_backtest.py — the trailing-stop grace period vs the current exit rule.

Concluded: not adopted. grace5 identical by construction, grace10 within noise.
Kept as the record of docs/EXIT_GRACE.md. The grace code was removed from exits.py
on 2026-10-07, so to re-run this, restore the version it used first:
    git show 890d966:exits.py > exits.py      # then git checkout exits.py afterwards

Post-hoc idea (2026-10-06), tested on the same walk-forward signals and decisions
as docs/RESULTS.md (data/research/walkforward.csv): every signal is re-scored with
exits.simulate_day on the same cached 5-minute candles under each rule, and the
current rule must reproduce every stored pnl_% exactly before anything is compared.

    ORBITAL_OFFLINE=1 python research/exit_grace/exit_grace_backtest.py           # writes docs/EXIT_GRACE.md

Rules: exit_v1 (current), exit_v2_grace5 and exit_v2_grace10 (registered in
exits.RULES; grace10 is tracked live side by side), plus a 15-minute grace as an
exploratory, unregistered variant.
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root: the shared modules live there

import datetime as dt
import os

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C

WF = "data/research/walkforward.csv"
OUT = "docs/EXIT_GRACE.md"
GRACES = {"exit_v1": 0, "exit_v2_grace5": 5, "exit_v2_grace10": 10, "grace15 (exploratory)": 15}
PERIODS = {"clean_backward": (str(C.CLEAN_BACKWARD[0]), str(C.CLEAN_BACKWARD[1])),
           "development": (str(C.DEV_START), str(C.DEV_END)),
           "clean_forward": (str(C.CLEAN_FORWARD[0]), str(C.CLEAN_FORWARD[1]))}
COSTS = (0.0, 0.05, 0.10)
BOOT = 2000


def rescore(wf):
    out = {name: np.full(len(wf), np.nan) for name in GRACES}
    reasons = {name: np.empty(len(wf), dtype=object) for name in GRACES}
    for sym, idx in wf.groupby("symbol").groups.items():
        c = history_cache._read_cache(history_cache._cache_path(sym, "5m"))
        if c is None or c.empty:
            continue
        c = dhan.regular_session(c)
        by_day = {d: g for d, g in c.groupby(c.index.date)}
        for i in idx:
            r = wf.loc[i]
            g = by_day.get(dt.date.fromisoformat(r["date"]))
            if g is None:
                continue
            t = dt.time.fromisoformat(r["time"])
            for name, grace in GRACES.items():
                res = exits.simulate_day(g, t, float(r["entry_price"]), float(r["orh"]), float(r["orl"]),
                                         r["direction"], trail_grace_min=grace)
                if res is not None:
                    out[name][wf.index.get_loc(i)] = round(res[0], 4)
                    reasons[name][wf.index.get_loc(i)] = res[1]
    for name in GRACES:
        wf[name], wf[name + "_reason"] = out[name], reasons[name]
    return wf


def boot_ci(days, diff, rng):
    """Day-block bootstrap 95% CI of the mean per-trade difference."""
    uniq = np.unique(days)
    pos = {d: np.flatnonzero(days == d) for d in uniq}
    means = []
    for _ in range(BOOT):
        pick = np.concatenate([pos[d] for d in rng.choice(uniq, len(uniq))])
        means.append(diff[pick].mean())
    return np.percentile(means, [2.5, 97.5])


def table(df, label, rng):
    rows = [f"### {label}", "",
            "| Rule | Trades | Mean/trade | after 0.05% | after 0.10% | Hit rate | Worst trade | "
            "Changed vs v1 | Better / worse | Δ mean vs v1 (95% CI, day bootstrap) |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    base = df["exit_v1"].to_numpy()
    for name in GRACES:
        x = df[name].to_numpy()
        d = x - base
        changed = np.abs(d) > 1e-9
        better, worse = d > 1e-9, d < -1e-9
        if name == "exit_v1":
            delta = "—"
        elif not changed.any():
            delta = "identical on every trade"
        else:
            lo, hi = boot_ci(df["date"].to_numpy(), d, rng)
            delta = f"{d.mean():+.4f}% [{lo:+.4f}, {hi:+.4f}]"
        rows.append(
            f"| {name} | {len(x):,} | {x.mean():+.3f}% | {x.mean() - 0.05:+.3f}% | {x.mean() - 0.10:+.3f}% | "
            f"{(x > 0).mean() * 100:.1f}% | {x.min():+.2f}% | {changed.sum():,} | "
            + (f"{better.sum():,} (+{d[better].sum():.1f} pts) / {worse.sum():,} ({d[worse].sum():.1f} pts)"
               if changed.any() else "0 / 0") + f" | {delta} |")
    return rows


def main():
    if "trail_grace_min" not in exits.simulate.__code__.co_varnames:
        raise SystemExit("the grace code was retired from exits.py; see this file's docstring to re-run it")
    os.environ.setdefault("ORBITAL_OFFLINE", "1")
    wf = pd.read_csv(WF, dtype={"date": str, "time": str})
    print(f"re-scoring {len(wf):,} walk-forward signals under {len(GRACES)} rules...", flush=True)
    wf = rescore(wf)
    missing = int(wf["exit_v1"].isna().sum())
    mismatch = int((~np.isclose(wf["exit_v1"], wf["pnl_%"], atol=1e-4) & wf["exit_v1"].notna()).sum())
    print(f"exit_v1 reproduces stored pnl_%: {len(wf) - missing - mismatch:,}/{len(wf):,} "
          f"(missing candles {missing}, mismatches {mismatch})")
    if mismatch:
        raise SystemExit("exit_v1 does not reproduce the published P&L; not comparing")
    wf = wf.dropna(subset=list(GRACES))
    rng = np.random.default_rng(0)
    md = ["# Trailing-stop grace period vs the current exit rule", "",
          f"Generated by `exit_grace_backtest.py` on {dt.date.today()}. **Post-hoc idea, tested on data that "
          "includes the design period** — treat it as exploratory, not as a clean test.", "",
          "Same walk-forward signals and model decisions as docs/RESULTS.md; every signal is re-scored on the "
          "same 5-minute candles by `exits.simulate_day`. `exit_v1` reproduces every published `pnl_%` "
          f"({len(wf):,} signals). P&L per trade, gross; the cost columns subtract a flat round trip. "
          "\"Better / worse\" counts trades whose P&L rose / fell under the variant, with the total change "
          "in percentage points.", "",
          "**Why exit_v2_grace5 changes nothing:** the trail only moves after a candle is survived, so on the "
          "first candle after entry (minutes 0-5) only the initial stop can ever trigger. A 5-minute grace is "
          "therefore the current rule; the first grace that changes anything is 10 minutes (the trail can't "
          "trigger on the second candle either).", ""]
    for period, (a, b) in PERIODS.items():
        p = wf[(wf["date"] >= a) & (wf["date"] <= b)]
        go = p[p["go"].astype(str) == "True"]
        md += table(go, f"{period} ({a} → {b}) — Model v2 GO trades", rng) + [""]
        md += table(p, f"{period} — all ORBITAL signals (no model)", rng) + [""]
    with open(OUT, "w") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
