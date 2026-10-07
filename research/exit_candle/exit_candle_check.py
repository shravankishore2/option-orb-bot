"""
exit_candle_check.py — the missing 15:15 candle (docs/EXIT_CANDLE.md). Investigation only.

From 2026-08-03 NSE runs a Closing Auction Session for F&O stocks: continuous trading ends at
15:15, so Dhan has no 15:15-15:25 candles for them and exits.simulate's forced exit (the 15:15
candle's close) falls back to the last candle there is, 15:10 ("LAST"). This re-walks every
walk-forward GO trade (data/research/walkforward.csv) on the cached candles with exits.simulate_day
and reports: exits by type and candle, before vs from 2026-08-03; trades that exit on the very
first candle after entry (entry at 15:10, exit at the 15:10 candle's close); and the per-trade
means. With a shadow log (copied from the VM, read-only) it does the same for live signals.

    ORBITAL_OFFLINE=1 python research/exit_candle/exit_candle_check.py [SHADOW_DIR]
"""

import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))   # repo root: the shared modules live there

import datetime as dt
import os

os.environ.setdefault("ORBITAL_OFFLINE", "1")

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C

CAS_START = "2026-08-03"
WF = "data/research/walkforward.csv"


def _m(t):
    return t.hour * 60 + t.minute


def rewalk(trades):
    """exit candle stamp, candles held (exit index + 1), reason, P&L for each trade."""
    out = {k: [None] * len(trades) for k in ("exit_stamp", "held", "reason_chk", "pnl_chk", "last_stamp")}
    for sym, idx in trades.groupby("symbol").groups.items():
        c = history_cache._read_cache(history_cache._cache_path(sym, "5m"))
        if c is None or c.empty:
            continue
        c = dhan.regular_session(c)
        by_day = {d: g for d, g in c.groupby(c.index.date)}
        for i in idx:
            r, k = trades.loc[i], trades.index.get_loc(i)
            g = by_day.get(dt.date.fromisoformat(r["date"]))
            if g is None:
                continue
            t = dt.time.fromisoformat(r["time"])
            res = exits.simulate_day(g, t, float(r["entry_price"]), float(r["orh"]), float(r["orl"]), r["direction"])
            if res is None:
                continue
            after = g[g.index.time >= t]
            out["exit_stamp"][k] = after.index[res[2]].strftime("%H:%M")
            out["held"][k] = res[2] + 1
            out["reason_chk"][k], out["pnl_chk"][k] = res[1], round(res[0], 4)
            out["last_stamp"][k] = g.index[-1].strftime("%H:%M")
    for k, v in out.items():
        trades[k] = v
    return trades


def summary(df, label):
    x = df["pnl_%"]
    return (f"{label}: n={len(df):,} mean={x.mean():+.3f}% after0.05={x.mean() - 0.05:+.3f}% "
            f"hit={(x > 0).mean() * 100:.0f}%") if len(df) else f"{label}: n=0"


def historical():
    wf = pd.read_csv(WF, dtype={"date": str, "time": str})
    go = wf[wf["go"].astype(str) == "True"].reset_index(drop=True)
    go = rewalk(go)
    bad = int((np.abs(go["pnl_chk"].astype(float) - go["pnl_%"]) > 1e-3).sum())
    print(f"walk-forward GO trades {len(go):,} ({go['date'].min()} → {go['date'].max()}); P&L reproduced except {bad}")
    go["period"] = np.where(go["date"] >= CAS_START, "from 3 Aug", "before 3 Aug")
    go["exit"] = go["exit_reason"] + "@" + go["exit_stamp"].fillna("?")
    print("\nexits by type and candle, per period (GO trades):")
    print(go.pivot_table(index="exit", columns="period", values="pnl_%", aggfunc="count", fill_value=0).to_string())
    print("\nper-trade mean by exit type x period (GO trades):")
    for (p, e), g in go[go["exit_reason"].isin(["TIME", "LAST"])].groupby(["period", "exit"]):
        print("  ", summary(g, f"{p:>13} {e}"))
    for p, g in go.groupby("period"):
        print("  ", summary(g, f"{p:>13} all GO"))
    one = go[(go["exit_reason"] == "LAST") & (go["exit_stamp"] == "15:10") & (go["held"] == 1)]
    last1510 = go[(go["exit_reason"] == "LAST") & (go["exit_stamp"] == "15:10")]
    print(f"\nLAST at 15:10: {len(last1510)}; of which exit on the first candle after entry (entry 15:10, "
          f"held 1 candle): {len(one)}; entry times: {last1510['time'].value_counts().to_dict()}")
    print("  ", summary(one, "first-candle exits"))
    print("  ", summary(last1510, "all LAST@15:10"))
    late = go[go["time"] >= "15:00:00"]
    print("\nlate slice (entry >= 15:00) by period and exit:")
    for (p, e), g in late.groupby(["period", "exit"]):
        print("  ", summary(g, f"{p:>13} {e}"))
    return go


def live(shadow_dir):
    s = pd.read_csv(os.path.join(shadow_dir, "shadow_signals.csv"), dtype=str).drop_duplicates("signal_id")
    o = pd.read_csv(os.path.join(shadow_dir, "shadow_outcomes.csv"), dtype=str).drop_duplicates("signal_id")
    df = s.merge(o[["signal_id", "exit_reason", "exit_candle", "exit_time", "pnl_pct"]], on="signal_id")
    df["pnl_%"] = pd.to_numeric(df["pnl_pct"])
    df = df[(df["date"] >= CAS_START) & df["model_go"].isin(["True", "true", "1"])]
    df["exit"] = df["exit_reason"] + "@" + df["exit_candle"].str[:5]
    df["first_candle"] = (df["exit_reason"] == "LAST") & (df["exit_candle"].str[:5] == "15:10") & \
                         (df["time"].str[:5] == "15:10")
    print(f"\nshadow log, champion GO since {CAS_START}: by source and exit")
    print(df.pivot_table(index="exit", columns="source", values="pnl_%", aggfunc="count", fill_value=0).to_string())
    for src, g in [("live", df[df["source"] == "live"]), ("live+replay", df)]:
        print("  ", summary(g, f"{src:>11} all GO"))
        print("  ", summary(g[~g["first_candle"]], f"{src:>11} without first-candle exits"))
        print("  ", summary(g[g["first_candle"]], f"{src:>11} first-candle exits only"))
    al = s.merge(o[["signal_id", "exit_reason", "exit_candle"]], on="signal_id")
    al = al[al["date"] >= "2026-09-23"]
    fe = al[al["exit_reason"].isin(["TIME", "LAST"])]
    print(f"\nall shadow signals since 23 Sep reaching the forced exit: {len(fe)}; "
          f"at 15:10 (LAST) {int((fe['exit_reason'] == 'LAST').sum())}, at 15:15 (TIME) {int((fe['exit_reason'] == 'TIME').sum())}")
    return df


if __name__ == "__main__":
    historical()
    if len(_sys.argv) > 1:
        live(_sys.argv[1])
