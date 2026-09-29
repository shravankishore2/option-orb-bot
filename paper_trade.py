"""
Paper test of the LIVE bot — paper_trade.py
-------------------------------------------
Replays past sessions through live_engine.LiveEngine exactly as the live bot
runs: a cycle every 5 minutes from 09:40 to 15:15, seeing only candles that
had completed by then, scoring with the model that would have been live that
month (models/walkforward/YYYY-MM.pkl). GO trades are then scored with the
same exit rule.

It also checks the live code against the batch walk-forward: same signals,
same scores, same decisions — or it says exactly where they differ.

    python paper_trade.py --start 2026-09-05 --end 2026-09-22
"""

import argparse
import datetime as dt
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

import exits
import survivorship
from live_engine import CacheSource, LiveEngine
from mine_features import sector_map

BASE_DIR = Path(__file__).resolve().parent
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
OUT = BASE_DIR / "data" / "research" / "paper_trades.csv"


def cycle_times(day):
    t = dt.datetime.combine(day, dt.time(9, 40), tzinfo=IST)
    end = dt.datetime.combine(day, dt.time(15, 15), tzinfo=IST)
    while t <= end:
        yield t
        t += dt.timedelta(minutes=5)


def run(start, end):
    table = survivorship.load_membership()
    sectors = sector_map()
    source = CacheSource(start - dt.timedelta(days=70), end)

    nifty = source._frame("NIFTY_INDEX", "5m")
    days = sorted({d for d in nifty.index.date if start <= d <= end})

    decisions = []
    for day in days:
        bundle_path = BASE_DIR / "models" / "walkforward" / f"{day:%Y-%m}.pkl"
        if not bundle_path.exists():
            print(f"  {day}: no walk-forward model for {day:%Y-%m}, skipped")
            continue
        b = pickle.loads(bundle_path.read_bytes())

        engine = LiveEngine(source, b["model"], b["features"], b["threshold"],
                            sorted(survivorship.members_on(table, day)), sectors)
        engine.prepare(day)

        day_rows = []
        for now in cycle_times(day):
            day_rows += engine.cycle(now)

        for d in day_rows:
            if d["decision"] == "GO":
                g = source.today(d["symbol"], day, dt.datetime.combine(day, dt.time(23, 59), tzinfo=IST))
                t = dt.datetime.strptime(d["time"], "%H:%M:%S").time()
                out = exits.simulate_day(g, t, float(d["entry_price"]), float(d["ORH"]),
                                         float(d["ORL"]), d["direction"])
                d["pnl_%"] = out[0] if out else np.nan
                d["exit_reason"] = out[1] if out else ""
        decisions += day_rows

        go = [d for d in day_rows if d["decision"] == "GO"]
        print(f"  {day}  {len(engine.prev):>3} symbols  {len(day_rows):>3} signals  "
              f"GO {len(go)}  " + "  ".join(f"{d['symbol']} {d['direction']} {d['pnl_%']:+.2f}%" for d in go),
              flush=True)

    df = pd.DataFrame(decisions)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    return df


def compare(paper):
    """Paper (live code, cycle by cycle) vs batch walk-forward."""
    wf = pd.read_csv(BASE_DIR / "data" / "research" / "walkforward.csv")
    days = paper["date"].unique()
    wf = wf[wf["date"].isin(days)]

    k = ["date", "symbol", "direction"]
    m = paper.merge(wf, on=k, how="outer", suffixes=("_live", "_batch"), indicator=True)
    both = m[m["_merge"] == "both"]

    print("\n🔬 LIVE CODE vs BATCH BACKTEST")
    print(f"   signals: live {len(paper)}, batch {len(wf)}, in both {len(both)}, "
          f"live-only {(m['_merge'] == 'left_only').sum()}, batch-only {(m['_merge'] == 'right_only').sum()}")
    if len(both):
        same_time = (both["time_live"] == both["time_batch"]).mean() * 100
        score_gap = (both["score_live"] - both["score_batch"]).abs().max()
        live_go = both["decision"] == "GO"
        same_dec = (live_go == both["go"].astype(bool)).mean() * 100
        print(f"   same entry time {same_time:.1f}%   max score difference {score_gap:.2e}   "
              f"same GO/SKIP {same_dec:.1f}%")
    return m


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    a = ap.parse_args()
    s, e = pd.to_datetime(a.start).date(), pd.to_datetime(a.end).date()
    paper = run(s, e)
    if len(paper):
        compare(paper)
        go = paper[paper["decision"] == "GO"]
        print(f"\n📒 Paper result: {len(go)} GO trades, "
              f"mean {go['pnl_%'].mean():+.3f}%/trade, total {go['pnl_%'].sum():+.2f}%, "
              f"win {(go['pnl_%'] > 0).mean()*100:.1f}%" if len(go) else "\n📒 No GO trades.")
