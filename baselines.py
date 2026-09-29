"""
baselines.py — what the strategy has to beat.

  random      random member stock, random entry candle (09:40-15:10), random
              direction — same exit rule. The "no skill at all" line.
  plain_orb   every ORB break, no other filter        (build_historical_signals --rules plain_orb)
  orbital     every signal the live rules fire         (the model's input pool)
  selection   random picks from the orbital pool, matched to the model's daily
              count — isolates the model's choosing skill (stats.permutation_vs_random)
"""

import datetime as dt
from collections import defaultdict

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C

# Candle stamps whose close can be an entry: 09:35 (observed 09:40) .. 15:05 (observed 15:10).
ENTRY_STAMPS = [dt.time(h, m) for h in range(9, 16) for m in range(0, 60, 5)
                if dt.time(9, 35) <= dt.time(h, m) <= dt.time(15, 5)]


def random_entries(member_days, n, seed=3):
    """member_days: {symbol: set(days)}. Returns up to n random trades scored by exits.py."""
    rng = np.random.default_rng(seed)
    # sorted: set/dict order of strings varies between Python processes, and the
    # baseline must be identical on every run for a given seed
    pairs = sorted((s, d) for s, ds in member_days.items() for d in ds)
    if not pairs:
        return pd.DataFrame(columns=["date", "symbol", "direction", "time", "pnl_%"])
    pick = rng.choice(len(pairs), size=min(n, len(pairs)), replace=False)

    by_symbol = defaultdict(list)
    for i in pick:
        by_symbol[pairs[i][0]].append(pairs[i][1])

    rows = []
    for sym, days in by_symbol.items():
        c = history_cache._read_cache(history_cache._cache_path(sym, "5m"))
        if c is None or c.empty:
            continue
        c = dhan.regular_session(c)
        grouped = {d: g for d, g in c.groupby(c.index.date)}
        for day in days:
            g = grouped.get(day)
            if g is None or len(g) < 20:
                continue
            t = g.index.time
            win = g[(t >= C.OR_START) & (t < C.OR_END)]
            if len(win) < C.OR_REQUIRED_CANDLES:
                continue
            stamp = ENTRY_STAMPS[rng.integers(len(ENTRY_STAMPS))]
            bar = g[g.index.time == stamp]
            if bar.empty:
                continue
            entry_time = (dt.datetime.combine(day, stamp) + dt.timedelta(minutes=5)).time()
            direction = "BUY" if rng.random() < 0.5 else "SELL"
            out = exits.simulate_day(g, entry_time, float(bar["Close"].iloc[0]),
                                     float(win["High"].max()), float(win["Low"].min()), direction)
            if out:
                rows.append({"date": day.isoformat(), "symbol": sym, "direction": direction,
                             "time": entry_time.strftime("%H:%M:%S"), "pnl_%": out[0]})
    return pd.DataFrame(rows)
