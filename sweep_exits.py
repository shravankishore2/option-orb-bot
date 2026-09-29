"""
Exit-rule sweep — sweep_exits.py
---------------------------------
The diagnostic on 50k replayed trades showed mean MFE of only +0.28%: trades
are being stopped out at noise before the move develops. Entry selection can't
fix that — the exit rule can.

This walks every historical signal's post-entry path ONCE (from the local
candle cache, no API calls) and scores several exit rules on the same paths,
so the comparison is apples-to-apples.

Usage:
    python sweep_exits.py                 # all signals
    python sweep_exits.py --before 11:00  # only signals fired before 11:00
"""

import argparse
import datetime as dt
from collections import defaultdict

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C

SIGNALS_FILE = "data/research/v1/historical_orb_signals.csv"
FORCE_EXIT = C.FORCE_EXIT_TIME


def load_paths(signals, quiet=False):
    """For each signal, pull the post-entry OHLC path from the candle cache."""
    paths = []

    by_symbol = defaultdict(list)
    for row in signals.itertuples():
        by_symbol[row.symbol].append(row)

    for i, (symbol, rows) in enumerate(sorted(by_symbol.items()), 1):
        dates = [pd.to_datetime(r.date).date() for r in rows]

        candles = history_cache.get_candles(
            symbol, min(dates), max(dates), interval=dhan.INTERVAL_5M
        )

        if candles.empty:
            continue

        by_day = {d: g for d, g in candles.groupby(candles.index.date)}

        for row in rows:
            day = pd.to_datetime(row.date).date()
            day_candles = by_day.get(day)

            if day_candles is None:
                continue

            entry_time = dt.datetime.strptime(row.time, "%H:%M:%S").time()
            after = day_candles[day_candles.index.time >= entry_time]

            if after.empty:
                continue

            paths.append({
                "date": row.date,
                "symbol": symbol,
                "direction": row.direction,
                "entry": float(row.entry_price),
                "orh": float(row.ORH),
                "orl": float(row.ORL),
                "high": after["High"].to_numpy(dtype=float),
                "low": after["Low"].to_numpy(dtype=float),
                "close": after["Close"].to_numpy(dtype=float),
                "times": np.array([t.time() for t in after.index]),
            })

        if not quiet and i % 25 == 0:
            print(f"   … loaded paths for {i}/{len(by_symbol)} symbols "
                  f"({len(paths):,} trades)")

    return paths


def simulate(path, stop_mult, target_mult, trail_mult):
    """Realised PnL % for one exit rule on one path (delegates to exits.py)."""
    out = exits.simulate(path["high"], path["low"], path["close"], path["times"],
                         path["entry"], path["orh"], path["orl"], path["direction"],
                         stop_mult, target_mult, trail_mult, FORCE_EXIT)
    return None if out is None else out[0]


RULES = [
    # (label, stop_mult, target_mult, trail_mult)
    ("CURRENT: trail 0.25, tgt 0.75",   0.60, 0.75, 0.25),
    ("trail 0.50, tgt 1.0",             0.60, 1.00, 0.50),
    ("trail 0.75, tgt 1.5",             0.60, 1.50, 0.75),
    ("trail 1.00, tgt 2.0",             0.60, 2.00, 1.00),
    ("fixed 0.6 stop, tgt 1.0 (no trail)", 0.60, 1.00, None),
    ("fixed 0.6 stop, tgt 1.5 (no trail)", 0.60, 1.50, None),
    ("fixed 0.6 stop, tgt 2.0 (no trail)", 0.60, 2.00, None),
    ("fixed 1.0 stop, tgt 2.0 (no trail)", 1.00, 2.00, None),
    ("RIDE: 0.6 stop, no target, EOD",  0.60, None, None),
    ("RIDE: 1.0 stop, no target, EOD",  1.00, None, None),
    ("RIDE: 1.0 stop, trail 1.0, EOD",  1.00, None, 1.00),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=str, default=None,
                        help="only signals fired before this time, e.g. 11:00")
    parser.add_argument("--costs", type=float, default=0.05,
                        help="round-trip cost in %% to subtract (default 0.05)")
    args = parser.parse_args()

    signals = pd.read_csv(SIGNALS_FILE)

    if args.before:
        cutoff = dt.datetime.strptime(args.before, "%H:%M").time()
        keep = pd.to_datetime(signals["time"], format="%H:%M:%S").dt.time < cutoff
        signals = signals[keep]
        print(f"⏰ Filtered to signals before {args.before}: {len(signals):,}")

    print(f"📥 Loading post-entry paths for {len(signals):,} signals from cache...")
    paths = load_paths(signals)
    print(f"✅ {len(paths):,} paths ready\n")

    ndays = signals["date"].nunique()

    print(f"{'exit rule':<38} {'win%':>6} {'gross':>8} {'net':>8} {'exp/day':>9}")
    print("-" * 74)

    for label, stop_mult, target_mult, trail_mult in RULES:
        pnls = [simulate(p, stop_mult, target_mult, trail_mult) for p in paths]
        pnls = np.array([x for x in pnls if x is not None])

        if not len(pnls):
            continue

        gross = pnls.mean()
        net = gross - args.costs
        per_day = net * len(pnls) / ndays

        print(f"{label:<38} {(pnls > 0).mean()*100:>5.1f}% "
              f"{gross:>+7.3f}% {net:>+7.3f}% {per_day:>+8.2f}%")

    print("-" * 74)
    print(f"net = gross - {args.costs}% round-trip costs; "
          f"exp/day = net x trades / {ndays} sessions")


if __name__ == "__main__":
    main()
