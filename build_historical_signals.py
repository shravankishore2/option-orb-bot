"""
Historical ORB signal builder — build_historical_signals.py
-----------------------------------------------------------
Replays the LIVE entry rules over years of Dhan history.

For every symbol x every trading day it:
  1. rebuilds the 9:20-9:35 opening range from 5-minute candles (all three
     candles must exist — a partial range is rejected, same as live),
  2. takes the previous COMPLETED session's H/L/C for the pivot and % filters,
  3. steps forward candle by candle like the live bot, firing the FIRST BUY and
     FIRST SELL of the day,
  4. scores each signal with the one exit rule in exits.py.

The rules are imported (signal_generator.evaluate), never reimplemented, and
live_engine.py calls replay_day() itself — so backtest and live cannot differ.

Rule sets:
  orbital    the live rules: ORB break + 1.8% from prev close + R1/S1 pivot
  plain_orb  baseline: ORB break only, no other filter
  orbital_nomove  research (v3 candidate): orbital without the 1.8% condition

Usage:
    python build_historical_signals.py --start 2021-01-01 --end 2026-09-22 \
        --universe pit --rules orbital --out data/research/signals_orbital.csv
"""

import argparse
import datetime as dt
import time as _time

import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C
from fetch_symbols import get_symbols
from signal_generator import evaluate

INTERVAL = pd.Timedelta(minutes=C.CANDLE_MINUTES)

SIGNAL_COLUMNS = ["date", "time", "symbol", "direction",
                  "entry_price", "ORH", "ORL", "prev_close"]
RESULT_COLUMNS = SIGNAL_COLUMNS + ["exit_reason", "pnl_%"]


def opening_range(day_candles):
    """(open, ORH, ORL) or None unless every opening-range candle is present."""
    t = day_candles.index.time
    win = day_candles[(t >= C.OR_START) & (t < C.OR_END)]
    if len(win) < C.OR_REQUIRED_CANDLES:
        return None
    return float(win.iloc[0]["Open"]), float(win["High"].max()), float(win["Low"].min())


def pivots(prev_high, prev_low, prev_close):
    p = (prev_high + prev_low + prev_close) / 3.0
    rng = prev_high - prev_low
    return p, p + C.PIVOT_FIB * rng, p - C.PIVOT_FIB * rng


def plain_orb(close, orh, orl):
    if close >= orh * (1 + C.BREAKOUT_BUFFER):
        return "BUY"
    if close <= orl * (1 - C.BREAKOUT_BUFFER):
        return "SELL"
    return None


def replay_day(symbol, day, day_candles, prev_high, prev_low, prev_close, rules="orbital"):
    """First BUY and first SELL of the session. Returns a list of signal dicts.

    Only COMPLETED candles must be passed in. A candle stamped T is observed at
    T+5min, which becomes the signal's entry time.
    """
    rng = opening_range(day_candles)
    if rng is None:
        return []

    _, orh, orl = rng
    _, r1, s1 = pivots(prev_high, prev_low, prev_close)

    fired = {}
    scan = day_candles[day_candles.index.time >= C.OR_END]

    for stamp, candle in zip(scan.index, scan["Close"].to_numpy(float)):
        observed_at = stamp + INTERVAL
        if observed_at.time() > C.LAST_ENTRY_TIME:
            break

        if rules == "orbital":
            direction = evaluate(candle, orh, orl, prev_close, r1, s1)
        elif rules == "orbital_nomove":           # research: no ±1.8%-from-previous-close condition
            direction = evaluate(candle, orh, orl, prev_close, r1, s1, prev_move=None)
        else:
            direction = plain_orb(candle, orh, orl)

        if direction is None or direction in fired:
            continue

        fired[direction] = {
            "date": day.isoformat(),
            "time": observed_at.strftime("%H:%M:%S"),
            "symbol": symbol,
            "direction": direction,
            "entry_price": round(float(candle), 2),
            "ORH": round(orh, 2),
            "ORL": round(orl, 2),
            "prev_close": round(prev_close, 2),
        }
        if len(fired) == 2:
            break

    return list(fired.values())


def score(signal, day_candles):
    entry_time = dt.datetime.strptime(signal["time"], "%H:%M:%S").time()
    out = exits.simulate_day(day_candles, entry_time, float(signal["entry_price"]),
                             float(signal["ORH"]), float(signal["ORL"]), signal["direction"])
    if out is None:
        return None
    pnl, reason, _ = out
    return {**signal, "exit_reason": reason, "pnl_%": round(pnl, 4)}


def prev_session(daily, day):
    """(high, low, close) of the last completed session strictly BEFORE `day`.

    Looked up by date, not by row position: Dhan's daily table never contains
    the running session, so "the row before today's row" doesn't exist live.
    """
    prior = daily[daily.index.date < day]
    if prior.empty:
        return None
    p = prior.iloc[-1]
    return float(p["High"]), float(p["Low"]), float(p["Close"])


def process_symbol(symbol, start, end, rules="orbital", member_days=None, refresh=False):
    candles = history_cache.get_candles(symbol, start, end, interval=dhan.INTERVAL_5M,
                                        refresh=refresh)
    if candles.empty:
        return [], []

    daily = history_cache.get_daily_candles(symbol, start - dt.timedelta(days=15), end,
                                            refresh=refresh)
    if daily.empty:
        return [], []

    signals, results = [], []

    for day, g in candles.groupby(candles.index.date):
        if member_days is not None and day not in member_days:
            continue
        prev = prev_session(daily, day)
        if prev is None:
            continue
        ph, pl, pc = prev
        if pc <= 0 or ph <= pl:
            continue

        for sig in replay_day(symbol, day, g, ph, pl, pc, rules=rules):
            signals.append(sig)
            r = score(sig, g)
            if r:
                results.append(r)

    return signals, results


def universe(kind, start, end):
    """[(symbol, set_of_member_days or None)]."""
    if kind == "current":
        return [(s, None) for s in get_symbols()]

    import survivorship
    table = survivorship.load_membership()
    days = pd.bdate_range(start, end).date
    members = {d: survivorship.members_on(table, d) for d in days}
    out = {}
    for d, syms in members.items():
        for s in syms:
            out.setdefault(s, set()).add(d)
    return sorted(out.items())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=C.HISTORY_START.isoformat())
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--universe", choices=["current", "pit"], default="pit")
    ap.add_argument("--rules", choices=["orbital", "plain_orb", "orbital_nomove"], default="orbital")
    ap.add_argument("--out", default="data/research/signals_orbital.csv",
                    help="signals file; results go next to it with _results suffix")
    ap.add_argument("--symbols", default=None, help="comma-separated subset")
    ap.add_argument("--refresh", action="store_true")
    a = ap.parse_args()

    start, end = pd.to_datetime(a.start).date(), pd.to_datetime(a.end).date()
    targets = universe(a.universe, start, end)
    if a.symbols:
        keep = {s.strip().upper() for s in a.symbols.split(",")}
        targets = [t for t in targets if t[0] in keep]

    print(f"🕰️  {a.rules} rules, {a.universe} universe, {start} → {end}, {len(targets)} symbols",
          flush=True)

    all_s, all_r, t0 = [], [], _time.time()
    for i, (sym, days) in enumerate(targets, 1):
        try:
            s, r = process_symbol(sym, start, end, rules=a.rules, member_days=days,
                                  refresh=a.refresh)
        except dhan.DhanError as e:
            print(f"⚠️ {sym}: {e}", flush=True)
            continue
        all_s += s
        all_r += r
        if i % 25 == 0 or i == len(targets):
            print(f"[{i:>3}/{len(targets)}] {len(all_s):>7,} signals  "
                  f"{(_time.time()-t0)/60:.1f}m", flush=True)

    out = a.out
    res_out = out.replace(".csv", "_results.csv")

    import os
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    pd.DataFrame(all_s, columns=SIGNAL_COLUMNS).sort_values(["date", "time", "symbol"]).to_csv(out, index=False)
    pd.DataFrame(all_r, columns=RESULT_COLUMNS).sort_values(["date", "time", "symbol"]).to_csv(res_out, index=False)
    print(f"💾 {out} ({len(all_s):,})  {res_out} ({len(all_r):,})")


if __name__ == "__main__":
    main()
