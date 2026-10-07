"""
exits.py — the one exit simulator (stop / trail / optional target / forced exit).

Used by the backtest, the walk-forward evaluation, the baselines and the
paper test, so every result in the project is scored by the same rule.
Conventions:
  * multiples are of the opening-range width (ORB = ORH - ORL);
  * the stop is checked before anything else in a candle (pessimistic when a
    single candle touches both the stop and a better price);
  * the trail only moves after the candle is survived;
  * the position is closed at the close of the first candle stamped at or
    after FORCE_EXIT_TIME.

The exit rule is named and versioned (CURRENT_RULE, "exit_v1"): every published
number and the live bot use it. A trailing-stop grace period (exit_v2_grace5 /
exit_v2_grace10) was tested and tracked live side by side, then retired on
2026-10-07 without being adopted (docs/EXIT_GRACE.md); its code is in git history
(commit 890d966), not here.
"""

import strategy_config as C

CURRENT_RULE = "exit_v1"


def simulate(high, low, close, times, entry, orh, orl, direction,
             stop_mult=C.STOP_ORB_MULT, target_mult=C.TARGET_ORB_MULT,
             trail_mult=C.TRAIL_ORB_MULT, force_exit=C.FORCE_EXIT_TIME, state=None):
    """Return (pnl_pct, exit_reason, exit_index) or None if ORB width is zero.

    high/low/close/times: sequences for the candles FROM entry onwards.
    "LAST" means the candles ran out before an exit — for a live position,
    still open.

    state: optional dict, filled with the stop in force after the last candle
    processed ("stop"), the best price so far ("best") and the target
    ("target", None without one) — the live tracker's view of the same walk.
    It never changes the result.
    """
    orb = orh - orl
    if orb <= 0 or len(close) == 0:
        return None

    long = str(direction).upper() == "BUY"
    sign = 1.0 if long else -1.0

    stop = entry - sign * stop_mult * orb
    target = entry + sign * target_mult * orb if target_mult is not None else None
    best = entry
    reason = "STOP"

    def done(result):
        if state is not None:
            state.update(stop=stop, best=best, target=target)
        return result

    def pnl(px):
        return sign * (px - entry) / entry * 100

    for i in range(len(close)):
        hi, lo, cl = high[i], low[i], close[i]

        if (long and lo <= stop) or (not long and hi >= stop):
            return done((pnl(stop), reason, i))

        if target is not None and ((long and hi >= target) or (not long and lo <= target)):
            return done((pnl(target), "TARGET", i))

        if trail_mult is not None:
            best = max(best, hi) if long else min(best, lo)
            trail = best - sign * trail_mult * orb
            new_stop = max(stop, trail) if long else min(stop, trail)
            if new_stop != stop:
                reason = "TRAIL"
            stop = new_stop

        if times[i] >= force_exit:
            return done((pnl(cl), "TIME", i))

    return done((pnl(close[-1]), "LAST", len(close) - 1))


def simulate_day(day_candles, entry_time, entry, orh, orl, direction, **kw):
    """Convenience wrapper over one day's IST-indexed candles."""
    after = day_candles[day_candles.index.time >= entry_time]
    if after.empty:
        return None
    return simulate(after["High"].to_numpy(float), after["Low"].to_numpy(float),
                    after["Close"].to_numpy(float), [t.time() for t in after.index],
                    entry, orh, orl, direction, **kw)


def levels(entry, orh, orl, direction):
    """Initial stop and trail distance, for the live Telegram message."""
    orb = orh - orl
    sign = 1.0 if str(direction).upper() == "BUY" else -1.0
    return {
        "stop": round(entry - sign * C.STOP_ORB_MULT * orb, 2),
        "trail_distance": round(C.TRAIL_ORB_MULT * orb, 2),
        "target": (round(entry + sign * C.TARGET_ORB_MULT * orb, 2)
                   if C.TARGET_ORB_MULT is not None else None),
        "force_exit": C.FORCE_EXIT_TIME.strftime("%H:%M"),
    }
