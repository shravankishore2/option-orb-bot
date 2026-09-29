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
"""

import strategy_config as C


def simulate(high, low, close, times, entry, orh, orl, direction,
             stop_mult=C.STOP_ORB_MULT, target_mult=C.TARGET_ORB_MULT,
             trail_mult=C.TRAIL_ORB_MULT, force_exit=C.FORCE_EXIT_TIME):
    """Return (pnl_pct, exit_reason, exit_index) or None if ORB width is zero.

    high/low/close/times: sequences for the candles FROM entry onwards.
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

    def pnl(px):
        return sign * (px - entry) / entry * 100

    for i in range(len(close)):
        hi, lo, cl = high[i], low[i], close[i]

        if (long and lo <= stop) or (not long and hi >= stop):
            return pnl(stop), reason, i

        if target is not None and ((long and hi >= target) or (not long and lo <= target)):
            return pnl(target), "TARGET", i

        if trail_mult is not None:
            best = max(best, hi) if long else min(best, lo)
            trail = best - sign * trail_mult * orb
            new_stop = max(stop, trail) if long else min(stop, trail)
            if new_stop != stop:
                reason = "TRAIL"
            stop = new_stop

        if times[i] >= force_exit:
            return pnl(cl), "TIME", i

    return pnl(close[-1]), "LAST", len(close) - 1


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
        "force_exit": C.FORCE_EXIT_TIME.strftime("%H:%M"),
    }
