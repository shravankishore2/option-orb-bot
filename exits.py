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

Exit rules are named and versioned (RULES). CURRENT_RULE is the one every
published number and the live bot use; another rule is only ever tracked side
by side (shadow.py) and replaces it after an explicit decision, never silently.
The rule parameters live here, not in strategy_config.py, so adding a variant
leaves the pre-registered config (and its hash) untouched.
"""

import datetime as dt

import strategy_config as C

# trail_grace_min: for this many minutes after entry the trailing stop can't
# trigger; the initial (hard) stop is active throughout. Measured from the entry
# candle's stamp, in whole candles: a candle can be trail-stopped only if it
# starts at least trail_grace_min after entry. Because the trail only moves after
# a candle is survived, the first candle can never meet a trailed stop anyway, so
# a 5-minute grace on 5-minute candles behaves exactly like exit_v1.
RULES = {
    "exit_v1": {"trail_grace_min": 0},            # every published result; the live rule
    "exit_v2_grace5": {"trail_grace_min": 5},     # post-hoc idea (2026-10-06): identical to exit_v1
    "exit_v2_grace10": {"trail_grace_min": 10},   # the smallest grace that changes anything
}
CURRENT_RULE = "exit_v1"
SHADOW_RULES = ("exit_v2_grace10",)               # tracked live beside CURRENT_RULE; never trains or decides
RULE_LABELS = {"exit_v1": "Current rule", "exit_v2_grace5": "5-min trail grace",
               "exit_v2_grace10": "10-min trail grace"}


def rule_params(rule):
    if rule not in RULES:
        raise KeyError(f"unknown exit rule {rule!r}; known: {sorted(RULES)}")
    return dict(RULES[rule])


def _minutes(t):
    return t.hour * 60 + t.minute if isinstance(t, dt.time) else int(t)


def simulate(high, low, close, times, entry, orh, orl, direction,
             stop_mult=C.STOP_ORB_MULT, target_mult=C.TARGET_ORB_MULT,
             trail_mult=C.TRAIL_ORB_MULT, force_exit=C.FORCE_EXIT_TIME, state=None,
             trail_grace_min=0, rule=None):
    """Return (pnl_pct, exit_reason, exit_index) or None if ORB width is zero.

    high/low/close/times: sequences for the candles FROM entry onwards.
    "LAST" means the candles ran out before an exit — for a live position,
    still open.

    state: optional dict, filled with the stop in force after the last candle
    processed ("stop"), the best price so far ("best") and the target
    ("target", None without one) — the live tracker's view of the same walk.
    It never changes the result.

    rule: a name from RULES (overrides trail_grace_min); None = the explicit
    arguments, whose defaults are exit_v1.
    """
    if rule is not None:
        trail_grace_min = rule_params(rule)["trail_grace_min"]
    orb = orh - orl
    if orb <= 0 or len(close) == 0:
        return None

    long = str(direction).upper() == "BUY"
    sign = 1.0 if long else -1.0

    stop = entry - sign * stop_mult * orb
    hard = stop                                   # the initial stop, active throughout
    target = entry + sign * target_mult * orb if target_mult is not None else None
    best = entry
    reason = "STOP"
    t0 = _minutes(times[0]) if trail_grace_min else 0

    def in_grace(i):
        return bool(trail_grace_min) and _minutes(times[i]) - t0 < trail_grace_min

    def done(result, in_force=None):
        if state is not None:
            state.update(stop=stop if in_force is None else in_force, best=best, target=target)
        return result

    def pnl(px):
        return sign * (px - entry) / entry * 100

    for i in range(len(close)):
        hi, lo, cl = high[i], low[i], close[i]

        if trail_grace_min and in_grace(i):       # only the hard stop can trigger
            if (long and lo <= hard) or (not long and hi >= hard):
                return done((pnl(hard), "STOP", i), in_force=hard)
        elif (long and lo <= stop) or (not long and hi >= stop):
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

    if trail_grace_min:                           # an open position: the stop the next candle meets
        nxt = _minutes(times[-1]) + C.CANDLE_MINUTES - t0 < trail_grace_min
        return done((pnl(close[-1]), "LAST", len(close) - 1), in_force=hard if nxt else None)
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
