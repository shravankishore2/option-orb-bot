"""The entry rules and the replay that fires them — including the no-lookahead contract."""

import datetime as dt

import pandas as pd

import build_historical_signals as B
import strategy_config as C
from signal_generator import evaluate
from conftest import make_day, make_daily

# prev H/L/C = 101 / 99 / 100  ->  pivot 100, R1 100.764, S1 99.236
PREV = (101.0, 99.0, 100.0)
R1, S1 = 100.764, 99.236


# ------------------------------------------------------------------ rules
def test_buy_needs_all_three_conditions():
    assert evaluate(102.0, 100.2, 99.8, 100.0, R1, S1) == "BUY"
    assert evaluate(101.7, 100.2, 99.8, 100.0, R1, S1) is None      # < +1.8% from prev close
    assert evaluate(102.0, 102.0, 99.8, 100.0, R1, S1) is None      # not above ORH x 1.001
    assert evaluate(102.0, 100.2, 99.8, 100.0, 102.5, S1) is None   # below R1


def test_sell_is_the_mirror():
    assert evaluate(98.0, 100.2, 99.8, 100.0, R1, S1) == "SELL"
    assert evaluate(98.3, 100.2, 99.8, 100.0, R1, S1) is None       # only -1.7%


def test_boundaries_are_inclusive():
    prev = 100.0
    at_move = prev * (1 + C.PREV_CLOSE_MOVE)                         # exactly +1.8%
    assert evaluate(at_move, 100.0, 99.0, prev, R1, S1) == "BUY"


def test_missing_prev_close_or_pivot_never_fires():
    assert evaluate(110.0, 100.0, 99.0, 0, R1, S1) is None
    assert evaluate(110.0, 100.0, 99.0, None, R1, S1) is None
    assert evaluate(110.0, 100.0, 99.0, 100.0, None, S1) is None


# ------------------------------------------------------------------ replay
def test_signal_time_is_the_candle_close_not_its_start(breakout_day, day):
    sigs = B.replay_day("X", day, breakout_day, *PREV)
    buy = [s for s in sigs if s["direction"] == "BUY"][0]
    # first close >= 101.8 is the 10:15 candle (close 102.0) -> known at 10:20
    assert buy["time"] == "10:20:00"
    assert buy["entry_price"] == 102.0


def test_only_the_first_signal_per_direction(breakout_day, day):
    sigs = B.replay_day("X", day, breakout_day, *PREV)
    assert [s["direction"] for s in sigs].count("BUY") == 1


def test_future_candles_cannot_change_a_signal(breakout_day, day):
    """No lookahead: rewriting everything after the signal changes nothing."""
    base = B.replay_day("X", day, breakout_day, *PREV)[0]
    tampered = breakout_day.copy()
    after = tampered.index.time >= dt.time(10, 20)
    tampered.loc[after, ["Open", "High", "Low", "Close"]] = 50.0
    again = B.replay_day("X", day, tampered, *PREV)[0]
    assert again == base


def test_partial_opening_range_is_rejected(breakout_day, day):
    """Live bug fix: a range missing its 09:30 candle must not trade."""
    missing = breakout_day[breakout_day.index.time != dt.time(9, 30)]
    assert B.opening_range(missing) is None
    assert B.replay_day("X", day, missing, *PREV) == []


def test_no_entries_after_last_entry_time(day):
    closes = [100.0] * 70 + [110.0] * 5                            # breakout only at 15:05+
    g = make_day(day, closes)
    sigs = B.replay_day("X", day, g, *PREV)
    assert all(s["time"] <= C.LAST_ENTRY_TIME.strftime("%H:%M:%S") for s in sigs)


def test_plain_orb_fires_without_the_filters(day):
    closes = [100.0] * 5 + [100.5] * 5                              # +0.5%: ORB break, no 1.8%
    g = make_day(day, closes)
    assert B.replay_day("X", day, g, *PREV, rules="orbital") == []
    assert B.replay_day("X", day, g, *PREV, rules="plain_orb")[0]["direction"] == "BUY"


# ------------------------------------------------------------------ previous session
def test_prev_session_is_strictly_before_the_day():
    daily = make_daily([dt.date(2026, 8, 28), dt.date(2026, 8, 31), dt.date(2026, 9, 1)])
    daily.loc[daily.index[1], ["High", "Low", "Close"]] = [105.0, 95.0, 102.0]
    assert B.prev_session(daily, dt.date(2026, 9, 1)) == (105.0, 95.0, 102.0)


def test_prev_session_works_when_today_is_not_in_the_table():
    """Live: Dhan's daily table never contains the running session."""
    daily = make_daily([dt.date(2026, 8, 28), dt.date(2026, 8, 31)])
    daily.loc[daily.index[1], ["High", "Low", "Close"]] = [105.0, 95.0, 102.0]
    assert B.prev_session(daily, dt.date(2026, 9, 1)) == (105.0, 95.0, 102.0)


def test_prev_session_none_without_history():
    daily = make_daily([dt.date(2026, 9, 1)])
    assert B.prev_session(daily, dt.date(2026, 9, 1)) is None


# --- research variant: no "moved 1.8% from the previous close" condition (v3 candidate) -----

def test_the_default_rule_still_requires_the_previous_close_move():
    from signal_generator import evaluate
    # breaks ORH and R1, but only +1.0% from the previous close
    assert evaluate(101.0, 100.0, 99.0, 100.0, 100.5, 98.0) is None
    assert evaluate(101.0, 100.0, 99.0, 100.0, 100.5, 98.0, prev_move=None) == "BUY"
    assert evaluate(102.0, 100.0, 99.0, 100.0, 100.5, 98.0) == "BUY"                 # +2%: both agree
    assert evaluate(98.9, 100.0, 99.0, 100.0, 100.5, 99.5, prev_move=None) == "SELL"
    assert evaluate(98.9, 100.0, 99.0, 100.0, 100.5, 99.5) is None


def test_nomove_signals_are_a_superset_of_the_live_rule(day):
    import numpy as np
    import build_historical_signals as B
    from conftest import make_day
    rng = np.random.default_rng(3)
    for _ in range(200):
        closes = list(100 + np.cumsum(rng.normal(0, 0.5, 75)))
        g = make_day(day, closes)
        live = {(s["direction"], s["time"]) for s in B.replay_day("X", day, g, 101.0, 98.5, 99.6)}
        new = {s["direction"]: s["time"] for s in B.replay_day("X", day, g, 101.0, 98.5, 99.6, rules="orbital_nomove")}
        for d, t in live:                       # every live signal fires, at the same time or earlier
            assert d in new and new[d] <= t
