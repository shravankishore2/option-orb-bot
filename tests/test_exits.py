"""The single exit rule used by every result in the project."""

import datetime as dt

import exits


def T(h, m):
    return dt.time(h, m)


def test_stop_is_checked_first_when_a_candle_touches_both():
    # entry 100, ORB 1 -> stop 99; one candle spans 98..105
    out = exits.simulate([105.0], [98.0], [104.0], [T(10, 0)], 100.0, 101.0, 100.0, "BUY",
                         stop_mult=1.0, target_mult=2.0, trail_mult=None)
    assert out[0] == -1.0 and out[1] == "STOP"


def test_target_hit():
    out = exits.simulate([101.0, 102.5], [99.5, 101.0], [100.8, 102.0], [T(10, 0), T(10, 5)],
                         100.0, 101.0, 100.0, "BUY", stop_mult=1.0, target_mult=2.0, trail_mult=None)
    assert out[1] == "TARGET" and abs(out[0] - 2.0) < 1e-9


def test_trail_locks_in_gains():
    # runs to 105, trail 1.0 -> stop 104, then drops
    highs = [102.0, 105.0, 104.5]
    lows = [100.5, 103.0, 103.5]
    closes = [101.5, 104.8, 103.8]
    out = exits.simulate(highs, lows, closes, [T(10, 0), T(10, 5), T(10, 10)],
                         100.0, 101.0, 100.0, "BUY", stop_mult=1.0, target_mult=None, trail_mult=1.0)
    assert out[1] == "TRAIL" and abs(out[0] - 4.0) < 1e-9


def test_trail_uses_the_previous_candle_not_the_current_one():
    """The trail moves only after a candle is survived (no intra-candle lookahead)."""
    out = exits.simulate([105.0], [103.5], [104.9], [T(10, 0)], 100.0, 101.0, 100.0, "BUY",
                         stop_mult=1.0, target_mult=None, trail_mult=1.0)
    assert out[1] != "TRAIL" or out[2] > 0


def test_forced_exit_at_1515():
    out = exits.simulate([100.5, 100.6], [99.5, 99.6], [100.2, 100.4], [T(15, 10), T(15, 15)],
                         100.0, 101.0, 100.0, "BUY", stop_mult=1.0, target_mult=None, trail_mult=None)
    assert out[1] == "TIME" and abs(out[0] - 0.4) < 1e-9


def test_sell_is_mirrored():
    out = exits.simulate([101.5], [99.0], [99.5], [T(10, 0)], 100.0, 100.5, 99.5, "SELL",
                         stop_mult=1.0, target_mult=None, trail_mult=None)
    assert out[1] == "STOP" and out[0] == -1.0


def test_zero_width_range_is_untradeable():
    assert exits.simulate([1.0], [1.0], [1.0], [T(10, 0)], 1.0, 1.0, 1.0, "BUY") is None


def test_levels_for_the_telegram_message():
    lv = exits.levels(102.0, 100.2, 99.8, "BUY")
    assert lv["stop"] == round(102.0 - 0.4, 2) and lv["force_exit"] == "15:15"
    assert exits.levels(98.0, 100.2, 99.8, "SELL")["stop"] == round(98.0 + 0.4, 2)


# --- one exit rule: the grace-period experiment is retired ------------------------------

import inspect


def test_only_the_current_rule_exists():
    """The trailing-stop grace experiment concluded without adoption (docs/EXIT_GRACE.md);
    exits.simulate has no grace path and no rule switch left."""
    assert exits.CURRENT_RULE == "exit_v1"
    params = inspect.signature(exits.simulate).parameters
    assert "trail_grace_min" not in params and "rule" not in params
    assert not hasattr(exits, "SHADOW_RULES") and not hasattr(exits, "RULES")
