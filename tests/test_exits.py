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


# --- named exit rules: the trailing-stop grace period -------------------------------------

import numpy as np
import pytest


def _times(n, start=(10, 0)):
    t0 = dt.datetime(2026, 10, 6, *start)
    return [(t0 + dt.timedelta(minutes=5 * i)).time() for i in range(n)]


def test_the_current_rule_is_exit_v1_and_rules_resolve():
    assert exits.CURRENT_RULE == "exit_v1" and exits.rule_params("exit_v1")["trail_grace_min"] == 0
    assert exits.rule_params("exit_v2_grace10")["trail_grace_min"] == 10
    assert set(exits.SHADOW_RULES) <= set(exits.RULES) and exits.CURRENT_RULE not in exits.SHADOW_RULES
    with pytest.raises(KeyError):
        exits.rule_params("exit_v9")


def test_a_5_minute_grace_is_the_current_rule_on_any_path():
    """On 5-minute candles the first candle can only meet the initial stop anyway (the trail
    moves after a candle is survived), so exit_v2_grace5 == exit_v1, trade for trade."""
    rng = np.random.default_rng(7)
    for _ in range(3000):
        n = int(rng.integers(1, 40))
        close = 100 + np.cumsum(rng.normal(0, 0.6, n))
        high, low = close + rng.uniform(0, 0.8, n), close - rng.uniform(0, 0.8, n)
        d = "BUY" if rng.random() < 0.5 else "SELL"
        args = (high, low, close, _times(n), 100.0, 100.6, 99.4, d)
        assert exits.simulate(*args) == exits.simulate(*args, rule="exit_v2_grace5")


def test_grace10_ignores_the_trail_on_the_second_candle_but_keeps_the_hard_stop():
    # long, entry 100, ORB 1 -> hard stop 99, trail 1x. Candle 0 runs to 102 (trail -> 101);
    # candle 1 dips to 100.5 (would hit the trailed stop under exit_v1); candle 2 rallies.
    high, low, close = [102.0, 101.5, 104.0, 104.2], [100.2, 100.5, 101.6, 103.6], [101.8, 101.2, 103.9, 104.0]
    v1 = exits.simulate(high, low, close, _times(4), 100.0, 101.0, 100.0, "BUY")
    g10 = exits.simulate(high, low, close, _times(4), 100.0, 101.0, 100.0, "BUY", rule="exit_v2_grace10")
    assert v1[1] == "TRAIL" and v1[2] == 1 and v1[0] == pytest.approx(1.0)        # stopped at 101 on candle 1
    assert g10[2] > 1 and g10[0] > v1[0]                                            # rode the rally instead
    # the hard stop is active during the grace period
    st = {}
    g = exits.simulate([100.5, 100.2], [99.6, 98.5], [100.1, 98.9], _times(2), 100.0, 101.0, 100.0, "BUY",
                       rule="exit_v2_grace10", state=st)
    assert g[1] == "STOP" and g[2] == 1 and g[0] == pytest.approx(-1.0) and st["stop"] == pytest.approx(99.0)


def test_an_open_position_in_its_grace_period_shows_the_hard_stop():
    st = {}
    out = exits.simulate([102.0], [100.5], [101.8], _times(1), 100.0, 101.0, 100.0, "BUY",
                         rule="exit_v2_grace10", state=st)
    assert out[1] == "LAST" and st["stop"] == pytest.approx(99.0)          # next candle is still in grace
    st1 = {}
    exits.simulate([102.0], [100.5], [101.8], _times(1), 100.0, 101.0, 100.0, "BUY", state=st1)
    assert st1["stop"] == pytest.approx(101.0)                              # exit_v1: the trail applies next
