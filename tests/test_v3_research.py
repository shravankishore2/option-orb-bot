"""v3 candidate research (docs/V3_PROTOCOL.md): the retest definition, exactly as fixed."""

import datetime as dt

import pandas as pd
import pytest

import v3_research as V
from conftest import IST


def candles(day, bars, start=dt.time(9, 15)):
    """bars: [(open, high, low, close, volume)] from `start`, 5 minutes apart."""
    t0 = dt.datetime.combine(day, start, tzinfo=IST)
    idx = pd.DatetimeIndex([t0 + dt.timedelta(minutes=5 * i) for i in range(len(bars))])
    return pd.DataFrame(bars, index=idx, columns=["Open", "High", "Low", "Close", "Volume"])


def sig(day, time="10:00:00", direction="BUY"):
    return {"date": day.isoformat(), "time": time, "symbol": "X", "direction": direction,
            "entry_price": 101.5, "ORH": 100.0, "ORL": 98.0, "prev_close": 99.0}


def day_with(after_breakout):
    # 09:15-09:50: eight quiet bars; 09:55 is the breakout bar B (signal timed 10:00)
    quiet = [(99, 99.5, 98.5, 99, 100)] * 8
    b = [(99.5, 101.8, 99.4, 101.5, 400)]
    return quiet + b + after_breakout


def test_a_pullback_to_the_edge_that_holds_is_a_retest(day):
    g = candles(day, day_with([(101.5, 102.4, 101.2, 102.0, 300),      # R-? no: low 101.2 > 100.5 (band)
                               (102.0, 102.1, 100.3, 101.0, 150)]))    # low 100.3 <= 100 + 0.25*2, close above ORH
    r = V.find_retest(g, sig(day))
    assert r["outcome"] == "retest" and r["bars"] == 2 and r["time"] == "10:10:00" and r["entry_price"] == 101.0
    assert r["level_penetration"] == 0.0                              # never traded back inside
    assert r["retest_depth_norm"] == pytest.approx((102.4 - 100.3) / 2.0)
    assert r["pre_retest_excursion"] == pytest.approx((102.4 - 100.0) / 2.0)
    assert r["retest_vol_ratio"] == pytest.approx(150 / 400)
    assert r["retest_rejection"] == pytest.approx((101.0 - 100.3) / (102.1 - 100.3))


def test_a_close_back_inside_the_range_kills_the_setup(day):
    g = candles(day, day_with([(101.5, 101.6, 99.6, 99.8, 300)]))     # closes <= ORH
    assert V.find_retest(g, sig(day))["outcome"] == "closed_inside"


def test_no_pullback_within_six_bars_is_no_retest_and_later_bars_are_never_read(day):
    up = [(101.5 + i, 102.5 + i, 101.4 + i, 102 + i, 200) for i in range(6)]
    g = candles(day, day_with(up + [(108, 108, 99.0, 100.2, 900)]))   # the 7th bar would qualify
    assert V.find_retest(g, sig(day))["outcome"] == "no_retest"


def test_short_side_mirrors(day):
    quiet = [(99, 99.5, 98.5, 99, 100)] * 8
    g = candles(day, quiet + [(98.5, 98.6, 96.0, 96.4, 400), (96.4, 97.7, 96.2, 97.0, 120)])
    r = V.find_retest(g, sig(day, direction="SELL"))
    assert r["outcome"] == "retest" and r["level_penetration"] == 0.0 and r["entry_price"] == 97.0
