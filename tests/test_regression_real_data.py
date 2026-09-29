"""Integration checks on real cached market data (skipped if the cache is absent)."""

import datetime as dt
import os

import pandas as pd
import pytest

from conftest import data_dir_has_cache

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

pytestmark = pytest.mark.skipif(not data_dir_has_cache(), reason="needs data/history cache")


def test_replay_reproduces_the_live_bots_log_for_2026_08_17():
    """The replay must fire exactly what the real bot fired on a real day."""
    import build_historical_signals as B

    live = pd.read_csv(os.path.join(ROOT, "backtest_opening_range.csv"))
    live.columns = [c.strip().lower() for c in live.columns]
    live = live[live["date"].astype(str).str[:10] == "2026-08-17"]
    live = live[pd.to_datetime(live["time"], format="%H:%M:%S").dt.time <= dt.time(15, 10)]
    expected = set(zip(live["symbol"].str.upper(), live["direction"].str.upper()))
    assert expected, "no live log rows for the reference day"

    day = dt.date(2026, 8, 17)
    fired = set()
    for sym in {s for s, _ in expected}:
        sigs, _ = B.process_symbol(sym, day - dt.timedelta(days=10), day)
        fired |= {(s["symbol"], s["direction"]) for s in sigs if s["date"] == day.isoformat()}

    assert expected <= fired, f"missed: {sorted(expected - fired)}"


def test_live_and_history_features_agree_on_a_real_symbol():
    import features as F
    import history_cache as h
    day = dt.date(2026, 9, 3)
    daily = h.get_daily_candles("TCS", dt.date(2026, 6, 1), dt.date(2026, 9, 4))
    c = h.get_candles("TCS", dt.date(2026, 8, 1), dt.date(2026, 9, 4))
    full = F.build_symbol_history(c, daily)
    live = F.build_symbol_history(c[c.index.date <= day], daily[daily.index.date < day])
    for field in ("atr", "pdh", "pdl", "or_volume", "vol_norm"):
        assert getattr(full, field).get(day) == getattr(live, field).get(day), field
