"""The live engine: completed candles only, first-signal-only, staleness, restarts,
and — the key property — cycle-by-cycle live decisions equal the batch replay."""

import datetime as dt

import numpy as np
import pandas as pd
import pytest

import build_historical_signals as B
import live_engine as L
from conftest import IST, make_day, make_daily


class FakeModel:
    """Scores every signal with a fixed probability."""

    def __init__(self, p):
        self.p = p

    def predict_proba(self, X):
        return np.column_stack([1 - np.full(len(X), self.p), np.full(len(X), self.p)])


class FakeSource:
    def __init__(self, sessions, daily, nifty=None):
        self.sessions = sessions          # {symbol: full-day candles}
        self.daily_ = daily
        self.nifty = nifty

    def history(self, symbol, start, end):
        return pd.DataFrame()

    def daily(self, symbol, start, end):
        return self.daily_

    def today(self, symbol, day, now):
        return L.completed(self.sessions[symbol], now)

    def nifty_today(self, day, now):
        return None


@pytest.fixture
def setup(day, breakout_day):
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 0, -1)])
    source = FakeSource({"X": breakout_day}, daily)
    import features as F
    feats = ["orb_range_pct", "gap_pct", "breakout_strength"] + F.FEATURES
    return source, feats


def at(day, h, m, s=0):
    return dt.datetime.combine(day, dt.time(h, m, s), tzinfo=IST)


def test_forming_candle_is_dropped(breakout_day, day):
    c = L.completed(breakout_day, at(day, 10, 17))
    assert c.index[-1].time() == dt.time(10, 10)      # 10:15 candle ends 10:20


def test_go_skip_by_threshold(setup, day):
    source, feats = setup
    go = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {}).cycle(at(day, 10, 20, 20))
    skip = L.LiveEngine(source, FakeModel(0.1), feats, 0.5, ["X"], {}).cycle(at(day, 10, 20, 20))
    assert go[0]["decision"] == "GO" and skip[0]["decision"] == "SKIP"
    assert go[0]["stop"] < go[0]["entry_price"]


def test_signal_is_decided_once(setup, day):
    source, feats = setup
    eng = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {})
    first = eng.cycle(at(day, 10, 20, 20))
    later = eng.cycle(at(day, 10, 25, 20))
    assert len(first) == 1 and later == []


def test_restart_does_not_resend(setup, day):
    source, feats = setup
    eng = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {})
    eng.prepare(day)
    eng.mark_seen({(day.isoformat(), "X", "BUY")})
    assert eng.cycle(at(day, 10, 20, 20)) == []


def test_late_start_marks_old_signals_stale(setup, day):
    """Bot started at 11:30: the 10:20 breakout is logged, never sent."""
    source, feats = setup
    out = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {}).cycle(at(day, 11, 30, 20))
    assert out[0]["decision"] == "STALE"


def test_no_signal_before_the_breakout_candle_closes(setup, day):
    source, feats = setup
    eng = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {})
    assert eng.cycle(at(day, 10, 19, 50)) == []        # 10:15 candle not closed yet
    assert len(eng.cycle(at(day, 10, 20, 20))) == 1


def test_live_cycles_match_the_batch_replay(setup, day, breakout_day):
    """Running every 5 minutes through the day == replaying the day at once."""
    source, feats = setup
    eng = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {})
    live = []
    t = at(day, 9, 40, 20)
    while t <= at(day, 15, 15, 20):
        live += eng.cycle(t)
        t += dt.timedelta(minutes=5)

    daily = source.daily_
    batch = B.replay_day("X", day, breakout_day, *B.prev_session(daily, day))
    key = lambda s: (s["symbol"], s["direction"], s["time"], s["entry_price"])
    assert sorted(map(key, live)) == sorted(map(key, batch))


def test_symbol_without_history_is_skipped(day, breakout_day):
    class NoDaily(FakeSource):
        def daily(self, symbol, start, end):
            return pd.DataFrame()
    eng = L.LiveEngine(NoDaily({"X": breakout_day}, None), FakeModel(0.9), [], 0.5, ["X"], {})
    assert eng.prepare(day) == 0
    assert eng.cycle(at(day, 10, 20, 20)) == []


def test_holiday_has_no_symbols_with_data(day):
    class Empty(FakeSource):
        def today(self, symbol, d, now):
            return pd.DataFrame()
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 0, -1)])
    eng = L.LiveEngine(Empty({"X": None}, daily), FakeModel(0.9), [], 0.5, ["X"], {})
    assert eng.cycle(at(day, 10, 0, 20)) == []
    assert eng.symbols_with_data == 0


def test_live_universe_is_dealiased_and_warns_when_stale(tmp_path, capsys):
    import os, time
    import fetch_symbols as FS
    f = tmp_path / "list.csv"
    pd.DataFrame({"Symbol": ["LTIM", "DUMMYTATAM", "TCS"]}).to_csv(f, index=False)
    old = time.time() - 300 * 86400
    os.utime(f, (old, old))
    assert FS.get_symbols(f) == ["LTM", "TCS"]
    assert "days old" in capsys.readouterr().out


def _past_with_latest(day, close_last=104.0):
    prev = day - dt.timedelta(days=1)
    return make_day(prev, [100.0] * 74 + [close_last])


def test_missing_daily_bar_uses_official_snapshot(day):
    """Dhan publishes the daily bar late; the snapshot's official close wins."""
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 1, -1)])   # no bar for day-1
    daily.index = daily.index.tz_convert("Asia/Kolkata")    # what Dhan's data actually carries
    past = _past_with_latest(day)
    out, src = L.with_latest_session(daily, past, day, snapshot=(100.0, 105.0, 99.0, 103.2))
    assert src == "snapshot"
    assert B.prev_session(out, day) == (105.0, 99.0, 103.2)


def test_missing_daily_bar_without_snapshot_falls_back_to_candles(day):
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 1, -1)])
    out, src = L.with_latest_session(daily, _past_with_latest(day), day, snapshot=None)
    assert src == "candles"
    assert B.prev_session(out, day)[2] == 104.0                 # last trade, flagged approximate


def test_published_daily_bar_is_used_as_is(day):
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 0, -1)])   # includes day-1
    out, src = L.with_latest_session(daily, _past_with_latest(day), day, snapshot=(0, 0, 0, 0))
    assert src == "daily" and len(out) == len(daily)


def test_failed_preparation_is_reported_not_silent(day, breakout_day):
    class Broken(FakeSource):
        def daily(self, symbol, start, end):
            raise L.dhan.DhanError("Dhan /charts/historical failed [500]")
    eng = L.LiveEngine(Broken({"X": breakout_day}, None), FakeModel(0.9), [], 0.5, ["X"], {})
    assert eng.prepare(day) == 0
    assert "X" in eng.prepare_errors


def test_zero_prepared_symbols_is_not_a_holiday(day, breakout_day, monkeypatch):
    """With nothing prepared, 'no candles today' says nothing about the exchange."""
    import main as M
    class Broken(FakeSource):
        def daily(self, symbol, start, end):
            return pd.DataFrame()
    eng = L.LiveEngine(Broken({"X": breakout_day}, None), FakeModel(0.9), [], 0.5, ["X"], {})
    monkeypatch.setattr(M, "reset_sent_notifications_if_new_day", lambda: None)
    monkeypatch.setattr(M, "load_snapshot", lambda: {})
    monkeypatch.setattr(M, "decided_today", lambda d: set())
    M.run_cycle(eng, {}, dry_run=True, now=at(day, 10, 0, 20))
    assert eng.day is None            # will prepare again next cycle
    assert not eng.prev               # so main's holiday check (needs engine.prev) can't fire
