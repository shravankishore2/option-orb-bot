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


# --- shadow variants: scored and logged, never deciding --------------------------------------

class _M:
    """A registry.Model stand-in."""
    def __init__(self, p, feats, threshold, version):
        self.model, self.features, self.threshold, self.version = FakeModel(p), feats, threshold, version


def test_shadow_variants_never_change_the_champions_decision(setup, day):
    source, feats = setup
    base = _M(0.56, feats, 0.6441, "v2")
    variants = [{"name": "v2.1", "version": "v2.1", "model": _M(0.70, feats[:-1], 0.6454, "v2.1"), "threshold": 0.6454},
                {"name": "v2@0.54", "version": "v2@0.54", "model": None, "threshold": 0.54}]
    plain = L.LiveEngine(source, FakeModel(0.3), feats, 0.5, ["X"], {}, baseline=base).cycle(at(day, 10, 20, 20))
    with_v = L.LiveEngine(source, FakeModel(0.3), feats, 0.5, ["X"], {}, baseline=base,
                          variants=variants).cycle(at(day, 10, 20, 20))
    strip = lambda ds: [{k: v for k, v in d.items() if k not in ("variants", "features")} for d in ds]
    assert strip(with_v) == strip(plain), [(k, a[k], b.get(k)) for a, b in zip(with_v, plain) for k in a
                                            if k not in ("variants", "features") and a[k] != b.get(k)]
    fa, fb = with_v[0]["features"], plain[0]["features"]       # NaN-aware: None or equal
    assert fa.keys() == fb.keys() and all((fa[k] is None and fb[k] is None) or fa[k] == fb[k] for k in fa)
    v = with_v[0]["variants"]
    assert v["v2.1"] == {"version": "v2.1", "threshold": 0.6454, "score": 0.7, "go": True}
    assert v["v2@0.54"] == {"version": "v2@0.54", "threshold": 0.54, "score": 0.56, "go": True}   # v2's own score
    assert with_v[0]["decision"] == "SKIP"                                                          # champion unchanged


def test_a_broken_variant_is_skipped_not_fatal(setup, day):
    source, feats = setup

    class Boom:
        features = feats
        def __getattr__(self, name):
            raise RuntimeError("corrupt pickle")
    d = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {},
                     variants=[{"name": "bad", "version": "x", "model": Boom(), "threshold": 0.5}]).cycle(at(day, 10, 20, 20))
    assert d[0]["decision"] == "GO" and d[0]["variants"] == {}


def test_registry_ships_v2_1_and_the_054_threshold_variant():
    import registry
    vs = {v["name"]: v for v in registry.shadow_variants()}
    v21 = vs["v2.1"]["model"]
    assert len(v21.features) == 25 and not {"entry_log", "orb_range_abs", "prev_close_vs_orb"} & set(v21.features)
    assert set(v21.features) < set(registry.baseline().features)
    assert v21.meta["trained_through"] == registry.baseline().trained_through
    assert vs["v2@0.54"]["model"] is None and vs["v2@0.54"]["threshold"] == 0.54


# --- the late-cut variant (docs/LATE_CUT_PROTOCOL.md) ----------------------------------------

@pytest.mark.parametrize("entry,late", [("14:59:00", False), ("15:00:00", True), ("15:10:00", True)])
def test_the_late_slice_flag_at_1459_1500_and_1510(setup, entry, late):
    import registry
    source, feats = setup
    eng = L.LiveEngine(source, FakeModel(0.9), feats, 0.644, ["X"], {}, version="v2",
                       variants=[v for v in registry.shadow_variants() if v["name"] == registry.LATE_CUT])
    v = next(v for v in eng.variants if v["name"] == registry.LATE_CUT)
    assert registry.in_late_slice(entry) is late and registry.in_late_slice(entry[:5]) is late
    # a champion GO: the variant keeps it only before 15:00
    d = eng._variant_decision(v, 0.9, entry)
    assert d == {"version": "v2@0.6440<15:00", "threshold": 0.644, "score": 0.9, "go": not late}
    # a champion SKIP is never a variant GO, early or late
    assert eng._variant_decision(v, 0.5, entry)["go"] is False


def test_the_late_cut_variant_scores_with_the_champion_and_changes_nothing(setup, day):
    import registry
    source, feats = setup
    variants = [v for v in registry.shadow_variants() if v["name"] == registry.LATE_CUT]
    plain = L.LiveEngine(source, FakeModel(0.9), feats, 0.644, ["X"], {}).cycle(at(day, 10, 20, 20))
    with_v = L.LiveEngine(source, FakeModel(0.9), feats, 0.644, ["X"], {}, variants=variants).cycle(at(day, 10, 20, 20))
    assert [d["decision"] for d in with_v] == [d["decision"] for d in plain] == ["GO"]
    v = with_v[0]["variants"][registry.LATE_CUT]
    assert v["score"] == with_v[0]["score"] and v["threshold"] == 0.644
    assert v["go"] is (with_v[0]["time"] < "15:00:00")                  # a 10:xx entry: kept


def test_registry_ships_the_late_cut_variant_fixed_at_1500():
    import registry
    vs = {v["name"]: v for v in registry.shadow_variants()}
    lc = vs[registry.LATE_CUT]
    assert lc["model"] == "champion" and lc["threshold"] is None and lc["before"] == dt.time(15, 0)
    assert registry.LATE_CUT_START == "2026-10-08" and set(vs) == {"v2.1", "v2@0.54", registry.LATE_CUT, registry.V3A}


# --- v3a (docs/V3A_PROTOCOL.md): frozen, and isolated from everything else ----------------------

def test_registry_ships_v3a_frozen_as_in_its_protocol():
    import json
    import registry
    v = {x["name"]: x for x in registry.shadow_variants()}[registry.V3A]
    meta = json.loads((registry.VARIANTS_DIR / "v3a.json").read_text())
    assert registry.sha256(registry.VARIANTS_DIR / "v3a.pkl") == registry.V3A_SHA256
    assert registry.V3A_SHA256 in (registry.BASE_DIR / "docs" / "V3A_PROTOCOL.md").read_text()
    assert v["threshold"] == meta["threshold"] == pytest.approx(0.6264, abs=1e-4)
    assert len(v["model"].features) == 25 and meta["trained_through"] == registry.baseline().trained_through
    assert registry.V3A_START == "2026-10-09"


class _Broken:
    """A v3a whose scoring raises (a corrupt or incompatible model)."""
    def __init__(self, feats, mode="raise"):
        self.features, self.mode, self.version, self.threshold = feats, mode, "v3a", 0.6264
        self.model = self

    def predict_proba(self, X):
        if self.mode == "raise":
            raise RuntimeError("v3a exploded")
        if self.mode == "nan":
            return np.column_stack([np.zeros(len(X)), np.full(len(X), np.nan)])
        return np.zeros((len(X) + 1, 2))                                   # wrong length


@pytest.mark.parametrize("mode", ["raise", "nan", "short"])
def test_a_failing_v3a_changes_nothing_else(setup, day, mode, capsys):
    source, feats = setup
    base = _M(0.56, feats, 0.6441, "v2")
    others = [{"name": "v2.1", "version": "v2.1", "model": _M(0.70, feats[:-1], 0.6454, "v2.1"), "threshold": 0.6454},
              {"name": "v2@0.54", "version": "v2@0.54", "model": None, "threshold": 0.54}]
    v3a = {"name": "v3a", "version": "v3a", "model": _Broken(feats[:25], mode), "threshold": 0.6264}
    kw = dict(baseline=base)
    plain = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {}, **kw).cycle(at(day, 10, 20, 20))
    good = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {}, variants=others, **kw).cycle(at(day, 10, 20, 20))
    bad = L.LiveEngine(source, FakeModel(0.9), feats, 0.5, ["X"], {}, variants=others + [v3a], **kw).cycle(at(day, 10, 20, 20))
    strip = lambda ds: [{k: v for k, v in d.items() if k not in ("variants", "features")} for d in ds]
    assert strip(bad) == strip(plain) and [d["decision"] for d in bad] == ["GO"]          # champion unchanged
    assert bad[0]["variants"] == good[0]["variants"] and "v3a" not in bad[0]["variants"]  # others unchanged
    assert "v3a" in capsys.readouterr().out                                                 # and it was logged


def test_a_corrupt_or_altered_v3a_file_is_skipped_and_the_others_still_load(tmp_path, monkeypatch, capsys):
    import shutil
    import registry
    for f in ("v2.1.pkl", "v2.1.json", "v3a.json"):
        shutil.copy(registry.VARIANTS_DIR / f, tmp_path / f)
    monkeypatch.setattr(registry, "VARIANTS_DIR", tmp_path)
    (tmp_path / "v3a.pkl").write_bytes(b"not a pickle")
    names = [v["name"] for v in registry.shadow_variants()]
    assert "v3a" not in names and "v2.1" in names and registry.LATE_CUT in names
    assert "v3a not loaded" in capsys.readouterr().out
    shutil.copy(registry.BASE_DIR / "models" / "variants" / "v2.1.pkl", tmp_path / "v3a.pkl")   # a real model, wrong hash
    assert "v3a" not in [v["name"] for v in registry.shadow_variants()]


# --- the previous-session snapshot: Dhan's 15:40 quote close isn't final ---------------------

def test_stale_close_share():
    assert L.stale_close_share({"A": 10.0, "B": 20.0, "C": 30.0}, {"A": 10.0, "B": 19.0}) == 0.5
    assert L.stale_close_share({"A": 10.0}, {}) == 0.0


def test_a_snapshot_with_stale_closes_warns_and_a_later_one_replaces_it(tmp_path, monkeypatch, capsys):
    path = tmp_path / "s.csv"
    pd.DataFrame([{"session_date": "2026-10-05", "symbol": s, "open": 1, "high": 2, "low": 0.5, "close": c}
                  for s, c in (("A", 10.0), ("B", 20.0), ("C", 30.0))]).to_csv(path, index=False)
    quotes = {"A": (1, 2, 0.5, 10.0), "B": (1, 2, 0.5, 20.0), "C": (1, 2, 0.5, 31.0)}     # 2 of 3 still yesterday's
    monkeypatch.setattr(L.dhan, "get_session_ohlc", lambda syms: quotes)
    L.take_session_snapshot(["A", "B", "C"], dt.date(2026, 10, 6), path)
    assert "67% of closes equal the previous session's" in capsys.readouterr().out
    quotes.update(A=(1, 2, 0.5, 10.5), B=(1, 2, 0.5, 21.0))                             # next morning: final
    L.take_session_snapshot(["A", "B", "C"], dt.date(2026, 10, 6), path)
    assert "closes equal" not in capsys.readouterr().out
    snap = L.load_snapshot(path)
    assert snap[(dt.date(2026, 10, 6), "A")][3] == 10.5 and len(pd.read_csv(path)) == 6      # replaced, not duplicated


def test_resnapshot_refuses_during_market_hours(monkeypatch, capsys):
    import main as M
    monkeypatch.setattr(M, "take_session_snapshot", lambda *a: (_ for _ in ()).throw(AssertionError("must not run")))
    assert M.resnapshot(["A"], dt.datetime(2026, 10, 7, 11, 0, tzinfo=IST)) == 1
    monkeypatch.setattr(M, "latest_completed_session", lambda now: dt.date(2026, 10, 6))
    calls = []
    monkeypatch.setattr(M, "take_session_snapshot", lambda syms, day: calls.append(day) or 3)
    assert M.resnapshot(["A"], dt.datetime(2026, 10, 7, 8, 50, tzinfo=IST)) == 0 and calls == [dt.date(2026, 10, 6)]


def test_the_session_loads_every_shadow_variant(capsys):
    """2026-10-08: the start-up message formatted v2-late-cut's threshold (None) and the
    exception dropped every variant for the session. Loading must return all of them."""
    import main as M
    import registry
    vs = M.load_variants()
    assert [v["name"] for v in vs] == [v["name"] for v in registry.shadow_variants()]
    assert {v["name"] for v in vs} == {"v2.1", "v3a", "v2@0.54", registry.LATE_CUT}
    out = capsys.readouterr().out
    assert "not loaded" not in out and "v2-late-cut (the champion's GO, entries before 15:00)" in out and "v3a (GO >= 0.626)" in out
