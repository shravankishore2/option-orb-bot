"""Data plumbing: Dhan response parsing, the candle cache, stats, the notifier."""

import datetime as dt

import numpy as np
import pandas as pd

import dhan_client as dhan
import history_cache as h
import stats


def test_dhan_timestamps_land_on_ist_market_hours():
    # 2026-09-01 09:15 IST == 03:45 UTC
    ts = int(pd.Timestamp("2026-09-01 03:45", tz="UTC").timestamp())
    f = dhan._to_frame({"timestamp": [ts], "open": [1], "high": [2], "low": [0.5],
                        "close": [1.5], "volume": [10]})
    assert f.index[0].strftime("%Y-%m-%d %H:%M") == "2026-09-01 09:15"
    assert str(f.index.tz) == "Asia/Kolkata"


def test_empty_dhan_response_is_an_empty_frame():
    assert dhan._to_frame({}).empty


def test_prev_session_skips_today(monkeypatch):
    idx = pd.DatetimeIndex([pd.Timestamp(d, tz="Asia/Kolkata") for d in
                            ["2026-08-28", "2026-08-31", "2026-09-01"]])
    daily = pd.DataFrame({"Open": 1.0, "High": [10, 20, 30], "Low": [1, 2, 3],
                          "Close": [5, 15, 25], "Volume": 1.0}, index=idx)
    monkeypatch.setattr(dhan, "get_daily", lambda *a, **k: daily)
    assert dhan.get_prev_session("X", before=dt.date(2026, 9, 1)) == (1.0, 20.0, 2.0, 15.0)


def test_cache_tail_is_exact_but_head_tolerant():
    f = pd.DataFrame({"Close": 1.0}, index=pd.date_range("2026-09-01", "2026-09-18",
                                                         freq="B", tz="Asia/Kolkata"))
    # yesterday missing -> must fetch (previous close depends on it)
    assert h._missing_ranges(f, dt.date(2026, 9, 1), dt.date(2026, 9, 21)) == \
        [(dt.date(2026, 9, 19), dt.date(2026, 9, 21))]
    # a start a few days before the first candle (listing, holiday) -> no fetch
    assert h._missing_ranges(f, dt.date(2026, 8, 29), dt.date(2026, 9, 18)) == []


def test_block_bootstrap_ci_brackets_the_mean():
    rng = np.random.default_rng(0)
    trades = pd.DataFrame({"date": np.repeat([f"2026-01-{d:02d}" for d in range(1, 29)], 5),
                           "pnl_%": rng.normal(0.2, 1.0, 140)})
    lo, hi = stats.block_bootstrap_ci(trades)
    assert lo < trades["pnl_%"].mean() < hi


def test_max_drawdown():
    assert stats.max_drawdown(pd.Series([1.0, -3.0, 1.0, 5.0])) == -3.0


def test_permutation_detects_a_perfect_picker():
    pool = pd.DataFrame({"date": ["d1"] * 10 + ["d2"] * 10, "pnl_%": list(range(10)) * 2})
    best = pool.sort_values("pnl_%").groupby("date").tail(2)
    _, p = stats.permutation_vs_random(best, pool, reps=500)
    assert p < 0.05


def test_notifier_uses_ist_and_escapes_nothing():
    import notifier
    assert notifier.datetime.now().utcoffset() == dt.timedelta(hours=5, minutes=30)
    msg = notifier.build_message("M&M", "BUY", 1, 1, 1, 1, 1, 1, extras={"Score": "0.8 (GO ≥ 0.7)"})
    assert "M&M" in msg and "Score: 0.8" in msg


def test_telegram_credentials_from_environment(monkeypatch):
    import notifier
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "c")
    assert notifier.load_telegram_config() == ("t", "c")


def test_regular_session_drops_preopen_and_evening_candles():
    idx = pd.DatetimeIndex([pd.Timestamp(f"2026-09-01 {t}", tz="Asia/Kolkata")
                            for t in ["02:15", "09:07:01", "09:15", "15:25", "18:30"]])
    f = pd.DataFrame({"Close": range(5)}, index=idx)
    kept = dhan.regular_session(f).index.strftime("%H:%M").tolist()
    assert kept == ["09:15", "15:25"]


def test_intraday_request_asks_past_the_end_and_trims(monkeypatch):
    """Dhan omits the newest session unless toDate is beyond it."""
    sent = []

    def fake_post(path, payload):
        sent.append(payload)
        day = pd.Timestamp(payload["fromDate"], tz="Asia/Kolkata")
        stamps = [day + pd.Timedelta(days=k, hours=9, minutes=15) for k in range(3)]
        return {"timestamp": [int(s.timestamp()) for s in stamps], "open": [1] * 3,
                "high": [1] * 3, "low": [1] * 3, "close": [1] * 3, "volume": [1] * 3}

    monkeypatch.setattr(dhan, "_post", fake_post)
    monkeypatch.setattr(dhan, "get_security_id", lambda s: 1)
    f = dhan.get_intraday("X", "5", "2026-09-22", "2026-09-22")
    assert sent[0]["toDate"] == "2026-09-23"
    assert sorted({d.isoformat() for d in f.index.date}) == ["2026-09-22"]


def test_no_data_answer_is_an_empty_frame_not_an_error(monkeypatch):
    """Dhan returns HTTP 400 DH-907 for an unpublished daily bar; that must read as 'empty'."""
    def fake_post(path, payload):
        raise dhan.DhanError('Dhan /charts/historical failed [400]: {"errorCode":"DH-907"}')
    monkeypatch.setattr(dhan, "_post", fake_post)
    monkeypatch.setattr(dhan, "get_security_id", lambda s: 1)
    assert dhan.get_daily("X", "2026-09-22", "2026-09-22").empty
    assert dhan.get_intraday("X", "5", "2026-09-22", "2026-09-22").empty


def test_other_dhan_errors_still_raise(monkeypatch):
    import pytest
    def fake_post(path, payload):
        raise dhan.DhanError("Dhan /charts/historical failed [500]: server")
    monkeypatch.setattr(dhan, "_post", fake_post)
    monkeypatch.setattr(dhan, "get_security_id", lambda s: 1)
    with pytest.raises(dhan.DhanError):
        dhan.get_daily("X", "2026-09-22", "2026-09-22")
