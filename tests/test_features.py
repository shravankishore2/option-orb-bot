"""Feature engine: no lookahead, live/history parity, and label isolation."""

import datetime as dt

import numpy as np
import pandas as pd

import features as F
import strategy_config as C
from conftest import make_day, make_daily


def _history(days_back=30, today=dt.date(2026, 9, 1)):
    days = pd.bdate_range(today - dt.timedelta(days=days_back), today).date
    intraday = pd.concat([make_day(d, [100.0 + (i % 3) for i in range(75)], volume=1000 + i)
                          for i, d in enumerate(days)])
    daily = make_daily(list(days))
    for i, idx in enumerate(daily.index):
        daily.loc[idx, ["High", "Low"]] = [102.0 + i * 0.1, 98.0]
    return intraday, daily, list(days)


def _signal(day, time="10:20:00", entry=102.0):
    return {"date": day.isoformat(), "time": time, "symbol": "X", "direction": "BUY",
            "entry_price": entry, "ORH": 100.2, "ORL": 99.8}


def test_features_and_labels_never_overlap():
    assert not set(F.FEATURES) & set(F.LABELS)


def test_features_ignore_candles_after_entry(breakout_day, day):
    intraday, daily, _ = _history(today=day)
    hist = F.build_symbol_history(intraday, daily)
    ctx = F.MarketContext({"X": breakout_day}, {"X": "IT"})
    sig = _signal(day)

    before = F.compute_features(sig, breakout_day, hist, ctx, "IT")

    tampered = breakout_day.copy()
    tampered.loc[tampered.index.time >= dt.time(10, 20), ["High", "Low", "Close", "Volume"]] = [999, 1, 500, 9e9]
    ctx2 = F.MarketContext({"X": tampered}, {"X": "IT"})
    after = F.compute_features(sig, tampered, hist, ctx2, "IT")

    for k in F.FEATURES:
        assert (np.isnan(before[k]) and np.isnan(after[k])) or before[k] == after[k], k


def test_atr_excludes_the_signal_days_own_range():
    """Lookahead fix: today's high/low isn't known at a 10:20 entry."""
    intraday, daily, days = _history()
    today = days[-1]
    hist = F.build_symbol_history(intraday, daily)
    k = len(days) - 1                                   # rows strictly before today
    expected = (daily["High"] - daily["Low"]).to_numpy()[max(0, k - 14):k].mean()
    assert abs(hist.atr[today] - expected) < 1e-9


def test_history_and_live_views_agree():
    """Training/serving parity: the live bot never has today's daily row."""
    intraday, daily, days = _history()
    today = days[-1]
    full = F.build_symbol_history(intraday, daily)
    live = F.build_symbol_history(intraday, daily[daily.index.date < today])
    for field in ("atr", "pdh", "pdl", "or_volume", "vol_norm"):
        assert getattr(full, field).get(today) == getattr(live, field).get(today), field


def test_or_volume_norm_uses_only_prior_days():
    intraday, daily, days = _history()
    hist = F.build_symbol_history(intraday, daily)
    today = days[-1]
    prior = [hist.or_volume[d] for d in days[:-1]][-20:]
    assert hist.vol_norm[today] == float(np.median(prior))


def test_breadth_uses_only_the_completed_stamp(day):
    up = make_day(day, [100.0] * 5 + [101.0] * 70)
    down = make_day(day, [100.0] * 5 + [99.0] * 70)
    ctx = F.MarketContext({"A": up, "B": down, "C": up}, {"A": "S", "B": "S", "C": "S"})
    _, _, breadth, sect, own = ctx.lookup("A", "S", (day, dt.time(10, 15)))
    assert abs(breadth - 2 / 3) < 1e-9
    assert own > 0


def test_membership_mask_changes_breadth(day):
    up = make_day(day, [100.0] * 5 + [101.0] * 70)
    down = make_day(day, [100.0] * 5 + [99.0] * 70)
    ctx = F.MarketContext({"A": up, "B": down}, {}, members=lambda d: {"A"})
    assert ctx.lookup("A", "UNKNOWN", (day, dt.time(10, 15)))[2] == 1.0


def test_forward_label_stop_wins_a_shared_candle(day):
    """Label fix: a candle touching both +1.5R and -1R counts as a stop."""
    closes = [100.0] * 13 + [102.0] + [102.0] * 60
    g = make_day(day, closes)
    # candle right after entry swings to both +1.5R and -1R (ORB = 0.4 -> R = 0.4)
    wild = g.index.time == dt.time(10, 20)
    g.loc[wild, ["High", "Low"]] = [102.0 + 1.5 * 0.4 + 0.1, 102.0 - 0.4 - 0.1]
    lab = F.forward_labels(_signal(day), g)
    legacy = F.forward_labels(_signal(day), g, legacy=True)
    assert lab["hit_1_5r"] == 0
    assert legacy["hit_1_5r"] == 1


def test_trainer_whitelist_blocks_forward_labels(tmp_path, monkeypatch):
    """Forward labels live in the same CSV as features — they must never be inputs."""
    import train_classifier as T
    rows = pd.DataFrame({"date": ["2026-09-01"], "time": ["10:20:00"], "symbol": ["X"],
                         "direction": ["BUY"], **{f: [0.1] for f in F.FEATURES},
                         **{l: [1] for l in F.LABELS}})
    path = tmp_path / "feats.csv"
    rows.to_csv(path, index=False)
    monkeypatch.setattr(T, "HIST_EXTRA_FILE", str(path))
    merged = pd.DataFrame({"date": ["2026-09-01"], "symbol": ["X"], "direction": ["BUY"]})
    _, names = T.attach_extra_features(merged)
    assert set(names) == set(F.FEATURES)
    assert not set(names) & set(F.LABELS)
