"""Shadow tracking: every signal logged at signal time, walked by the backtest's
exit engine, labelled only once its outcome is complete — and never earlier."""

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

import exits
import live_engine as L
import shadow
from conftest import IST, make_day, make_daily
from test_live_engine import FakeModel, FakeSource

CANDLE = dt.timedelta(minutes=5)


class Base:
    """A stand-in for registry.Model."""

    def __init__(self, p, threshold=0.5, version="v2"):
        self.model, self.threshold, self.version = FakeModel(p), threshold, version
        self.features = list(shadow.FEATURE_COLUMNS)


def at(day, h, m, s=0):
    return dt.datetime.combine(day, dt.time(h, m, s), tzinfo=IST)


def cycles(day):
    t = at(day, 9, 40, 20)
    while t.time() <= dt.time(15, 16):
        yield t
        t += CANDLE


@pytest.fixture
def book(tmp_path):
    return shadow.ShadowBook(tmp_path / "s.csv", tmp_path / "o.csv", tmp_path / "t.json")


def days(day):
    """Three sessions, one BUY each (opening range 99.45-100.55, prev close 100, so the
    BUY fires on the first close >= 101.8; nothing ever reaches the SELL side at 98.2):
    trailed out mid-day, straight through the initial stop, and held to the 15:15 exit."""
    opening = [100.0, 100.5, 99.5, 100.0, 100.0]                      # 09:15-09:35
    hold = opening + [100.0 + 0.3 * i for i in range(1, 70)]
    climb = opening + [100.0 + 0.3 * i for i in range(1, 15)]         # to 104.2
    trail = climb + [climb[-1] - 0.4 * i for i in range(1, 8)] + [101.5] * 48
    stop = opening + [100.6, 101.2, 101.9, 100.5] + [100.5] * 65
    return {k: make_day(day, v, spread=0.05) for k, v in
            {"TRAIL": trail, "STOP": stop, "HOLD": hold}.items()}


def run_session(day, candles, book, p=0.9):
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 0, -1)])
    src = FakeSource({"X": candles}, daily)
    eng = L.LiveEngine(src, FakeModel(p), list(shadow.FEATURE_COLUMNS), 0.5, ["X"], {},
                       version="champ-test", baseline=Base(0.2))
    labelled_at, decisions = {}, []
    for now in cycles(day):
        new = eng.cycle(now)
        decisions += new
        book.record(new, now)
        for lab in book.update(eng.today_candles, now):
            labelled_at[lab["signal_id"]] = now
    for close in (dt.time(15, 20, 20), dt.time(15, 40, 20)):
        now = at(day, close.hour, close.minute, close.second)
        for lab in book.update({"X": L.completed(candles, now)}, now, final=close >= dt.time(15, 40)):
            labelled_at[lab["signal_id"]] = now
    return decisions, labelled_at


@pytest.mark.parametrize("kind", ["TRAIL", "STOP", "HOLD"])
def test_label_appears_only_once_the_exit_candle_has_closed(kind, day, book):
    candles = days(day)[kind]
    decisions, labelled_at = run_session(day, candles, book)
    assert len(decisions) == 1, "fixture should fire exactly one signal"
    sid, out = next(iter(book.closed.items()))
    exit_end = dt.datetime.combine(day, dt.time.fromisoformat(out["exit_time"]), tzinfo=IST)
    when = labelled_at[sid]
    assert exit_end <= when, "a label existed before its exit"
    assert when - exit_end < CANDLE + dt.timedelta(minutes=5 if kind == "HOLD" else 0), \
        "the label should appear at the first cycle after the exit"
    assert dt.datetime.fromisoformat(out["labelled_at"]) == when


@pytest.mark.parametrize("kind,reason", [("TRAIL", "TRAIL"), ("STOP", "STOP"), ("HOLD", "TIME")])
def test_outcome_equals_the_backtest_exit_engine(kind, reason, day, book):
    candles = days(day)[kind]
    decisions, _ = run_session(day, candles, book)
    d = decisions[0]
    pnl, why, _ = exits.simulate_day(candles, dt.time.fromisoformat(d["time"]), d["entry_price"],
                                     d["ORH"], d["ORL"], d["direction"])
    out = next(iter(book.closed.values()))
    assert why == reason == out["exit_reason"]
    assert out["pnl_pct"] == pytest.approx(pnl, abs=1e-4)
    assert out["status"] == shadow.STATUS[reason]


def test_no_label_while_open_and_none_from_a_forming_candle(day, book):
    """Even a source that leaks the whole day can't produce an early label."""
    candles = days(day)["TRAIL"]
    decisions, _ = run_session(day, candles, shadow.ShadowBook(book.signals_file.with_name("a.csv"),
                                                              book.outcomes_file.with_name("b.csv"),
                                                              book.tracker_file.with_name("c.json")))
    sig = {**decisions[0]}
    first = L.completed(candles, at(day, 15, 30))
    w = shadow.walk(sig, first, at(day, 15, 30))
    exit_end = w["exit_candle"] + CANDLE

    before = exit_end - dt.timedelta(seconds=1)
    book.record([sig], at(day, 10, 0))
    assert book.update({"X": candles}, before) == []         # full day handed over, still no label
    assert shadow.labelled_rows(book.signals_file, book.outcomes_file).empty
    row = next(r for r in json.loads(book.tracker_file.read_text())["rows"])
    assert row["status"] == "open"
    assert len(book.update({"X": candles}, exit_end)) == 1
    assert book.update({"X": candles}, exit_end + CANDLE) == []   # labelled once, never again


def test_label_guard_rejects_an_incomplete_exit(day, book):
    candles = days(day)["STOP"]
    sig = {"date": day.isoformat(), "time": "09:50:00", "symbol": "X", "direction": "BUY",
           "entry_price": 101.0, "ORH": 100.05, "ORL": 99.95}
    w = shadow.walk(sig, candles, at(day, 15, 30))
    with pytest.raises(AssertionError, match="before its exit"):
        book._label("x", sig, w, w["exit_candle"] + dt.timedelta(minutes=2))


def test_open_position_is_closed_at_its_last_candle_after_the_close(day, book):
    candles = days(day)["HOLD"]
    no_1515 = candles[candles.index.time < dt.time(15, 15)]         # Dhan's missing closing candles
    run_session(day, no_1515, book)
    out = next(iter(book.closed.values()))
    assert out["exit_reason"] == "LAST" and out["status"] == "eod"
    assert out["exit_time"] == "15:15:00"                            # close of the 15:10 candle
    assert dt.datetime.fromisoformat(out["labelled_at"]).time() >= dt.time(15, 40)


def test_every_signal_records_versions_scores_and_features_at_signal_time(day, book):
    candles = days(day)["TRAIL"]
    decisions, _ = run_session(day, candles, book, p=0.1)           # a NO-GO is tracked too
    s = pd.read_csv(book.signals_file)
    assert len(s) == 1 and not bool(s["model_go"][0]) and s["decision"][0] == "SKIP"
    assert s["model_version"][0] == "champ-test" and s["baseline_version"][0] == "v2"
    assert s["baseline_score"][0] == pytest.approx(0.2)
    assert all(f"x_{f}" in s for f in shadow.FEATURE_COLUMNS)
    for f, v in decisions[0]["features"].items():                   # file == what the model saw
        logged = s[f"x_{f}"][0]
        assert (v is None and pd.isna(logged)) or logged == pytest.approx(v, rel=1e-9)


def test_features_ignore_candles_after_the_signal(day, book):
    candles = days(day)["TRAIL"]
    d1, _ = run_session(day, candles, shadow.ShadowBook(book.signals_file.with_name("1.csv"),
                                                       book.outcomes_file.with_name("1o.csv"),
                                                       book.tracker_file.with_name("1.json")))
    entry = dt.time.fromisoformat(d1[0]["time"])
    tampered = candles.copy()
    late = tampered.index.time >= entry
    tampered.loc[late, ["High", "Low", "Close", "Open"]] *= 1.5
    tampered.loc[late, "Volume"] *= 9
    d2, _ = run_session(day, tampered, book)
    assert d1[0]["features"] == d2[0]["features"]
    assert d1[0]["score"] == d2[0]["score"]


def test_restart_reloads_today_and_never_relabels(day, tmp_path):
    files = (tmp_path / "s.csv", tmp_path / "o.csv", tmp_path / "t.json")
    candles = days(day)["TRAIL"]
    run_session(day, candles, shadow.ShadowBook(*files))
    again = shadow.ShadowBook(*files)
    again.load(day)
    assert again.update({"X": candles}, at(day, 15, 45), final=True) == []
    assert len(pd.read_csv(files[1])) == 1


def test_training_rows_need_a_complete_label(day, book):
    run_session(day, days(day)["TRAIL"], book)
    rows = shadow.labelled_rows(book.signals_file, book.outcomes_file)
    shadow.check_labels_are_complete(rows)
    assert set(shadow.FEATURE_COLUMNS) <= set(rows.columns)
    bad = rows.copy()
    bad["labelled_at"] = (dt.datetime.combine(day, dt.time(9, 0), tzinfo=IST)).isoformat()
    with pytest.raises(AssertionError, match="before its exit"):
        shadow.check_labels_are_complete(bad)


def test_scorecard_counts():
    df = pd.DataFrame({
        "date": ["2026-10-01"] * 6 + ["2026-10-02"] * 2,
        "model_go": [True, True, False, False, False, False, True, False],
        "baseline_go": [True] * 8,
        "pnl_%": [0.5, -0.2, 0.3, -0.4, -0.1, 0.0, 0.2, 0.6],
        "source": ["replay"] * 6 + ["live"] * 2,
    })
    sc = shadow.scorecard(df)
    t = sc["total"]
    assert t["go"]["n"] == 3 and t["go"]["hit_rate"] == pytest.approx(200 / 3)
    assert t["nogo"]["n"] == 5 and t["missed_winners"] == 2 and t["avoided_losers"] == 2
    assert t["go"]["too_few"] and t["nogo"]["too_few"]
    assert t["go"]["net"]["0.05"] == pytest.approx(t["go"]["avg_pnl"] - 0.05)
    assert [d["date"] for d in sc["days"]] == ["2026-10-02", "2026-10-01"]
    assert sc["days"][1]["replayed"] and not sc["days"][0]["replayed"]
    assert shadow.scorecard(df, by="baseline_go")["total"]["nogo"]["n"] == 0


# --- stops: initial at entry, current while open, in force at the exit -------------------

def _tracker(book):
    return {r["id"]: r for r in json.loads(book.tracker_file.read_text())["rows"]}


@pytest.mark.parametrize("kind", ["TRAIL", "STOP", "HOLD"])
def test_every_open_position_has_a_current_stop_and_every_exit_its_stop(kind, day, book):
    candles = days(day)[kind]
    daily = make_daily([day - dt.timedelta(days=k) for k in range(20, 0, -1)])
    eng = L.LiveEngine(FakeSource({"X": candles}, daily), FakeModel(0.9), list(shadow.FEATURE_COLUMNS), 0.5,
                       ["X"], {}, version="champ-test", baseline=Base(0.2))
    seen_open = 0
    for now in cycles(day):
        book.record(eng.cycle(now), now)
        book.update(eng.today_candles, now)
        for r in _tracker(book).values():
            assert r["initial_stop"] is not None
            if r["status"] == "open" and r["price"] is not None:     # walked at least one candle
                seen_open += 1
                assert r["trail_stop"] is not None, "an open position must show its current stop"
                long = r["direction"] == "BUY"
                assert (r["trail_stop"] >= r["initial_stop"]) if long else (r["trail_stop"] <= r["initial_stop"])
    book.update({"X": L.completed(candles, at(day, 15, 40, 20))}, at(day, 15, 40, 20), final=True)
    out = next(iter(book.closed.values()))
    row = next(iter(_tracker(book).values()))
    assert seen_open > 0 or kind == "STOP"            # STOP is stopped on its first candle after entry
    assert row["exit_stop"] is not None and row["trail_stop"] == row["exit_stop"] == out["exit_stop"]
    if out["exit_reason"] in ("STOP", "TRAIL"):
        # exits.simulate fills a stop or trail exit exactly at the stop in force
        assert out["exit_stop"] == pytest.approx(out["exit_price"], abs=0.01)
    else:                                       # closed at 15:15 above a long's stop
        assert out["exit_stop"] < out["exit_price"]
    if out["exit_reason"] == "TRAIL":
        assert out["exit_stop"] > row["initial_stop"]        # the trail had moved the stop up


def test_outcomes_written_before_exit_stop_existed_are_migrated_not_rewritten(day, book):
    old_cols = shadow.OUTCOME_COLUMNS[:-1]
    old = {c: "" for c in old_cols} | {"signal_id": f"{day}|OLD|BUY", "date": day.isoformat(), "status": "trailed",
                                        "exit_reason": "TRAIL", "exit_price": "101.25", "pnl_pct": "0.5"}
    shadow._append(book.outcomes_file, old_cols, [old])
    before = book.outcomes_file.read_text().splitlines()[1]
    candles = days(day)["STOP"]
    run_session(day, candles, book)
    lines = book.outcomes_file.read_text().splitlines()
    assert lines[0].split(",") == shadow.OUTCOME_COLUMNS
    assert lines[1] == before + ","                          # old row kept, blank exit_stop
    o = shadow._read(book.outcomes_file)
    assert len(o) == 2 and pd.isna(o["exit_stop"].iloc[0]) and o["exit_stop"].iloc[1] > 0
    assert len(shadow.labelled_rows(book.signals_file, book.outcomes_file)) == 1   # only signals we logged


def test_old_labels_show_the_exit_price_as_the_stop_for_stop_and_trail_exits():
    assert shadow._exit_stop({"exit_reason": "TRAIL", "exit_price": 101.2, "exit_stop": ""}) == 101.2
    assert shadow._exit_stop({"exit_reason": "TIME", "exit_price": 101.2, "exit_stop": None}) is None
    assert shadow._exit_stop({"exit_reason": "TIME", "exit_price": 101.2, "exit_stop": 99.4}) == 99.4


# --- only the current exit rule is tracked ---------------------------------------------

def test_no_side_by_side_exit_rule_is_written_or_shown(day, book):
    run_session(day, days(day)["TRAIL"], book)
    assert sorted(p.name for p in book.outcomes_file.parent.glob(book.outcomes_file.stem + "_*")) == []
    row = next(iter(json.loads(book.tracker_file.read_text())["rows"]))
    assert row["rule"] == exits.CURRENT_RULE and "alt" not in row


def test_variant_decisions_are_logged_beside_each_signal(day, book):
    import live_engine as LE  # noqa: F401
    d = {"date": day.isoformat(), "time": "10:00:00", "symbol": "X", "direction": "BUY", "entry_price": 101.0,
         "ORH": 100.5, "ORL": 99.5, "prev_close": 99.0, "score": 0.3, "threshold": 0.644, "decision": "SKIP",
         "variants": {"v2.1": {"version": "v2.1", "threshold": 0.6454, "score": 0.66, "go": True},
                      "v2@0.54": {"version": "v2@0.54", "threshold": 0.54, "score": 0.56, "go": True}}}
    book.record([d], at(day, 10, 0, 20))
    book.record([d], at(day, 10, 5, 20))                      # already logged: not again
    v = shadow._read(book.variants_file)
    assert len(v) == 2 and set(v["variant"]) == {"v2.1", "v2@0.54"} and v["go"].astype(str).eq("True").all()
    assert "variants" not in shadow._read(book.signals_file).columns
