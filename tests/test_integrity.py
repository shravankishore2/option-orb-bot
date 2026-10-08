"""integrity.py: after each session every expected shadow variant must load and log exactly one
row per live signal; any gap is alerted through the notifier and shown on the Scorecard."""

import pandas as pd
import pytest

import integrity
import registry
import shadow

DAY = "2026-10-09"
ALL = registry.EXPECTED_VARIANTS            # all four are expected from 2026-10-09


@pytest.fixture
def live(tmp_path, monkeypatch):
    monkeypatch.setattr(shadow, "LIVE_DIR", tmp_path)
    monkeypatch.setattr(shadow, "SIGNALS_FILE", tmp_path / "shadow_signals.csv")
    return tmp_path


def write(live, n_signals, rows_per_variant):
    """rows_per_variant: {variant: number of the day's signals that got a row}."""
    ids = [f"{DAY}|S{i}|BUY" for i in range(n_signals)]
    pd.DataFrame([{c: "" for c in shadow.SIGNAL_COLUMNS} | {"signal_id": s, "date": DAY, "time": "10:00:00",
                   "source": "live"} for s in ids], columns=shadow.SIGNAL_COLUMNS).to_csv(live / "shadow_signals.csv", index=False)
    rows = [{"signal_id": s, "date": DAY, "time": "10:00:00", "variant": v, "version": v, "score": 0.5,
             "threshold": 0.6, "go": False, "logged_at": f"{DAY}T10:00:20+05:30", "source": "live"}
            for v, k in rows_per_variant.items() for s in ids[:k]]
    pd.DataFrame(rows, columns=shadow.VARIANT_COLUMNS).to_csv(live / "shadow_variants.csv", index=False)


def test_a_complete_session_is_ok_and_sends_nothing(live):
    write(live, 5, {v: 5 for v in ALL})
    r = integrity.check(DAY)
    assert r["ok"] and r["signals"] == 5 and r["rows"] == {v: 5 for v in ALL}
    assert "OK, 5 signals" in integrity.message(r)


def test_a_missing_variant_is_a_problem(live):
    write(live, 5, {v: 5 for v in ALL if v != "v3a"})
    r = integrity.check(DAY)
    assert not r["ok"] and r["problems"] == ["v3a: no rows for 5 signals"]


def test_a_short_row_count_is_a_problem(live):
    write(live, 5, {**{v: 5 for v in ALL}, "v2.1": 3})
    r = integrity.check(DAY)
    assert not r["ok"] and r["problems"] == ["v2.1: 3 rows for 5 signals, 2 signals without a row"]


def test_the_2026_10_08_failure_every_variant_missing(live):
    write(live, 142, {})
    r = integrity.check(DAY)
    assert not r["ok"] and len(r["problems"]) == len(ALL)


def test_a_variant_that_fails_to_load_is_reported(live, monkeypatch):
    write(live, 5, {v: 5 for v in ALL})
    real = registry.shadow_variants
    monkeypatch.setattr(registry, "shadow_variants", lambda: [v for v in real() if v["name"] != "v3a"])
    r = integrity.check(DAY)
    assert not r["ok"] and r["problems"] == ["v3a fails to load"]


def test_no_session_is_not_an_alert(live):
    write(live, 0, {})
    r = integrity.check(DAY)
    assert r["ok"] and r["signals"] == 0


def test_problems_are_alerted_through_the_notifier_and_recorded(live, monkeypatch, capsys):
    write(live, 5, {v: 5 for v in ALL if v != "v2@0.54"})
    sent = []
    monkeypatch.setattr("notifier.send_telegram_message", sent.append)
    monkeypatch.setattr(integrity, "wait_for_bot", lambda limit_s=1800: True)
    monkeypatch.setattr("sys.argv", ["integrity.py", "--date", DAY])
    assert integrity.main() == 1
    assert len(sent) == 1 and "PROBLEM" in sent[0] and "v2@0.54: no rows" in sent[0]
    assert "PROBLEM" in capsys.readouterr().out                       # the journal
    assert integrity.last()["date"] == DAY and not integrity.last()["ok"]


def test_a_failing_notifier_never_raises(capsys):
    def boom(text):
        raise RuntimeError("telegram down")
    assert integrity.alert("x", send=boom) is False
    assert "not delivered" in capsys.readouterr().out


def test_the_scorecard_shows_the_last_check(live, monkeypatch):
    import webapp
    monkeypatch.setattr(webapp, "PASSWORD", None, raising=False)
    monkeypatch.setattr(webapp, "CHECKER", None, raising=False)
    write(live, 5, {v: 5 for v in ALL})
    integrity.record(integrity.check(DAY))
    html = webapp.app.test_client().get("/scorecard").get_data(as_text=True)
    assert "Last session's logging check" in html and f"OK</span> ({DAY}): 5 live signals" in html
    write(live, 5, {**{v: 5 for v in ALL}, "v2.1": 4})
    integrity.record(integrity.check(DAY))
    html = webapp.app.test_client().get("/scorecard").get_data(as_text=True)
    assert "PROBLEM</span>" in html and "v2.1: 4 rows for 5 signals" in html


def test_a_variant_is_expected_only_from_its_first_live_session(live, monkeypatch):
    monkeypatch.setitem(globals(), "DAY", "2026-10-07")          # write() logs that session
    write(live, 5, {"v2.1": 5, "v2@0.54": 5})
    r = integrity.check("2026-10-07")
    assert r["ok"] and set(r["rows"]) == {"v2.1", "v2@0.54"}
