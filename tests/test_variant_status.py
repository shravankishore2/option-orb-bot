"""variant_status.py: the late-cut protocol's card and verdict, and condition 3 of the
v2.1 / threshold protocols (met only when the whole 95% CI of variant minus champion,
after 0.05%, is above zero)."""

import datetime as dt

import pandas as pd
import pytest

import registry
import shadow
import variant_status as V


@pytest.fixture
def live(tmp_path, monkeypatch):
    monkeypatch.setattr(shadow, "LIVE_DIR", tmp_path)
    monkeypatch.setattr(shadow, "SIGNALS_FILE", tmp_path / "shadow_signals.csv")
    monkeypatch.setattr(shadow, "OUTCOMES_FILE", tmp_path / "shadow_outcomes.csv")
    return tmp_path


def days_from(start, n):
    d, out = dt.date.fromisoformat(start), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def write(live, trades):
    """trades: (date, time, symbol, champion score, pnl, {variant: go}) rows."""
    sig, out, var = [], [], []
    for date, time, sym, score, pnl, vgo in trades:
        sid = f"{date}|{sym}|BUY"
        sig.append({c: "" for c in shadow.SIGNAL_COLUMNS} | {
            "signal_id": sid, "date": date, "time": time, "symbol": sym, "direction": "BUY", "score": score,
            "threshold": 0.644, "model_go": score >= 0.644, "baseline_go": score >= 0.644,
            "decision": "GO" if score >= 0.644 else "SKIP", "model_version": "v2", "source": "live"})
        out.append({"signal_id": sid, "date": date, "status": "trailed", "exit_reason": "TRAIL",
                    "exit_candle": "15:15:00", "exit_time": "15:20:00", "exit_price": 1, "pnl_pct": pnl,
                    "profit": int(pnl > 0), "mfe_pct": 1, "mae_pct": -1, "labelled_at": f"{date}T15:20:20+05:30",
                    "source": "live", "exit_stop": 1})
        late_go = score >= 0.644 and not registry.in_late_slice(time)
        for name, go in {registry.LATE_CUT: late_go, **vgo}.items():
            var.append({"signal_id": sid, "date": date, "time": time, "variant": name, "version": name,
                        "score": score, "threshold": 0.644, "go": go, "logged_at": f"{date}T{time}+05:30",
                        "source": "live"})
    pd.DataFrame(sig).to_csv(live / "shadow_signals.csv", index=False)
    pd.DataFrame(out).to_csv(live / "shadow_outcomes.csv", index=False)
    pd.DataFrame(var).to_csv(live / "shadow_variants.csv", index=False)


def test_the_late_slice_is_judged_on_its_own_and_only_after_the_protocol(live):
    trades = [("2026-10-07", "15:05:00", "OLD", 0.9, -5.0, {})]          # before the protocol: never counted
    for d in days_from("2026-10-08", 3):
        trades += [(d, "14:55:00", "A", 0.9, 0.5, {}), (d, "15:00:00", "B", 0.9, -0.2, {}),
                   (d, "15:10:00", "C", 0.9, 0.1, {}), (d, "15:05:00", "D", 0.3, -1.0, {})]   # D: champion SKIP
    write(live, trades)
    lc = V.scorecard()["late"]
    assert lc["start"] == "2026-10-08" and lc["sessions"] == 3 and lc["consistent"]
    assert lc["n"] == 6 and lc["days"] == 3
    assert lc["mean"][0.05] == pytest.approx(-0.05 - 0.05) and lc["mean"][0.10] == pytest.approx(-0.15)
    assert lc["total"][0.05]["variant"] == pytest.approx(3 * 0.45)                     # the 14:55 trades
    assert lc["total"][0.05]["champion"] == pytest.approx(3 * (0.5 - 0.2 + 0.1 - 0.15))
    assert [s for _, _, s in lc["conditions"]] == ["not yet", "not yet", "not yet"]
    assert lc["verdict"] == "not enough data"
    line = V.status_line(V.scorecard(), "2026-10-09")
    assert "v2-late-cut (late slice: champion GO entered at or after 15:00, live from 2026-10-08): 6 late trades, 3 days" in line
    assert "1 not yet; 2 not yet; 3 not yet. Verdict: not enough data." in line


def test_a_late_slice_that_loses_supports_the_cut_and_the_first_verdict_is_locked(live):
    days = days_from("2026-10-08", 15)
    losing = [(d, t, s, 0.9, p, {}) for d in days
              for t, s, p in (("15:00:00", "A", -0.10), ("15:05:00", "B", -0.20), ("15:10:00", "C", -0.05),
                              ("15:10:00", "D", -0.15), ("10:00:00", "E", 0.30))]
    write(live, losing)
    lc = V.scorecard()["late"]
    assert lc["n"] == 60 and lc["days"] == 15 and lc["floor"]
    assert lc["ci"][0.05][1] < 0 and [s for _, _, s in lc["conditions"]] == ["met", "met", "not yet"]
    assert lc["verdict"].startswith("supported")
    V.record(days[-1])
    # later weeks: the late slice turns profitable; reported, but the first verdict stands
    more = days_from("2026-10-29", 30)[1:]
    write(live, losing + [(d, "15:05:00", f"W{i}", 0.9, 2.0, {}) for d in more for i in range(4)])
    h = V.record(more[-1])
    assert not h[-1]["late"]["verdict"].startswith("supported")
    assert V.locked_late_verdict() == {"date": days[-1], "verdict": lc["verdict"]}


@pytest.mark.parametrize("dlo,dhi,met", [(0.01, 0.4, True), (-0.1, 0.4, False), (-0.6, -0.1, False)])
def test_condition_3_is_met_only_when_the_whole_ci_is_above_zero(dlo, dhi, met):
    row = {"key": "v2.1", "n": 60, "days": 15,
           "rule": {"enough": True, "beats": (dlo + dhi) / 2 > 0, "ci_excludes_zero": dlo > 0 or dhi < 0,
                    "ci_above_zero": dlo > 0}}
    states = {i: s for i, _, s in V.conditions(row)}
    assert states[3] == ("met" if met else "not met")


def test_condition_3_through_the_scorecard_and_the_status_line(live):
    trades = []
    for d in days_from("2026-10-08", 15):
        trades += [(d, "10:00:00", f"C{i}", 0.9, 0.10 + 0.01 * i, {"v2.1": False}) for i in range(2)]
        trades += [(d, "11:00:00", f"V{i}", 0.5, 1.00 + 0.01 * i, {"v2.1": True}) for i in range(4)]
    write(live, trades)
    card = V.scorecard()
    v = next(r for r in card["rows"] if r["key"] == "v2.1")
    assert v["diff_ci"][0] > 0 and {i: s for i, _, s in v["conditions"]} == {1: "met", 2: "met", 3: "met", 5: "not yet"}
    assert "Conditions met: 1, 2, 3; 5 not yet." in V.status_line(card, "2026-10-30")
    assert registry.LATE_CUT not in {r["key"] for r in card["rows"]}             # judged by its own protocol
