"""Tracker and scorecard pages, and the live push stream (Server-Sent Events)."""

import functools
import json

import pytest

import auth
import shadow
import webapp

SNAP = {"version": "2026-10-01T10:05:20+05:30/0", "day": "2026-10-01",
        "as_of": "2026-10-01T10:05:20+05:30",
        "rows": [{"id": "2026-10-01|M&M|BUY", "symbol": "M&M", "time": "09:55", "direction": "BUY",
                  "score": 0.7, "threshold": 0.644, "go": True, "decision": "GO", "baseline_go": True,
                  "entry": 3000.0, "stop": 2970.0, "target": None, "status": "open",
                  "status_label": "open", "price": 3010.0, "pnl_pct": 0.33, "trail_stop": 2980.0,
                  "best_pct": 0.5, "worst_pct": -0.1, "exit_time": None, "model_version": "v2"}]}


@pytest.fixture
def files(tmp_path, monkeypatch):
    t = tmp_path / "tracker.json"
    t.write_text(json.dumps(SNAP))
    monkeypatch.setattr(shadow, "TRACKER_FILE", t)
    monkeypatch.setattr(shadow, "SIGNALS_FILE", tmp_path / "s.csv")
    monkeypatch.setattr(shadow, "OUTCOMES_FILE", tmp_path / "o.csv")
    return tmp_path


@pytest.fixture
def open_client(files, monkeypatch):
    monkeypatch.setattr(webapp, "CHECKER", None)
    return webapp.app.test_client()


def test_tracker_page_embeds_the_snapshot_safely(open_client):
    html = open_client.get("/tracker").get_data(as_text=True)
    assert 'id="trk-data"' in html and "tracker.js" in html
    assert "M\\u0026M" in html or "M&amp;M" in html            # escaped inside the script tag
    assert 'data-tab="all"' in html and 'data-tab="go"' in html and 'data-tab="nogo"' in html
    assert webapp.SELECTIVE_NOTE in html


def test_stream_pushes_the_snapshot(open_client):
    r = open_client.get("/api/tracker/stream?max_events=1")
    assert r.mimetype == "text/event-stream"
    body = r.get_data(as_text=True)
    assert body.startswith("retry: 3000")
    event = body.split("event: tracker\ndata: ")[1].split("\n\n")[0]
    assert json.loads(event)["rows"][0]["symbol"] == "M&M"
    assert f"id: {SNAP['version']}" in body


def test_stream_skips_a_snapshot_the_browser_already_has(open_client, monkeypatch):
    monkeypatch.setattr(webapp, "STREAM_SECONDS", 0.01)
    body = open_client.get("/api/tracker/stream",
                           headers={"Last-Event-ID": SNAP["version"]}).get_data(as_text=True)
    assert "event: tracker" not in body


def test_stream_and_api_need_login(files, monkeypatch):
    monkeypatch.setattr(webapp, "CHECKER", auth.PasswordCheck("pw"))
    c = webapp.app.test_client()
    for path in ("/api/tracker/stream", "/api/tracker", "/api/me"):
        r = c.get(path)
        assert r.status_code == 401 and r.get_json()["error"] == "login required"
    assert c.get("/tracker").status_code == 302


def test_stream_connections_are_capped(open_client, monkeypatch):
    import threading
    monkeypatch.setattr(webapp, "_streams", threading.BoundedSemaphore(1))
    webapp._streams.acquire()
    r = open_client.get("/api/tracker/stream")
    assert r.status_code == 503 and r.headers["Retry-After"]
    webapp._streams.release()


def test_scorecard_page(open_client, files):
    import pandas as pd
    s = pd.DataFrame([{c: "" for c in shadow.SIGNAL_COLUMNS}] * 2)
    s["signal_id"] = ["2026-10-01|A|BUY", "2026-10-01|B|BUY"]
    s["date"], s["time"], s["symbol"], s["direction"] = "2026-10-01", "10:00:00", ["A", "B"], "BUY"
    s["model_go"], s["baseline_go"] = [True, False], [True, True]
    s["model_version"], s["score"], s["threshold"], s["source"] = "v2", 0.7, 0.6, "replay"
    s.to_csv(files / "s.csv", index=False)
    o = pd.DataFrame({"signal_id": ["2026-10-01|A|BUY"], "date": ["2026-10-01"], "status": ["trailed"],
                      "exit_reason": ["TRAIL"], "exit_candle": ["10:30:00"], "exit_time": ["10:35:00"],
                      "exit_price": [101], "pnl_pct": [0.4], "profit": [1], "mfe_pct": [0.6],
                      "mae_pct": [-0.1], "labelled_at": ["2026-10-01T10:35:20+05:30"], "source": ["replay"]})
    o.to_csv(files / "o.csv", index=False)
    html = open_client.get("/scorecard").get_data(as_text=True)
    assert "Filter Scorecard" in html and "1 labelled, 1 still open" in html
    assert "too few" in html and "replay" in html
    assert open_client.get("/scorecard?filter=baseline").status_code == 200


def test_empty_scorecard(open_client):
    assert "No completed signals yet" in open_client.get("/scorecard").get_data(as_text=True)


# --- live view: SKIP rows carry the tracker's live state ---------------------------------

def _live_files(files, monkeypatch):
    import pandas as pd
    day = webapp.now_ist().date().isoformat()
    dec = pd.DataFrame([
        {"date": day, "time": "10:25:00", "symbol": "DMART", "direction": "SELL", "entry_price": 3626.0,
         "ORH": 3700, "ORL": 3640, "prev_close": 3700, "score": 0.69, "threshold": 0.644, "decision": "GO",
         "stop": 3686.0},
        {"date": day, "time": "09:45:00", "symbol": "KALYANKJIL", "direction": "BUY", "entry_price": 550.6,
         "ORH": 548, "ORL": 540, "prev_close": 530, "score": 0.52, "threshold": 0.644, "decision": "SKIP",
         "stop": 542.6},
        {"date": day, "time": "11:30:00", "symbol": "MARICO", "direction": "BUY", "entry_price": 797.1,
         "ORH": 795, "ORL": 785, "prev_close": 780, "score": 0.53, "threshold": 0.644, "decision": "SKIP",
         "stop": 787.1}])
    dec.to_csv(files / "dec.csv", index=False)
    monkeypatch.setattr(webapp, "DECISIONS_FILE", str(files / "dec.csv"))
    monkeypatch.setattr(webapp, "SENT_FILE", str(files / "none.csv"))

    def row(sym, d, go, price, pnl, status="open", exit_time=None):
        return {"id": f"{day}|{sym}|{d}", "symbol": sym, "direction": d, "go": go,
                "decision": "GO" if go else "SKIP", "price": price, "pnl_pct": pnl, "trail_stop": 1.0,
                "best_pct": abs(pnl) + 0.1, "worst_pct": -0.2, "status": status,
                "status_label": "open" if status == "open" else "stopped out", "exit_time": exit_time}
    snap = {"day": day, "as_of": f"{day}T14:20:20+05:30", "version": "v",
            "rows": [row("DMART", "SELL", True, 3587.1, 1.06), row("KALYANKJIL", "BUY", False, 562.0, 2.07),
                     row("MARICO", "BUY", False, 790.8, -0.79, "stopped", "13:05")]}
    (files / "tracker.json").write_text(json.dumps(snap))


def test_live_view_shows_all_signals_with_live_state_for_skips(open_client, files, monkeypatch):
    _live_files(files, monkeypatch)
    html = open_client.get("/live").get_data(as_text=True)          # default: all signals
    assert "KALYANKJIL" in html and "MARICO" in html and "DMART" in html
    assert "₹562.00" in html and "+2.07%" in html                    # SKIP: current + % from entry
    assert "stopped out 13:05" in html                                # closed SKIP: status + exit time
    assert html.count("tracked, not taken") == 2 and "SKIP (TRACKED, NOT TAKEN)" in html
    assert "1/2" in html and "+0.64%" in html                         # SKIPs in profit now, avg live P&L
    assert webapp.SELECTIVE_NOTE in html
    go_only = open_client.get("/live?show=go").get_data(as_text=True)
    assert "DMART" in go_only and "KALYANKJIL" not in go_only


def test_signal_summary_counts_and_skip_pnl():
    s = webapp.signal_summary([{"decision": "GO", "pnl_pct": 1.0}, {"decision": "SKIP", "pnl_pct": 2.0},
                               {"decision": "SKIP", "pnl_pct": -1.0}, {"decision": "SKIP", "pnl_pct": None},
                               {"decision": "STALE", "pnl_pct": 0.5}])
    assert (s["signals"], s["go"], s["skip"], s["stale"]) == (5, 1, 3, 1)
    assert (s["skip_priced"], s["skip_in_profit"], s["skip_avg_pnl"]) == (2, 1, 0.5)


def test_tracker_snapshot_carries_the_summary(open_client):
    snap = open_client.get("/api/tracker").get_json()
    assert snap["summary"]["go"] == 1 and snap["summary"]["skip"] == 0


def test_footer_says_btech_project(open_client):
    html = open_client.get("/about").get_data(as_text=True)
    assert "B.Tech project" in html and "final-year" not in html


def test_skip_report_after_close(open_client, files):
    import pandas as pd
    day = "2026-10-05"
    syms = ["A", "B", "C", "D", "E"]
    s = pd.DataFrame([{c: "" for c in shadow.SIGNAL_COLUMNS}] * 5)
    s["signal_id"] = [f"{day}|{x}|BUY" for x in syms]
    s["date"], s["time"], s["symbol"], s["direction"], s["source"] = day, "10:00:00", syms, "BUY", "live"
    s["decision"] = ["GO", "SKIP", "SKIP", "SKIP", "SKIP"]
    s["model_go"] = s["decision"] == "GO"
    s["baseline_go"], s["model_version"], s["threshold"] = s["model_go"], "v2", 0.644
    s["score"] = [0.70, 0.60, 0.55, 0.50, 0.45]
    s.to_csv(files / "s.csv", index=False)
    pnl = [1.0, 0.8, 0.3, -0.2]                                  # E (a SKIP) is still open
    o = pd.DataFrame({"signal_id": s["signal_id"][:4], "date": day, "status": "trailed", "exit_reason": "TRAIL",
                      "exit_candle": "11:00:00", "exit_time": "11:05:00", "exit_price": 1, "pnl_pct": pnl,
                      "profit": [int(p > 0) for p in pnl], "mfe_pct": 1, "mae_pct": -1,
                      "labelled_at": f"{day}T11:05:20+05:30", "source": "live"})
    o.to_csv(files / "o.csv", index=False)
    rep = webapp.skip_report(shadow.labelled_rows(), shadow._read(shadow.SIGNALS_FILE))
    assert rep["date"] == day and rep["n"] == 3 and rep["open"] == 1 and rep["wins"] == 2
    assert abs(rep["avg"] - 0.3) < 1e-9 and rep["best"][0]["symbol"] == "B"
    assert rep["rho"] == 1.0 and rep["go_n"] == 1                # higher score, better outcome, today
    html = open_client.get("/scorecard").get_data(as_text=True)
    assert "Skipped signals · 2026-10-05" in html and "2/3" in html and "1 still open" in html
