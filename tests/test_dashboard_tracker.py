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
    assert 'data-tab="go"' in html and 'data-tab="nogo"' in html


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
