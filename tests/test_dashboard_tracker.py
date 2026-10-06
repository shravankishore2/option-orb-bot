"""Tracker and scorecard pages, and the live push stream (Server-Sent Events)."""

import functools
import json
import re

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

def _live_files(files, monkeypatch, extra=()):
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
         "stop": 787.1}, *extra])
    dec.to_csv(files / "dec.csv", index=False)
    monkeypatch.setattr(webapp, "DECISIONS_FILE", str(files / "dec.csv"))
    monkeypatch.setattr(webapp, "SENT_FILE", str(files / "none.csv"))

    def row(sym, d, go, price, pnl, initial, stop_now, status="open", exit_time=None):
        closed = status != "open"
        return {"id": f"{day}|{sym}|{d}", "symbol": sym, "direction": d, "go": go,
                "decision": "GO" if go else "SKIP", "price": price, "pnl_pct": pnl,
                "initial_stop": initial, "trail_stop": stop_now, "exit_stop": stop_now if closed else None,
                "best_pct": abs(pnl) + 0.1, "worst_pct": -0.2, "status": status,
                "status_label": "stopped out (trailing stop)" if closed else "open", "exit_time": exit_time,
                # under the what-if rule every position is still open, 0.11% better
                "alt": {"exit_v2_grace10": {"status": "open", "status_label": "open", "price": price,
                                            "pnl_pct": pnl + 0.11, "initial_stop": initial, "trail_stop": initial,
                                            "exit_stop": None, "best_pct": abs(pnl) + 0.2, "worst_pct": -0.2,
                                            "exit_time": None}}}
    snap = {"day": day, "as_of": f"{day}T14:20:20+05:30", "version": "v",
            "rows": [row("DMART", "SELL", True, 3587.1, 1.06, 3686.0, 3648.7),
                     row("KALYANKJIL", "BUY", False, 562.0, 2.07, 542.6, 551.05),
                     row("MARICO", "BUY", False, 790.8, -0.79, 787.1, 790.8, "trailed", "13:05")]}
    (files / "tracker.json").write_text(json.dumps(snap))


def _bodies(html):
    """The signal tbodies in page order: [(symbol, decision, body html)]."""
    return [(m.group(2), m.group(1), m.group(0)) for m in
            re.finditer(r'<tbody class="sig[^"]*"[^>]*data-decision="(\w+)"[^>]*data-symbol="([^"]+)".*?</tbody>', html, re.S)]


def test_live_view_is_one_table_with_go_pinned_and_live_state_for_skips(open_client, files, monkeypatch):
    _live_files(files, monkeypatch)
    html = open_client.get("/live").get_data(as_text=True)
    assert html.count('<table class="sig-table"') == 1 and "signal-panel" not in html     # one table, not BUY/SELL
    rows = _bodies(html)
    assert [r[0] for r in rows] == ["DMART", "MARICO", "KALYANKJIL"]           # GO first, then newest first
    kal = dict((r[0], r[2]) for r in rows)["KALYANKJIL"]
    assert "₹550.60 → ₹562.00" in kal and "+2.07%" in kal                    # SKIP: entry -> current, % from entry
    assert 'class="dir dir-buy"' in kal and "tracked, not taken" in kal
    assert html.count("tracked, not taken") == 2 and "SKIP (TRACKED, NOT TAKEN)" in html
    assert "1/2" in html and "+0.64%" in html and webapp.SELECTIVE_NOTE in html
    for chip in ('data-value="GO"', 'data-value="SKIP"', 'data-value="BUY"', 'data-value="SELL"',
                 'data-value="open"', 'data-value="closed"'):
        assert chip in html
    assert 'aria-sort="none"' in html and 'aria-expanded="false"' in html and "live.js" in html


def test_live_view_shows_current_initial_and_exit_stops(open_client, files, monkeypatch):
    _live_files(files, monkeypatch)
    rows = {r[0]: r[2] for r in _bodies(open_client.get("/live").get_data(as_text=True))}
    assert "₹551.05" in rows["KALYANKJIL"] and "initial ₹542.60" in rows["KALYANKJIL"]   # open: current stop
    assert "at exit · initial ₹787.10" in rows["MARICO"] and "₹790.80" in rows["MARICO"]  # closed: stop at exit
    assert "Trailed out" in rows["MARICO"] and "at 13:05" in rows["MARICO"]
    assert "Opening range high (ORH)" in rows["MARICO"] and "₹795.00" in rows["MARICO"]    # in the detail row
    for body in rows.values():
        stop_cell = body.split('data-label="Stop"')[1].split("</td>")[0]
        assert "—" not in stop_cell.split("<small")[0], "every row must show a stop"


def test_live_fragment_is_strip_and_table_only(open_client, files, monkeypatch):
    _live_files(files, monkeypatch)
    frag = open_client.get("/live?fragment=1").get_data(as_text=True)
    assert 'id="live-strip"' in frag and 'id="live-table"' in frag and "<nav" not in frag


def test_a_skip_without_a_price_yet_does_not_break_the_page(open_client, files, monkeypatch):
    day = webapp.now_ist().date().isoformat()
    _live_files(files, monkeypatch, extra=[{"date": day, "time": "14:20:00", "symbol": "UNITDSPR", "direction": "BUY",
                                           "entry_price": 1360.0, "ORH": 1350, "ORL": 1340, "prev_close": 1330,
                                           "score": 0.48, "threshold": 0.644, "decision": "SKIP", "stop": 1350.0}])
    r = open_client.get("/live")
    assert r.status_code == 200 and "UNITDSPR" in r.get_data(as_text=True)


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


# --- exit-rule toggle, side-by-side comparison, ticker chips -----------------------------

def test_the_live_view_shows_the_current_rule_by_default_with_a_what_if_toggle(open_client, files, monkeypatch):
    _live_files(files, monkeypatch)
    cur = open_client.get("/live").get_data(as_text=True)
    assert "+2.07%" in cur and "What-if view" not in cur and "10-min trail grace" in cur
    rows = {r[0]: r[2] for r in _bodies(cur)}
    assert "Trailed out" in rows["MARICO"]
    alt = open_client.get("/live?rule=exit_v2_grace10").get_data(as_text=True)
    arows = {r[0]: r[2] for r in _bodies(alt)}
    assert "What-if view" in alt and "+2.18%" in arows["KALYANKJIL"] and "Open" in arows["MARICO"]
    assert 'href="/live?time=10:30&amp;rule=exit_v2_grace10"' in alt                  # time links keep the rule
    assert "What-if view" not in open_client.get("/live?rule=nonsense").get_data(as_text=True)


def test_ticker_chips_in_time_order_with_go_and_direction(open_client, files, monkeypatch):
    import exits  # noqa: F401
    _live_files(files, monkeypatch)
    html = open_client.get("/live").get_data(as_text=True)
    chips = re.findall(r'data-filter="ticker" data-value="([^"]+)"', html)
    assert chips == ["all", "KALYANKJIL", "DMART", "MARICO"]                        # first-signal time order
    dm = html.split('data-value="DMART"')[0].rsplit("<button", 1)[1]
    assert "tchip-go" in dm
    assert 'tdir tdir-sell' in html.split('data-value="DMART"')[1].split("</button>")[0]
    assert 'id="live-tickers"' in open_client.get("/live?fragment=1").get_data(as_text=True)
    assert webapp.ticker_chips([{"symbol": "A", "time": "10:00:00", "direction": "BUY", "decision": "SKIP"},
                                {"symbol": "A", "time": "11:00:00", "direction": "SELL", "decision": "GO"}]) == \
        [{"symbol": "A", "time": "10:00", "dirs": ["BUY", "SELL"], "go": True}]


def test_scorecard_compares_the_exit_rules_on_live_signals(open_client, files):
    import pandas as pd
    import exits
    day = "2026-10-06"
    s = pd.DataFrame([{c: "" for c in shadow.SIGNAL_COLUMNS}] * 2)
    s["signal_id"] = [f"{day}|A|BUY", f"{day}|B|BUY"]
    s["date"], s["time"], s["symbol"], s["direction"], s["source"] = day, "10:00:00", ["A", "B"], "BUY", "live"
    s["decision"], s["model_go"], s["baseline_go"] = ["GO", "SKIP"], [True, False], [True, False]
    s["score"], s["threshold"], s["model_version"] = [0.7, 0.5], 0.644, "v2"
    s.to_csv(files / "s.csv", index=False)

    def out(pnls):
        return pd.DataFrame({"signal_id": s["signal_id"], "date": day, "status": "trailed", "exit_reason": "TRAIL",
                             "exit_candle": "11:00:00", "exit_time": "11:05:00", "exit_price": 1, "pnl_pct": pnls,
                             "profit": 0, "mfe_pct": 1, "mae_pct": -1, "labelled_at": f"{day}T11:05:20+05:30",
                             "source": "live", "exit_stop": 1})
    out([0.4, -0.2]).to_csv(files / "o.csv", index=False)
    out([0.6, -0.2]).to_csv(shadow.alt_outcomes_file(files / "o.csv", exits.SHADOW_RULES[0]), index=False)
    ec = webapp.exit_rule_comparison()
    assert ec["n"] == 2 and ec["go"]["cur"] == 0.4 and ec["go"]["alt"] == 0.6 and ec["all"]["better"] == 1
    html = open_client.get("/scorecard").get_data(as_text=True)
    assert "Exit rule side by side" in html and "10-min trail grace" in html
