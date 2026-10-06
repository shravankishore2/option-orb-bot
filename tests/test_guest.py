"""Guest (read-only demo) view: /guest?k=<key>. No raw prices anywhere, no session, no way
into anything else; key rotation without a restart; rate limits; noindex."""

import functools
import json

import pandas as pd
import pytest

import auth
import guest
import shadow
import webapp

PW = "correct horse battery"
KEY = "test-guest-key-0123456789abcdef"

# Distinctive raw prices: none of them may appear in any guest response, in any format.
PRICES = [3626.35, 3587.15, 3686.45, 3648.75, 3700.15, 3640.65, 3711.35,       # DMART (GO, open)
          550.65, 562.05, 542.65, 551.05, 548.15, 540.35, 530.45,              # KALYANKJIL (SKIP, open)
          797.15, 790.85, 787.15, 795.25, 785.45, 780.55,                      # MARICO (SKIP, closed)
          4321.55]                                                             # a paper trade's entry


def tokens(p):
    return {f"{p:.2f}", f"{p:,.2f}", f"{p:.1f}", repr(p)}


@pytest.fixture
def files(tmp_path, monkeypatch):
    day = webapp.now_ist().date().isoformat()
    dec = pd.DataFrame([
        {"date": day, "time": "10:25:00", "symbol": "DMART", "direction": "SELL", "entry_price": 3626.35,
         "ORH": 3700.15, "ORL": 3640.65, "prev_close": 3711.35, "score": 0.69, "threshold": 0.644,
         "decision": "GO", "stop": 3686.45, "decided_at": "10:25:20", "age_min": 0.3},
        {"date": day, "time": "09:45:00", "symbol": "KALYANKJIL", "direction": "BUY", "entry_price": 550.65,
         "ORH": 548.15, "ORL": 540.35, "prev_close": 530.45, "score": 0.52, "threshold": 0.644,
         "decision": "SKIP", "stop": 542.65, "decided_at": "09:45:20", "age_min": 0.3},
        {"date": day, "time": "11:30:00", "symbol": "MARICO", "direction": "BUY", "entry_price": 797.15,
         "ORH": 795.25, "ORL": 785.45, "prev_close": 780.55, "score": 0.53, "threshold": 0.644,
         "decision": "SKIP", "stop": 787.15, "decided_at": "11:30:20", "age_min": 0.3}])
    dec.to_csv(tmp_path / "dec.csv", index=False)
    pd.DataFrame([{"date": day, "symbol": "DMART", "direction": "SELL", "entry_price": 3626.35,
                   "current_price": 3587.15}]).to_csv(tmp_path / "sent.csv", index=False)
    pd.DataFrame([{"date": "2026-09-30", "time": "10:00:00", "symbol": "ABB", "direction": "BUY",
                   "entry_price": 4321.55, "score": 0.7, "decision": "GO", "pnl_%": 0.4,
                   "exit_reason": "TRAIL"}]).to_csv(tmp_path / "paper.csv", index=False)

    def row(sym, d, go, entry, price, pnl, initial, stop_now, status="open", exit_time=None):
        closed = status != "open"
        return {"id": f"{day}|{sym}|{d}", "symbol": sym, "time": "10:25", "direction": d, "score": 0.6,
                "threshold": 0.644, "go": go, "decision": "GO" if go else "SKIP", "baseline_go": go,
                "entry": entry, "stop": initial, "target": None, "status": status,
                "status_label": "stopped out (trailing stop)" if closed else "open", "price": price,
                "pnl_pct": pnl, "initial_stop": initial, "trail_stop": stop_now,
                "exit_stop": stop_now if closed else None, "best_pct": abs(pnl) + 0.1, "worst_pct": -0.2,
                "exit_time": exit_time, "model_version": "v2"}
    snap = {"version": f"{day}T14:20:20+05:30/1", "day": day, "as_of": f"{day}T14:20:20+05:30",
            "rows": [row("DMART", "SELL", True, 3626.35, 3587.15, 1.08, 3686.45, 3648.75),
                     row("KALYANKJIL", "BUY", False, 550.65, 562.05, 2.07, 542.65, 551.05),
                     row("MARICO", "BUY", False, 797.15, 790.85, -0.79, 787.15, 790.85, "trailed", "13:05")]}
    (tmp_path / "tracker.json").write_text(json.dumps(snap))

    s = pd.DataFrame([{c: "" for c in shadow.SIGNAL_COLUMNS}])
    s["signal_id"], s["date"], s["time"], s["symbol"] = f"{day}|MARICO|BUY", day, "11:30:00", "MARICO"
    s["direction"], s["entry_price"], s["ORH"], s["ORL"], s["prev_close"] = "BUY", 797.15, 795.25, 785.45, 780.55
    s["decision"], s["model_go"], s["baseline_go"], s["source"] = "SKIP", False, False, "live"
    s["score"], s["threshold"], s["model_version"], s["stop"] = 0.53, 0.644, "v2", 787.15
    s.to_csv(tmp_path / "s.csv", index=False)
    o = pd.DataFrame([{"signal_id": f"{day}|MARICO|BUY", "date": day, "status": "trailed", "exit_reason": "TRAIL",
                       "exit_candle": "13:00:00", "exit_time": "13:05:00", "exit_price": 790.85, "pnl_pct": -0.79,
                       "profit": 0, "mfe_pct": 0.3, "mae_pct": -0.9, "labelled_at": f"{day}T13:05:20+05:30",
                       "source": "live", "exit_stop": 790.85}])
    o.to_csv(tmp_path / "o.csv", index=False)

    (tmp_path / "key").write_text(KEY + "\n")
    monkeypatch.setattr(webapp, "DECISIONS_FILE", str(tmp_path / "dec.csv"))
    monkeypatch.setattr(webapp, "SENT_FILE", str(tmp_path / "sent.csv"))
    monkeypatch.setattr(webapp, "PAPER_FILE", str(tmp_path / "paper.csv"))
    monkeypatch.setattr(shadow, "TRACKER_FILE", tmp_path / "tracker.json")
    monkeypatch.setattr(shadow, "SIGNALS_FILE", tmp_path / "s.csv")
    monkeypatch.setattr(shadow, "OUTCOMES_FILE", tmp_path / "o.csv")
    monkeypatch.setattr(shadow, "LIVE_DIR", tmp_path)
    monkeypatch.setattr(webapp, "GUEST_KEY_FILE", tmp_path / "key")
    monkeypatch.setattr(guest, "KEY_FILE", tmp_path / "key")
    monkeypatch.setattr(webapp, "_guest_key_cache", {"stat": None, "key": None})
    monkeypatch.setattr(webapp, "GUEST_RATE", webapp.RateWindow(1000, 60))
    monkeypatch.setattr(webapp, "GUEST_BAD_KEY", webapp.RateWindow(1000, 600))
    monkeypatch.setattr(webapp, "CHECKER", auth.PasswordCheck(PW))            # login is ON, as deployed
    return tmp_path


@pytest.fixture
def client(files):
    c = webapp.app.test_client()
    c.open = functools.partial(c.open, base_url="https://orbital.example")
    return c


GUEST_PAGES = ["/guest", "/guest?fragment=1", "/guest?time=10:30", "/guest/tracker", "/guest/api/tracker",
               "/guest/scorecard", "/guest/scorecard?filter=baseline", "/guest/performance",
               "/guest/performance?period=development"]
PRICE_KEYS = {"entry", "price", "stop", "trail_stop", "exit_stop", "initial_stop", "target", "entry_price",
              "current", "trail", "orh", "orl", "prev_close", "exit_price", "entry_raw", "orh_raw", "orl_raw",
              "prev_close_raw", "initial_raw", "trail_raw"}


def _keys(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _keys(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _keys(v)


def test_no_raw_price_anywhere_in_the_guest_view(client):
    for path in GUEST_PAGES:
        r = client.get(path + ("&" if "?" in path else "?") + "k=" + KEY)
        assert r.status_code == 200, path
        body = r.get_data(as_text=True)
        assert "₹" not in body, f"rupee sign in {path}"
        for p in PRICES:
            for t in tokens(p):
                assert t not in body, f"raw price {t} in {path}"
        if r.is_json:
            assert not PRICE_KEYS & set(_keys(r.get_json())), f"price field in {path}"
        else:
            assert webapp.GUEST_BANNER in body and "Log out" not in body, path


def test_the_guest_view_shows_the_moves_as_percentages(client):
    html = client.get(f"/guest?k={KEY}").get_data(as_text=True)
    assert "+2.07%" in html and "+1.08%" in html and "-0.79%" in html             # entry -> now
    assert "Entry beyond the range" in html and "Opening range width" in html
    snap = client.get(f"/guest/api/tracker?k={KEY}").get_json()
    kal = next(r for r in snap["rows"] if r["symbol"] == "KALYANKJIL")
    assert kal["stop_pct"] == pytest.approx((551.05 - 550.65) / 550.65 * 100)    # trailed above entry
    assert kal["initial_stop_pct"] == pytest.approx((542.65 - 550.65) / 550.65 * 100)
    dm = next(r for r in snap["rows"] if r["symbol"] == "DMART")                 # SELL: favour = price falls
    assert dm["initial_stop_pct"] < 0 and dm["stop_pct"] < 0


def test_the_leak_check_would_catch_prices(client):
    """The logged-in pages DO show these prices, so the test above isn't vacuous."""
    client.post("/api/login", json={"password": PW})
    body = client.get("/live").get_data(as_text=True) + client.get("/api/tracker").get_data(as_text=True)
    assert "₹" in body and "3,626.35" in body and "550.65" in body


def test_wrong_or_missing_key_or_no_key_file_is_a_404(client, files):
    assert client.get("/guest").status_code == 404
    assert client.get("/guest?k=wrong").status_code == 404
    assert client.get("/guest/api/tracker?k=" + KEY[:-1]).status_code == 404
    (files / "key").unlink()
    assert client.get(f"/guest?k={KEY}").status_code == 404                      # guest view off


def test_the_key_opens_nothing_else(client):
    for path in ("/live", "/tracker", "/scorecard", "/historical", "/performance", "/about"):
        assert client.get(f"{path}?k={KEY}").status_code == 302, path             # still the login page
    for path in ("/api/tracker", "/api/tracker/stream", "/api/me"):
        assert client.get(f"{path}?k={KEY}").status_code == 401, path
    assert client.post(f"/guest?k={KEY}").status_code == 405                      # read-only: GET only
    r = client.get(f"/guest?k={KEY}")
    assert "Set-Cookie" not in r.headers                                           # no session is created
    html = r.get_data(as_text=True)
    assert f"/guest/tracker?k={KEY}" in html and f"/guest/scorecard?k={KEY}" in html
    assert 'href="/historical"' not in html and 'href="/about"' not in html


def test_rotating_the_key_takes_effect_without_a_restart(client, files):
    assert client.get(f"/guest?k={KEY}").status_code == 200
    new = guest.rotate()
    assert (files / "key").stat().st_mode & 0o777 == 0o600
    assert client.get(f"/guest?k={KEY}").status_code == 404
    assert client.get(f"/guest?k={new}").status_code == 200


def test_rate_limits(client, monkeypatch):
    monkeypatch.setattr(webapp, "GUEST_RATE", webapp.RateWindow(3, 60))
    codes = [client.get(f"/guest/api/tracker?k={KEY}").status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    monkeypatch.setattr(webapp, "GUEST_RATE", webapp.RateWindow(1000, 60))
    monkeypatch.setattr(webapp, "GUEST_BAD_KEY", webapp.RateWindow(2, 600))
    codes = [client.get("/guest?k=nope").status_code for _ in range(3)]
    assert codes == [404, 404, 429]
    r = client.get(f"/guest?k={KEY}")                                              # the right key still works
    assert r.status_code == 200


def test_noindex_everywhere_the_guest_goes(client):
    r = client.get(f"/guest?k={KEY}")
    assert r.headers["X-Robots-Tag"] == "noindex, nofollow, noarchive"
    assert '<meta name="robots" content="noindex, nofollow">' in r.get_data(as_text=True)
    robots = client.get("/robots.txt")
    assert robots.status_code == 200 and "Disallow: /" in robots.get_data(as_text=True)
