"""ORBITAL is signals-only, and on the VM it borrows QuantRadar's Dhan token.

These tests fail if any order (or other non-market-data) endpoint could be
called, and pin down how the shared token file is read and waited for.
"""

import datetime as dt
import json
import re
from pathlib import Path

import pytest

import dhan_client as dhan

ROOT = Path(__file__).resolve().parents[1]

TRADING_PATHS = [
    "/orders", "/orders/123", "/v2/orders", "/super/orders", "/forever/orders",
    "/positions", "/positions/convert", "/holdings", "/fundlimit", "/margincalculator",
    "/killswitch", "/edis/tpin", "/trades", "/charts/../orders", "//orders",
    "/charts", "", "/marketfeedx/../orders",
]


class Response:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


@pytest.fixture
def no_network(monkeypatch):
    """Any real HTTP send fails the test."""
    def boom(*a, **k):
        raise AssertionError("a request reached the network")
    monkeypatch.setattr("requests.adapters.HTTPAdapter.send", boom)
    monkeypatch.setattr(dhan, "_throttle", lambda: None)


# --- no order path --------------------------------------------------------

@pytest.mark.parametrize("path", TRADING_PATHS)
def test_post_refuses_every_non_market_data_endpoint(path, no_network, monkeypatch):
    called = []
    monkeypatch.setattr(dhan, "load_dhan_config", lambda **k: called.append(1) or ("t", "c"))
    with pytest.raises(dhan.OrderPathBlocked):
        dhan._post(path, {"transactionType": "BUY"})
    assert not called, "credentials were loaded before the path was refused"


@pytest.mark.parametrize("path", ["/orders", "/v2/orders", "/v2/super/orders", "/v2/positions"])
def test_session_refuses_trading_endpoints_even_when_called_directly(path, no_network):
    for method in (dhan._session.post, dhan._session.get, dhan._session.delete):
        with pytest.raises(dhan.OrderPathBlocked):
            method(f"https://api.dhan.co{path}", json={})


def test_market_data_paths_are_allowed():
    for path in ("/charts/intraday", "/charts/historical", "/marketfeed/ltp",
                 "/marketfeed/ohlc", "/v2/charts/intraday"):
        dhan._check_path(path)


def test_no_order_endpoint_anywhere_in_the_live_code():
    """Source scan: no module the bot runs mentions a Dhan trading endpoint or SDK order call."""
    pattern = re.compile(r"""["'/](v2/)?(super/|forever/)?orders\b|place_order|modify_order|"""
                         r"""cancel_order|/positions\b|/killswitch|dhanhq\s*\(""")
    files = [f for f in ROOT.glob("*.py")] + list((ROOT / "templates").glob("*"))
    hits = [f"{f.name}:{i}" for f in files
            for i, line in enumerate(f.read_text(errors="ignore").splitlines(), 1)
            if pattern.search(line.split("#")[0])]
    assert hits == [], f"possible order path: {hits}"


# --- shared token file ----------------------------------------------------

def write_token(path, token, minutes=600, client="1100000000"):
    expiry = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"access_token": token, "client_id": client,
                               "expiry": expiry.isoformat()}))
    tmp.replace(path)


@pytest.fixture
def token_file(tmp_path, monkeypatch):
    path = tmp_path / "dhan_token.json"
    monkeypatch.setenv("ORBITAL_DHAN_TOKEN_FILE", str(path))
    monkeypatch.setattr(dhan, "_token_cache", {"mtime": None, "value": None})
    monkeypatch.setattr(dhan, "TOKEN_POLL_SECONDS", 0)
    return path


def test_reads_the_shared_token(token_file):
    write_token(token_file, "tok-A")
    assert dhan.load_dhan_config() == ("tok-A", "1100000000")


def test_waits_while_the_token_is_being_refreshed(token_file, monkeypatch):
    """No file (or an expired one) → wait; the refresher's new token is used."""
    write_token(token_file, "tok-old", minutes=-5)
    sleeps = []

    def refresher_finishes(_):
        sleeps.append(1)
        if len(sleeps) == 3:
            write_token(token_file, "tok-new")
    monkeypatch.setattr(dhan.time, "sleep", refresher_finishes)
    assert dhan.load_dhan_config() == ("tok-new", "1100000000")
    assert len(sleeps) == 3


def test_gives_up_with_a_credentials_error(token_file, monkeypatch):
    monkeypatch.setattr(dhan, "TOKEN_WAIT_SECONDS", 0)
    with pytest.raises(dhan.DhanError, match="credentials"):
        dhan.load_dhan_config()


def test_401_waits_for_a_new_token_and_retries_once(token_file, monkeypatch, no_network):
    write_token(token_file, "tok-A")
    seen = []

    def fake_post(url, headers, json, timeout):
        seen.append(headers["access-token"])
        if headers["access-token"] == "tok-A":
            write_token(token_file, "tok-B")          # the refresher rotates it
            return Response(401, {"errorCode": "DH-901"})
        return Response(200, {"ok": True})
    monkeypatch.setattr(dhan._session, "post", fake_post)
    assert dhan._post("/charts/intraday", {}) == {"ok": True}
    assert seen == ["tok-A", "tok-B"]


def test_a_second_401_stops_with_a_credentials_error(token_file, monkeypatch, no_network):
    write_token(token_file, "tok-A")
    monkeypatch.setattr(dhan, "TOKEN_WAIT_SECONDS", 0)
    tokens = iter(["tok-B", "tok-C"])

    def fake_post(url, headers, json, timeout):
        nxt = next(tokens, None)
        if nxt:
            write_token(token_file, nxt)
        return Response(401)
    monkeypatch.setattr(dhan._session, "post", fake_post)
    with pytest.raises(dhan.DhanError, match="credentials"):
        dhan._post("/charts/intraday", {})


def test_orbital_never_writes_the_shared_token(token_file, monkeypatch):
    write_token(token_file, "tok-A")
    before = token_file.stat().st_mtime_ns
    dhan.load_dhan_config()
    assert token_file.stat().st_mtime_ns == before
    src = (ROOT / "dhan_client.py").read_text()
    assert "generate_token" not in src and "/login" not in src


@pytest.mark.parametrize("status,body", [
    (400, {"errorType": "Order_Error", "errorCode": "DH-906", "errorMessage": "Invalid Token"}),
    (400, {"errorType": "Invalid_Authentication", "errorCode": "DH-901", "errorMessage": "expired"}),
    (401, {}),
])
def test_token_rejection_in_any_form_waits_for_the_refresher(status, body, token_file, monkeypatch, no_network):
    """Dhan's charts API rejects a dead token with HTTP 400 DH-906, not 401 — it
    must still be treated as "get a new token", never as a data error."""
    write_token(token_file, "tok-A")
    seen = []

    def fake_post(url, headers, json, timeout):
        seen.append(headers["access-token"])
        if headers["access-token"] == "tok-A":
            write_token(token_file, "tok-B")
            return Response(status, body)
        return Response(200, {"ok": True})
    monkeypatch.setattr(dhan._session, "post", fake_post)
    assert dhan._post("/charts/historical", {}) == {"ok": True}
    assert seen == ["tok-A", "tok-B"]


def test_no_data_400_is_not_a_token_problem():
    assert not dhan._token_rejected(Response(400, {"errorCode": "DH-907", "errorMessage": "No data"}))
