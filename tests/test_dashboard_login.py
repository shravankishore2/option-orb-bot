"""Dashboard login: password page (no browser popup), signed cookie, lockout, logout."""

import functools

import pytest

import auth
import webapp

PW = "correct horse battery"


@pytest.fixture
def client(monkeypatch):
    clock = {"t": 1_000_000.0}
    tick = lambda: clock["t"]
    monkeypatch.setattr(webapp, "CHECKER", auth.PasswordCheck(PW, clock=tick))
    monkeypatch.setattr(webapp, "SIGNER", auth.SessionSigner(ttl_s=3600, clock=tick))
    c = webapp.app.test_client()
    c.open = functools.partial(c.open, base_url="https://orbital.example")   # as served by Caddy
    c.clock = clock
    return c


def login(c, pw=PW):
    return c.post("/api/login", json={"password": pw})


def test_pages_redirect_to_the_login_page_not_a_basic_auth_popup(client):
    for path in ("/", "/live", "/historical", "/performance", "/about"):
        r = client.get(path)
        assert r.status_code == 302 and "/login" in r.headers["Location"], path
        assert "WWW-Authenticate" not in r.headers
    page = client.get("/login")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert 'type="password"' in html and 'name="username"' not in html


def test_login_sets_a_signed_secure_httponly_cookie(client):
    r = login(client)
    assert r.status_code == 204
    cookie = r.headers["Set-Cookie"]
    assert cookie.startswith(auth.COOKIE + "=")
    for flag in ("Secure", "HttpOnly", "SameSite=Strict", "Path=/"):
        assert flag in cookie
    assert client.get("/about").status_code == 200


def test_forged_or_expired_cookie_is_rejected(client):
    client.set_cookie(auth.COOKIE, "eyJleHAiOiA5OTk5OTk5OTk5fQ.forged")
    assert client.get("/live").status_code == 302
    login(client)
    assert client.get("/live").status_code == 200
    client.clock["t"] += 3601
    assert client.get("/live").status_code == 302


def test_wrong_password(client):
    r = login(client, "nope")
    assert r.status_code == 401 and r.get_json() == {"error": "wrong password"}
    assert "Set-Cookie" not in r.headers


def test_lockout_after_five_wrong_tries(client):
    codes = [login(client, "nope").status_code for _ in range(5)]
    assert codes == [401, 401, 401, 401, 429]
    r = login(client)                       # even the right password waits out the lockout
    assert r.status_code == 429 and r.get_json()["retry_after"] > 0
    client.clock["t"] += 61
    assert login(client).status_code == 204


def test_logout_clears_the_session(client):
    login(client)
    assert client.get("/live").status_code == 200
    r = client.post("/logout")
    assert r.status_code == 303 and r.headers["Location"].endswith("/login")
    assert "Max-Age=0" in r.headers["Set-Cookie"] or "Expires=Thu, 01 Jan 1970" in r.headers["Set-Cookie"]
    assert client.get("/live").status_code == 302


def test_form_fallback_and_no_open_redirect(client):
    r = client.post("/login", data={"password": PW, "next": "//evil.example/x"},
                    base_url="https://orbital.example")
    assert r.status_code == 303 and r.headers["Location"].endswith("/live")
    r = client.post("/login", data={"password": "nope", "next": "/about"})
    assert r.status_code == 401 and "Wrong password." in r.get_data(as_text=True)


def test_secure_flag_comes_from_the_proxy_header(client):
    plain = webapp.app.test_client()                  # waitress sees plain http from Caddy
    r = plain.post("/api/login", json={"password": PW}, headers={"X-Forwarded-Proto": "https"})
    assert "Secure" in r.headers["Set-Cookie"]
    r = plain.post("/api/login", json={"password": PW})     # local http: no Secure, or it'd never be sent
    assert "Secure" not in r.headers["Set-Cookie"]


def test_deployed_mode_always_marks_the_cookie_secure(client, monkeypatch):
    monkeypatch.setattr(webapp, "REQUIRE_LOGIN", True)
    r = webapp.app.test_client().post("/api/login", json={"password": PW})   # plain http, no header
    assert "Secure" in r.headers["Set-Cookie"]


def test_static_files_are_public_but_carry_no_data(client):
    """CSS/JS/figures are served without a session (the guest view needs them); data never is."""
    assert client.get("/static/style.css").status_code == 200
    assert client.get("/api/tracker").status_code == 401


def test_session_lasts_30_days(client):
    r = login(client)
    assert f"Max-Age={30 * 24 * 3600}" in r.headers["Set-Cookie"]


def test_session_key_survives_a_restart_but_not_a_password_change():
    cookie = auth.SessionSigner(auth.signing_key(PW)).issue()
    assert auth.SessionSigner(auth.signing_key(PW)).valid(cookie)             # restarted process
    assert not auth.SessionSigner(auth.signing_key("new password")).valid(cookie)
    assert auth.signing_key(PW, "explicit secret") == b"explicit secret"
    assert auth.signing_key("") is None                                        # local, no password
