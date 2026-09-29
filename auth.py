"""Dashboard login: one password, an HMAC-signed session cookie, lockout on guessing.

Same scheme as QuantRadar's dashboard (newsalert/web/auth.py), so both sites on
the VM behave alike. The password comes from ORBITAL_DASHBOARD_PASSWORD or the
file named by ORBITAL_DASHBOARD_PASSWORD_FILE. Sessions are signed with a
per-process random key unless ORBITAL_DASHBOARD_SECRET is set, so restarting
the server logs everyone out. Five wrong passwords within 5 minutes lock login
for 60 s.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path

COOKIE = "orbital_session"


def configured_password():
    """The dashboard password, or "" when none is configured (local use)."""
    pw = os.getenv("ORBITAL_DASHBOARD_PASSWORD", "")
    path = os.getenv("ORBITAL_DASHBOARD_PASSWORD_FILE")
    if not pw and path:
        pw = Path(path).expanduser().read_text().strip()
    return pw


class SessionSigner:
    def __init__(self, key=None, ttl_s=12 * 3600, clock=time.time):
        self.key = key or secrets.token_bytes(32)
        self.ttl_s, self.clock = ttl_s, clock

    def _mac(self, payload):
        return base64.urlsafe_b64encode(hmac.new(self.key, payload, hashlib.sha256).digest()).decode().rstrip("=")

    def issue(self):
        payload = base64.urlsafe_b64encode(json.dumps(
            {"exp": self.clock() + self.ttl_s, "n": secrets.token_hex(8)}).encode()).decode().rstrip("=")
        return f"{payload}.{self._mac(payload.encode())}"

    def valid(self, cookie):
        if not cookie or cookie.count(".") != 1:
            return False
        payload, mac = cookie.split(".")
        if not hmac.compare_digest(mac, self._mac(payload.encode())):
            return False
        try:
            data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            return float(data["exp"]) > self.clock()
        except (ValueError, KeyError, TypeError):
            return False


class PasswordCheck:
    """Constant-time password comparison with a lockout after repeated failures."""

    def __init__(self, password, max_failures=5, window_s=300, lockout_s=60, clock=time.time):
        if not password:
            raise ValueError("dashboard password is empty")
        self._digest = hashlib.sha256(password.encode()).digest()
        self.max_failures, self.window_s, self.lockout_s, self.clock = max_failures, window_s, lockout_s, clock
        self._failures = []
        self._locked_until = 0.0

    def locked_for(self):
        return max(0.0, self._locked_until - self.clock())

    def check(self, attempt):
        if self.locked_for() > 0:
            return False
        ok = hmac.compare_digest(hashlib.sha256(attempt.encode()).digest(), self._digest)
        now = self.clock()
        if ok:
            self._failures.clear()
            return True
        self._failures = [t for t in self._failures if now - t < self.window_s] + [now]
        if len(self._failures) >= self.max_failures:
            self._locked_until = now + self.lockout_s
            self._failures.clear()
        return False
