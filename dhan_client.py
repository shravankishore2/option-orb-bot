# dhan_client.py — DhanHQ market data layer (replaces yfinance)
#
# Why: yfinance serves NSE quotes on a ~15 minute delay, which makes an
# intraday ORB bot act on stale prices. The Dhan Data API is real-time and
# already paid for, so every price in this project now comes from here.
#
# Exposes yfinance-shaped helpers so the rest of the project barely changes:
#   get_ltp(symbols)     -> {symbol: live price}   (one batched call)
#   get_intraday(...)    -> DataFrame[Open, High, Low, Close, Volume], IST index
#   get_daily(...)       -> same, one row per session
#   get_prev_session(..) -> last COMPLETED daily candle

import os
import time
import threading
import io
import configparser
import datetime as dt
from collections import deque
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = BASE_DIR / "config.ini"

API_BASE = "https://api.dhan.co/v2"
IST = "Asia/Kolkata"

# NSE cash segment — every symbol in this project is a Nifty 200 equity.
EXCHANGE_SEGMENT = "NSE_EQ"
INSTRUMENT = "EQUITY"

# Dhan publishes the full tradable universe as a CSV; we cache it in data/.
SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
SCRIP_MASTER_CACHE = DATA_DIR / "dhan_nse_equities.csv"
SCRIP_MASTER_MAX_AGE_HOURS = 24

# Data APIs 429 on the 6th request inside a second (verified against live API).
MAX_REQUESTS_PER_SEC = 4
MAX_LTP_BATCH = 1000

# Dhan rejects intraday spans over 90 days (DH-905), so chunk at the cap.
MAX_INTRADAY_DAYS = 90

INTERVAL_1M = "1"
INTERVAL_5M = "5"
INTERVAL_15M = "15"
INTERVAL_60M = "60"

OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


class DhanError(RuntimeError):
    """Raised when Dhan rejects a request or credentials are unusable."""


def _token_rejected(response):
    """Dhan rejects a bad token with 401/403 on some endpoints but HTTP 400 on
    others: /charts/* answers 400 {"errorCode": "DH-906", "errorMessage":
    "Invalid Token"} (seen 2026-10-02), and DH-901 is "invalid or expired access
    token". All of these mean "get a new token", not "bad data"."""
    if response.status_code in (401, 403):
        return True
    if response.status_code != 400:
        return False
    text = response.text or ""
    return "DH-901" in text or ("DH-906" in text and "token" in text.lower())


def _no_data(err):
    """Dhan answers a range with no candles (e.g. a daily bar it hasn't
    published yet) with HTTP 400 DH-907 instead of an empty list."""
    return "DH-907" in str(err)


# ---------------------------------------------------------------------------
# CREDENTIALS
# ---------------------------------------------------------------------------
#
# One token per Dhan account, one refresher. On the shared VM, ORBITAL never
# logs in to Dhan: QuantRadar's token service owns the account's token and
# rewrites it atomically each trading morning as JSON
# {"client_id", "access_token", "expiry"}. ORBITAL reads that file
# ([dhan] token_file in config.ini, or ORBITAL_DHAN_TOKEN_FILE) and, while a
# refresh is in progress (token missing, expired, or just rejected by Dhan),
# waits for the refresher instead of logging in itself.
#
# Without a token file (local research) a static token from the environment
# or config.ini is used, as before.

TOKEN_WAIT_SECONDS = 15 * 60      # how long to wait for the refresher before giving up
TOKEN_POLL_SECONDS = 30
TOKEN_EXPIRY_MARGIN = dt.timedelta(minutes=2)

_credentials = None               # static-token mode only
_token_cache = {"mtime": None, "value": None}


def _config():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config["dhan"] if "dhan" in config else config["DEFAULT"]


def token_file_path():
    """The shared token file, or None when running on a static token."""
    raw = os.getenv("ORBITAL_DHAN_TOKEN_FILE") or _config().get("token_file")
    return Path(raw).expanduser() if raw else None


def _read_token_file(path):
    """(access_token, client_id, expiry) or None if absent, partial or unreadable.

    The refresher writes via rename, so a reader never sees half a file; a
    missing or unparsable file just means "not ready yet".
    """
    try:
        st = path.stat()
        mtime = (str(path), st.st_mtime_ns, st.st_size)
        if _token_cache["mtime"] == mtime:
            return _token_cache["value"]
        import json
        d = json.loads(path.read_text())
        value = (d["access_token"].strip(), str(d["client_id"]).strip(),
                 dt.datetime.fromisoformat(d["expiry"]))
    except (OSError, ValueError, KeyError, AttributeError):
        return None
    _token_cache.update(mtime=mtime, value=value)
    return value


def _usable(expiry):
    now = dt.datetime.now(expiry.tzinfo) if expiry.tzinfo else dt.datetime.now()
    return expiry - now > TOKEN_EXPIRY_MARGIN


def load_dhan_config(reject=None):
    """Return (access_token, client_id).

    Token-file mode: wait (up to TOKEN_WAIT_SECONDS) for a usable token that is
    not `reject` — the one Dhan just refused. Static mode: env vars win over
    config.ini.
    """
    global _credentials

    path = token_file_path()
    if path is not None:
        deadline = time.monotonic() + TOKEN_WAIT_SECONDS
        announced = False
        while True:
            t = _read_token_file(path)
            if t and t[0] != reject and _usable(t[2]):
                return t[0], t[1]
            if time.monotonic() >= deadline:
                why = ("missing or unreadable" if t is None else
                       "still the token Dhan rejected" if t[0] == reject else f"expired at {t[2]}")
                raise DhanError(
                    f"Dhan credentials unavailable: the shared token file {path} is {why} "
                    f"after waiting {TOKEN_WAIT_SECONDS // 60} min for the refresher. "
                    f"Check the token service (e.g. newsalert-token.service).")
            if not announced:
                print(f"⏳ waiting for the Dhan token refresher ({path})", flush=True)
                announced = True
            time.sleep(TOKEN_POLL_SECONDS)

    if _credentials is not None:
        return _credentials

    token = os.getenv("DHAN_ACCESS_TOKEN")
    client_id = os.getenv("DHAN_CLIENT_ID")

    if not token or not client_id:
        section = _config()
        token = token or section.get("dhan_access_token") or section.get("access_token")
        client_id = client_id or section.get("dhan_client_id") or section.get("client_id")

    if not token:
        raise DhanError(
            "Dhan credentials missing: set [dhan] token_file (shared token service), "
            "or DHAN_ACCESS_TOKEN / dhan_access_token for a static token"
        )

    if not client_id:
        raise DhanError(
            "Dhan client id missing. Set DHAN_CLIENT_ID or add "
            "dhan_client_id under [dhan] in config.ini"
        )

    _credentials = (token.strip(), client_id.strip())
    return _credentials


# ---------------------------------------------------------------------------
# SIGNALS ONLY — market data, never orders
# ---------------------------------------------------------------------------
#
# ORBITAL is an alert/paper system. This is an ALLOWLIST, not a blocklist:
# any Dhan path outside it — orders, super/forever orders, positions,
# holdings, funds, eDIS, kill switch — is refused before a request is built,
# both in _post() and in the HTTP session itself.

ALLOWED_PATH_PREFIXES = ("/charts/", "/marketfeed/")


class OrderPathBlocked(DhanError):
    """Raised for any Dhan endpoint that isn't read-only market data."""


def _check_path(path):
    import posixpath
    clean = posixpath.normpath("/" + path.lstrip("/"))           # defeats /charts/../orders
    if clean.startswith("/v2/"):
        clean = clean[3:]
    if not any(clean.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES):
        raise OrderPathBlocked(f"ORBITAL is signals-only: refusing Dhan endpoint {path!r}")
    return clean


class _MarketDataOnlySession(requests.Session):
    """A requests session that will not send anything to Dhan's trading API
    except market-data paths — a second lock behind _post()."""

    def request(self, method, url, *args, **kwargs):
        from urllib.parse import urlparse
        u = urlparse(url)
        if u.hostname and u.hostname.endswith("dhan.co") and u.hostname.startswith("api"):
            _check_path(u.path)
        return super().request(method, url, *args, **kwargs)


# ---------------------------------------------------------------------------
# HTTP (rate limited + retrying)
# ---------------------------------------------------------------------------
#
# Dhan's data-API limit (5 req/s) is per ACCOUNT, and QuantRadar shares the
# account. Defaults are ORBITAL-alone values; on the shared VM, config.ini sets
#   rate_per_sec = 3         (leaves QuantRadar headroom)
#   quiet_seconds = 57-4     (no requests around each minute boundary, when
#                             QuantRadar makes its once-a-minute batched call)

def _int_setting(name, env, default):
    raw = os.getenv(env) or _config().get(name)
    return int(raw) if raw else default


def _quiet_window():
    raw = os.getenv("ORBITAL_DHAN_QUIET_SECONDS") or _config().get("quiet_seconds")
    if not raw:
        return None
    a, b = (int(x) for x in raw.split("-"))
    return a, b


MAX_REQUESTS_PER_SEC = _int_setting("rate_per_sec", "ORBITAL_DHAN_RPS", MAX_REQUESTS_PER_SEC)
QUIET_SECONDS = _quiet_window()

_session = _MarketDataOnlySession()
_rate_lock = threading.Lock()
_recent_calls = deque()


def _in_quiet(second):
    a, b = QUIET_SECONDS
    return (a <= second or second < b) if a > b else (a <= second < b)


def _throttle():
    """Block until firing another request stays under the per-second cap and
    outside the quiet window."""
    with _rate_lock:
        while True:
            if QUIET_SECONDS:
                sec = dt.datetime.now().second
                if _in_quiet(sec):
                    time.sleep(1.0 - dt.datetime.now().microsecond / 1e6)
                    continue

            now = time.monotonic()

            while _recent_calls and now - _recent_calls[0] >= 1.0:
                _recent_calls.popleft()

            if len(_recent_calls) < MAX_REQUESTS_PER_SEC:
                _recent_calls.append(now)
                return

            time.sleep(1.0 - (now - _recent_calls[0]))


def _post(path, payload, retries=5):
    """POST to the Dhan API, retrying rate limits and flaky DNS/connections.

    A rejected token (401/403) in token-file mode means the shared token was
    refreshed or is being refreshed: wait for the refresher's new token and
    retry once.
    """
    _check_path(path)
    token, client_id = load_dhan_config()
    url = f"{API_BASE}{path}"
    last_error = None
    reauthed = False

    for attempt in range(retries + 1):
        _throttle()

        headers = {
            "access-token": token,
            "client-id": client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            response = _session.post(url, headers=headers, json=payload, timeout=30)
        except requests.exceptions.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"
            time.sleep(min(2.0 * (attempt + 1), 8.0))
            continue

        if response.status_code == 429:
            last_error = "rate limited (DH-904)"
            time.sleep(1.0 * (attempt + 1))
            continue

        if _token_rejected(response):
            if token_file_path() is not None and not reauthed:
                token, client_id = load_dhan_config(reject=token)
                reauthed = True
                continue
            raise DhanError(
                f"Dhan rejected the credentials ({response.status_code}). "
                f"Access tokens expire — refresh the token (shared token service, or web.dhan.co). "
                f"Response: {response.text[:200]}"
            )

        if response.status_code != 200:
            raise DhanError(f"Dhan {path} failed [{response.status_code}]: {response.text[:200]}")

        return response.json()

    raise DhanError(f"Dhan {path} unreachable after {retries + 1} attempts: {last_error}")


# ---------------------------------------------------------------------------
# INSTRUMENT MASTER  (symbol -> securityId)
# ---------------------------------------------------------------------------

_security_ids = None


def _cache_is_fresh():
    if not SCRIP_MASTER_CACHE.exists():
        return False

    age_hours = (time.time() - SCRIP_MASTER_CACHE.stat().st_mtime) / 3600
    return age_hours < SCRIP_MASTER_MAX_AGE_HOURS


def _download_scrip_master():
    """Fetch Dhan's full instrument master and cache just the NSE cash names.

    The published CSV is ~33 MB of every tradable contract; we only ever need
    the couple of thousand NSE equities, so the cache holds only those.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # (connect, read) — bounded so a DNS/network stall can't hang a trading
    # cycle; callers fall back to the cached copy.
    response = requests.get(SCRIP_MASTER_URL, timeout=(10, 60))
    response.raise_for_status()

    master = pd.read_csv(io.BytesIO(response.content), low_memory=False)

    equities = master[
        (master["EXCH_ID"] == "NSE")
        & (master["SEGMENT"] == "E")
        & (master["SERIES"].isin(["EQ", "BE"]))
    ]

    # INSTRUMENT_TYPE separates ordinary shares ("ES") from ETFs, REITs, InvITs.
    equities = equities[["UNDERLYING_SYMBOL", "SECURITY_ID", "SERIES", "LOT_SIZE", "INSTRUMENT_TYPE"]]

    equities.to_csv(SCRIP_MASTER_CACHE, index=False)


def _load_security_ids():
    """Build {SYMBOL: securityId} for NSE cash equities, refreshing daily."""
    global _security_ids

    if _security_ids is not None:
        return _security_ids

    if not _cache_is_fresh():
        try:
            print("📥 Refreshing Dhan instrument master...")
            _download_scrip_master()
        except Exception as e:
            if not SCRIP_MASTER_CACHE.exists():
                raise DhanError(f"Could not download Dhan instrument master: {e}")
            print(f"⚠️ Instrument master refresh failed ({e}) — using cached copy.")

    equities = pd.read_csv(SCRIP_MASTER_CACHE, low_memory=False)

    symbols = equities["UNDERLYING_SYMBOL"].astype(str).str.upper().str.strip()

    _security_ids = dict(zip(symbols, equities["SECURITY_ID"].astype(int)))

    print(f"✅ Mapped {len(_security_ids)} NSE equities from Dhan instrument master.")
    return _security_ids


def get_security_id(symbol):
    """Return Dhan securityId for an NSE symbol, or None if not tradable."""
    return _load_security_ids().get(str(symbol).upper().strip())


def resolve_symbols(symbols):
    """Split symbols into ({symbol: securityId}, [unmapped symbols])."""
    resolved = {}
    unmapped = []

    for symbol in symbols:
        symbol = str(symbol).upper().strip()
        security_id = get_security_id(symbol)

        if security_id is None:
            unmapped.append(symbol)
        else:
            resolved[symbol] = security_id

    return resolved, unmapped


# ---------------------------------------------------------------------------
# LIVE PRICES
# ---------------------------------------------------------------------------

def get_ltp(symbols):
    """Real-time last traded price for many symbols in one request.

    Returns {symbol: price}; symbols Dhan cannot price are simply absent.
    """
    resolved, unmapped = resolve_symbols(symbols)

    if unmapped:
        print(f"⚠️ Not on Dhan NSE cash list, skipping: {', '.join(sorted(unmapped))}")

    if not resolved:
        return {}

    by_security_id = {str(sid): sym for sym, sid in resolved.items()}
    security_ids = list(resolved.values())

    prices = {}

    for start in range(0, len(security_ids), MAX_LTP_BATCH):
        batch = security_ids[start:start + MAX_LTP_BATCH]

        payload = _post("/marketfeed/ltp", {EXCHANGE_SEGMENT: batch})

        quotes = (payload.get("data") or {}).get(EXCHANGE_SEGMENT, {})

        for security_id, quote in quotes.items():
            price = quote.get("last_price")
            symbol = by_security_id.get(str(security_id))

            if symbol and price:
                prices[symbol] = float(price)

    return prices


def get_session_ohlc(symbols):
    """{symbol: (open, high, low, close)} from the quote feed, batched.

    Outside market hours this is the LAST COMPLETED session with NSE's OFFICIAL
    close (a 30-minute weighted average — it can differ from the last 5-minute
    candle's close by a few tenths of a percent). The live bot snapshots it so
    the previous close is right even when Dhan hasn't published the daily bar.
    """
    resolved, _ = resolve_symbols(symbols)
    by_id = {str(sid): sym for sym, sid in resolved.items()}
    ids = list(resolved.values())
    out = {}
    for start in range(0, len(ids), MAX_LTP_BATCH):
        payload = _post("/marketfeed/ohlc", {EXCHANGE_SEGMENT: ids[start:start + MAX_LTP_BATCH]})
        for sid, q in ((payload.get("data") or {}).get(EXCHANGE_SEGMENT, {})).items():
            o = q.get("ohlc") or {}
            sym = by_id.get(str(sid))
            if sym and all(o.get(k) for k in ("open", "high", "low", "close")):
                out[sym] = (float(o["open"]), float(o["high"]), float(o["low"]), float(o["close"]))
    return out


def get_latest_price(symbol):
    """Real-time price for a single symbol, or None."""
    return get_ltp([symbol]).get(str(symbol).upper().strip())


# ---------------------------------------------------------------------------
# CANDLES
# ---------------------------------------------------------------------------

SESSION_OPEN = dt.time(9, 15)
SESSION_CLOSE = dt.time(15, 30)


def regular_session(frame):
    """Drop candles outside 09:15-15:30 IST.

    Dhan's intraday data occasionally carries pre-open auction candles
    (e.g. 09:07:01), special evening sessions (Diwali Muhurat trading) and, for
    the NIFTY index, stray stamps at odd hours. Any of those would become the
    "day's open" for move-from-open, breadth and sector features.
    """
    if frame is None or frame.empty:
        return frame
    t = frame.index.time
    return frame[(t >= SESSION_OPEN) & (t < SESSION_CLOSE)]


def _to_frame(payload):
    """Turn Dhan's column-oriented chart response into an IST OHLCV frame."""
    timestamps = payload.get("timestamp") or []

    if not timestamps:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    frame = pd.DataFrame({
        "Open": payload.get("open", []),
        "High": payload.get("high", []),
        "Low": payload.get("low", []),
        "Close": payload.get("close", []),
        "Volume": payload.get("volume", []),
    })

    # Dhan returns true UTC epoch seconds; converting to IST lands on 09:15.
    index = pd.to_datetime(pd.Series(timestamps), unit="s", utc=True).dt.tz_convert(IST)

    frame.index = pd.DatetimeIndex(index, name="Datetime")

    return frame.dropna().sort_index()


def _as_date(value):
    if value is None:
        return None
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return pd.to_datetime(value).date()


def get_intraday(symbol, interval=INTERVAL_5M, from_date=None, to_date=None):
    """Intraday candles for a symbol. interval is one of 1/5/15/25/60 minutes.

    Unlike yfinance (60 days of intraday history), Dhan serves years of it.
    """
    security_id = get_security_id(symbol)

    if security_id is None:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    today = dt.datetime.now(tz=dt.timezone.utc).astimezone(
        dt.timezone(dt.timedelta(hours=5, minutes=30))
    ).date()

    to_date = _as_date(to_date) or today
    from_date = _as_date(from_date) or to_date

    chunks = []
    window_start = from_date

    while window_start <= to_date:
        window_end = min(window_start + dt.timedelta(days=MAX_INTRADAY_DAYS - 1), to_date)

        # Dhan omits the MOST RECENT session unless toDate is past it (verified:
        # 22-Sep→22-Sep returns nothing the next morning, 22→23 returns the 22nd;
        # older sessions are inclusive). Asking one day beyond and trimming makes
        # the range inclusive in every case, including "today" live.
        try:
            payload = _post("/charts/intraday", {
                "securityId": str(security_id),
                "exchangeSegment": EXCHANGE_SEGMENT,
                "instrument": INSTRUMENT,
                "interval": str(interval),
                "oi": False,
                "fromDate": window_start.isoformat(),
                "toDate": (window_end + dt.timedelta(days=1)).isoformat(),
            })
        except DhanError as e:
            if not _no_data(e):
                raise
            payload = {}

        chunk = _to_frame(payload)
        if not chunk.empty:
            chunk = chunk[(chunk.index.date >= window_start) & (chunk.index.date <= window_end)]

        if not chunk.empty:
            chunks.append(chunk)

        window_start = window_end + dt.timedelta(days=1)

    if not chunks:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    combined = pd.concat(chunks)
    return regular_session(combined[~combined.index.duplicated(keep="last")].sort_index())


def get_daily(symbol, from_date=None, to_date=None, days=30):
    """Daily candles. Dhan only returns COMPLETED sessions — today is absent."""
    security_id = get_security_id(symbol)

    if security_id is None:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    today = dt.datetime.now(tz=dt.timezone.utc).astimezone(
        dt.timezone(dt.timedelta(hours=5, minutes=30))
    ).date()

    to_date = _as_date(to_date) or today
    from_date = _as_date(from_date) or (to_date - dt.timedelta(days=days))

    try:
        payload = _post("/charts/historical", {
            "securityId": str(security_id),
            "exchangeSegment": EXCHANGE_SEGMENT,
            "instrument": INSTRUMENT,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date.isoformat(),
            "toDate": to_date.isoformat(),
        })
    except DhanError as e:
        if not _no_data(e):
            raise
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    return _to_frame(payload)


def get_prev_session(symbol, before=None, lookback_days=15):
    """Last completed daily candle strictly before `before` (default: today).

    Returns (open, high, low, close) or (None, None, None, None).

    Dhan already excludes the running session, but filtering on the date makes
    this correct after the close too, when today becomes a completed candle.
    """
    before = _as_date(before) or dt.datetime.now(tz=dt.timezone.utc).astimezone(
        dt.timezone(dt.timedelta(hours=5, minutes=30))
    ).date()

    daily = get_daily(
        symbol,
        from_date=before - dt.timedelta(days=lookback_days),
        to_date=before,
    )

    if daily.empty:
        return None, None, None, None

    completed = daily[daily.index.date < before]

    if completed.empty:
        return None, None, None, None

    prev = completed.iloc[-1]

    return (
        float(prev["Open"]),
        float(prev["High"]),
        float(prev["Low"]),
        float(prev["Close"]),
    )


if __name__ == "__main__":
    print("🔌 Dhan connectivity check\n")

    load_dhan_config()

    for sym in ["RELIANCE", "TCS", "INFY"]:
        print(f"{sym:<10} securityId={get_security_id(sym)}")

    print("\nLive prices:", get_ltp(["RELIANCE", "TCS", "INFY"]))

    candles = get_intraday("RELIANCE", interval=INTERVAL_5M)
    print(f"\nRELIANCE 5m candles today: {len(candles)}")
    if not candles.empty:
        print(candles.tail(3))

    print("\nRELIANCE prev session (O,H,L,C):", get_prev_session("RELIANCE"))
