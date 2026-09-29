# history_cache.py — on-disk cache of Dhan candles.
#
# Pulling years of 5-minute data for 200 symbols is a ~10 minute API job; the
# strategy replay on top of it takes seconds and gets re-run constantly. So we
# fetch once, store per symbol under data/history/, and replay from disk.

import datetime as dt
import os
from pathlib import Path

import pandas as pd

import dhan_client as dhan

BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "data" / "history"

IST = "Asia/Kolkata"


def _cache_path(symbol, tag):
    safe = str(symbol).upper().strip().replace("/", "_").replace("&", "_AND_")
    return CACHE_DIR / f"{safe}_{tag}.csv.gz"


def _read_cache(path):
    if not path.exists():
        return None

    try:
        frame = pd.read_csv(path, compression="gzip")
    except Exception:
        return None

    if frame.empty or "Datetime" not in frame.columns:
        return None

    index = pd.to_datetime(frame["Datetime"], utc=True).dt.tz_convert(IST)

    frame = frame.drop(columns=["Datetime"])
    frame.index = pd.DatetimeIndex(index, name="Datetime")

    return frame


def _write_cache(path, frame):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    out = frame.copy()
    out.insert(0, "Datetime", out.index.strftime("%Y-%m-%dT%H:%M:%S%z"))

    out.to_csv(path, index=False, compression="gzip")


def _covers(frame, start, end):
    """True if the cached frame already spans the requested window."""
    if frame is None or frame.empty:
        return False

    cached_start = frame.index[0].date()
    cached_end = frame.index[-1].date()

    # Allow a few days of slack at the edges for weekends and holidays.
    return cached_start <= start + dt.timedelta(days=5) and cached_end >= end - dt.timedelta(days=5)


def _missing_ranges(cached, start, end):
    """Date ranges inside [start, end] that the cache doesn't hold yet."""
    if cached is None or cached.empty:
        return [(start, end)]

    have_start = cached.index[0].date()
    have_end = cached.index[-1].date()
    slack = dt.timedelta(days=5)          # weekends / holidays at the edges

    ranges = []
    # Head: tolerate a few days (listing dates, holidays) — a stock listed
    # after `start` would otherwise be re-requested on every call.
    if start + slack < have_start:
        ranges.append((start, have_start - dt.timedelta(days=1)))
    # Tail: exact. The live bot needs YESTERDAY's candle for the previous
    # close; "within five days" would silently hand it an older session.
    # If the gap is only a weekend or holiday the request just returns empty.
    if end > have_end:
        ranges.append((have_end + dt.timedelta(days=1), end))
    return ranges


def offline():
    """ORBITAL_OFFLINE=1: serve only what's cached, never call the API.

    Research steps run offline: the fetch step downloads exactly the spans
    each stock was in the index, and nothing afterwards should widen them.
    """
    return os.getenv("ORBITAL_OFFLINE") == "1"


def _get(symbol, start, end, tag, fetcher, refresh):
    path = _cache_path(symbol, tag)
    cached = None if refresh else _read_cache(path)
    if cached is not None and tag.endswith("m"):
        cached = dhan.regular_session(cached)     # older caches hold raw candles

    if offline():
        if cached is None or cached.empty:
            return pd.DataFrame(columns=dhan.OHLCV_COLUMNS)
        mask = (cached.index.date >= start) & (cached.index.date <= end)
        return cached[mask]

    parts = [] if cached is None else [cached]
    fetched = False

    for lo, hi in _missing_ranges(cached, start, end):
        if lo > hi:
            continue
        chunk = fetcher(lo, hi)
        if not chunk.empty:
            parts.append(chunk)
            fetched = True

    if not parts:
        return pd.DataFrame(columns=dhan.OHLCV_COLUMNS)

    frame = pd.concat(parts) if len(parts) > 1 else parts[0]
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()

    if fetched:
        _write_cache(path, frame)

    mask = (frame.index.date >= start) & (frame.index.date <= end)
    return frame[mask]


def get_candles(symbol, start, end, interval=dhan.INTERVAL_5M, refresh=False):
    """Intraday candles for [start, end]. Only missing date ranges hit the API."""
    return _get(symbol, start, end, f"{interval}m",
                lambda lo, hi: dhan.get_intraday(symbol, interval=interval,
                                                 from_date=lo, to_date=hi),
                refresh)


def get_daily_candles(symbol, start, end, refresh=False):
    """Daily candles for [start, end]. Only missing date ranges hit the API."""
    return _get(symbol, start, end, "1d",
                lambda lo, hi: dhan.get_daily(symbol, from_date=lo, to_date=hi),
                refresh)


def cache_size():
    """(file count, megabytes) currently held in the cache."""
    if not CACHE_DIR.exists():
        return 0, 0.0

    files = list(CACHE_DIR.glob("*.csv.gz"))
    megabytes = sum(f.stat().st_size for f in files) / (1024 * 1024)

    return len(files), megabytes
