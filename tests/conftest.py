"""Shared helpers: build synthetic 5-minute sessions with known shapes."""

import datetime as dt
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def make_day(day, closes, start=dt.time(9, 15), volume=1000.0, spread=0.2):
    """One session of 5-min candles. closes[i] is the close of candle i.

    Open = previous close, High/Low = max/min(open, close) +/- spread.
    """
    stamps = pd.date_range(dt.datetime.combine(day, start), periods=len(closes),
                           freq="5min", tz=IST)
    closes = np.asarray(closes, float)
    opens = np.concatenate([[closes[0]], closes[:-1]])
    frame = pd.DataFrame({
        "Open": opens,
        "High": np.maximum(opens, closes) + spread,
        "Low": np.minimum(opens, closes) - spread,
        "Close": closes,
        "Volume": np.full(len(closes), volume),
    }, index=pd.DatetimeIndex(stamps, name="Datetime"))
    return frame


def make_daily(days, close=100.0, high=101.0, low=99.0):
    idx = pd.DatetimeIndex([pd.Timestamp(d, tz=IST) for d in days], name="Datetime")
    n = len(days)
    return pd.DataFrame({"Open": [close] * n, "High": [high] * n, "Low": [low] * n,
                         "Close": [close] * n, "Volume": [1e6] * n}, index=idx)


@pytest.fixture
def day():
    return dt.date(2026, 9, 1)


@pytest.fixture
def breakout_day(day):
    """Flat 100 through the opening range, then a steady climb to ~104.

    Opening range (09:20-09:30 candles): ~99.8-100.2. Prev close 100 means the
    BUY needs close >= 101.8 (and >= R1, which is ~100.4 for H/L 101/99).
    """
    closes = [100.0] * 5 + [100.0 + 0.25 * i for i in range(1, 70)]
    return make_day(day, closes)


def data_dir_has_cache():
    return os.path.exists(os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                       "data", "history", "RELIANCE_5m.csv.gz"))
