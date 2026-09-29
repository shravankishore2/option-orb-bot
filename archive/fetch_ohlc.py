# fetch_ohlc.py — Fetch the 9:20–9:35 Opening Range (OHLC) for all Nifty 200 stocks
# Includes the previous session's levels for the pivot / % move filters.
#
# Data source: DhanHQ (real-time). Previously yfinance, which is ~15 min delayed.

import time
from datetime import time as dtime

import pandas as pd

import dhan_client as dhan
from fetch_symbols import get_symbols

# Opening range window. Dhan stamps each 1-min candle with its START time, so
# 09:20 covers 09:20-09:21 — same convention yfinance used.
OR_START = dtime(9, 20)
OR_END = dtime(9, 35)


def get_prev_day_levels(symbol):
    """Previous completed session's (open, high, low, close)."""
    try:
        return dhan.get_prev_session(symbol)
    except dhan.DhanError as e:
        print(f"⚠️ {symbol}: prev-day fetch failed — {e}")
        return None, None, None, None


def get_opening_range(symbol):
    """Fetch the 9:20–9:35 opening range for a given stock."""
    try:
        data = dhan.get_intraday(symbol, interval=dhan.INTERVAL_1M)

        if data.empty:
            print(f"⚠️ {symbol}: No intraday data.")
            return None

        times = data.index.time
        window = data[(times >= OR_START) & (times < OR_END)]

        if window.empty:
            print(f"⚠️ {symbol}: No 9:20–9:35 data found.")
            return None

        # True range: based on all 1-minute highs/lows in the window
        o = float(window.iloc[0]["Open"])
        h = float(window["High"].max())
        l = float(window["Low"].min())
        c = float(window.iloc[-1]["Close"])

        prev_open, prev_high, prev_low, prev_close = get_prev_day_levels(symbol)

        if prev_close is None:
            print(f"⚠️ {symbol}: No previous session data — skipping.")
            return None

        # Fibonacci pivot points from the previous session's high, low, close.
        Pivot = (prev_high + prev_low + prev_close) / 3.0
        Range = prev_high - prev_low

        r1 = Pivot + 0.382 * Range
        s1 = Pivot - 0.382 * Range

        print(f"✅ {symbol}: O={o:.2f} H={h:.2f} L={l:.2f} C={c:.2f} PrevClose={prev_close}")

        return {
            "symbol": symbol,
            "open": o,
            "ORH": h,
            "ORL": l,
            "close": c,
            "prev_close": prev_close,
            "Pivot": Pivot,
            "Range": Range,
            "S1": s1,
            "R1": r1,
        }

    except dhan.DhanError as e:
        print(f"⚠️ {symbol}: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        return None


def fetch_all(symbols):
    """Fetch the 9:20–9:35 opening range for all given symbols."""
    results = []
    total = len(symbols)
    started = time.time()

    for i, sym in enumerate(symbols, 1):
        row = get_opening_range(sym)

        if row:
            results.append(row)

        if i % 25 == 0 or i == total:
            print(f"   … {i}/{total} scanned, {len(results)} with data "
                  f"({time.time() - started:.0f}s)")

    return results


if __name__ == "__main__":
    print("📊 Fetching 9:20–9:35 IST Opening Range for all Nifty 200 stocks (Dhan)...")

    symbols = get_symbols()
    print(f"✅ Loaded {len(symbols)} symbols from CSV.")

    rows = fetch_all(symbols)

    if not rows:
        print("⚠️ No valid data fetched — market may be closed or the Dhan token expired.")
    else:
        df = pd.DataFrame(rows)
        df.to_csv("opening_15min_ohlc.csv", index=False)
        print(f"💾 Saved -> opening_15min_ohlc.csv ({len(df)} rows)")
