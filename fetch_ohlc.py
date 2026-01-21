# fetch_ohlc.py — Fetch true 9:15–9:30 Opening Range (OHLC) for all Nifty 200 F&O stocks
# Includes previous day's close for 2% up/down filtering.

import warnings
warnings.simplefilter("ignore", FutureWarning)

import pandas as pd
import yfinance as yf
from datetime import time
from fetch_symbols import get_symbols

IST = "Asia/Kolkata"


def normalize_index_to_ist(data):
    """Ensure timestamps are localized to IST timezone."""
    if data.empty:
        return data
    if data.index.tz is None:
        data.index = data.index.tz_localize("UTC")
    data.index = data.index.tz_convert(IST)
    return data


def get_prev_close(symbol):
    """Fetch previous day's close for the given symbol."""
    try:
        ticker = f"{symbol}.NS"
        df = yf.download(ticker, period="2d", interval="1d", progress=False)
        if len(df) >= 2:
            return float(df["Close"].iloc[-2])
        elif len(df) == 1:
            return float(df["Close"].iloc[-1])
        return None
    except Exception:
        return None


def get_opening_range(symbol):
    """Fetch the true 9:15–9:30 OHLC range for a given stock."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if data.empty:
            print(f"⚠️ {symbol}: No intraday data.")
            return None

        # Localize timestamps to IST
        data = normalize_index_to_ist(data).reset_index()
        data["Time"] = data["Datetime"].dt.time

        # Select only the first 15 minutes (9:15–9:30)
        mask = (data["Time"] >= time(9, 20)) & (data["Time"] < time(9, 35))
        window = data.loc[mask]
        if window.empty:
            print(f"⚠️ {symbol}: No 9:15–9:35 data found.")
            return None

        # ✅ True range: based on all 1-minute highs/lows
        o = float(window.iloc[0]["Open"])
        h = float(window["High"].max())  # true high
        l = float(window["Low"].min())   # true low
        c = float(window.iloc[-1]["Close"])

        prev_close = get_prev_close(symbol)

        print(f"✅ {symbol}: O={o:.2f} H={h:.2f} L={l:.2f} C={c:.2f} PrevClose={prev_close}")
        return {
            "symbol": symbol,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "prev_close": prev_close,
            "ORH": h,
            "ORL": l
        }

    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        return None


def fetch_all(symbols):
    """Fetch 9:15–9:35 OHLC for all given symbols."""
    results = []
    for sym in symbols:
        row = get_opening_range(sym)
        if row:
            results.append(row)
    return results


if __name__ == "__main__":
    print("📊 Fetching 9:15–9:30 IST Opening Range for all Nifty 200 F&O stocks...")

    symbols = get_symbols()
    print(f"✅ Loaded {len(symbols)} symbols from CSV.")
    rows = fetch_all(symbols)

    if not rows:
        print("⚠️ No valid data fetched — possibly market closed or Yahoo blocked requests.")
    else:
        df = pd.DataFrame(rows)
        df.to_csv("opening_15min_ohlc.csv", index=False)
        print(f"💾 Saved -> opening_15min_ohlc.csv ({len(df)} rows)")