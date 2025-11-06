# fetch_ohlc.py — Fetch today's Opening Range (9:15–9:30 IST) + Previous Close
import warnings
warnings.simplefilter("ignore", FutureWarning)

import pandas as pd
import yfinance as yf
from datetime import time
from fetch_symbols import get_symbols
import pytz
from configparser import ConfigParser
from notifier import format_and_send

IST = pytz.timezone("Asia/Kolkata")


def normalize_index_to_ist(data):
    """Ensure timestamps are in IST timezone."""
    if data.empty:
        return data
    if data.index.tz is None:
        data.index = data.index.tz_localize("UTC")
    data.index = data.index.tz_convert(IST)
    return data


def get_previous_close(symbol):
    """Fetch previous day's close."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="3d", interval="1d", progress=False)
        if len(data) >= 2:
            return float(data["Close"].iloc[-2])
        elif len(data) == 1:
            return float(data["Close"].iloc[-1])
        return None
    except Exception as e:
        print(f"⚠️ {symbol}: Failed to fetch previous close: {e}")
        return None


def get_opening_range(symbol):
    """Fetch 9:15–9:30 OHLC and previous close for a symbol."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if data.empty:
            return None

        data = normalize_index_to_ist(data).reset_index()
        data["Time"] = data["Datetime"].dt.time

        mask = (data["Time"] >= time(9, 15)) & (data["Time"] < time(9, 30))
        window = data.loc[mask]
        if window.empty:
            return None

        o = float(window.iloc[0]["Open"])
        h = float(window["High"].max())
        l = float(window["Low"].min())
        c = float(window.iloc[-1]["Close"])
        prev_close = get_previous_close(symbol)

        return {
            "symbol": symbol,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "prev_close": prev_close,
        }

    except Exception as e:
        print(f"⚠️ {symbol}: {e}")
        return None


def fetch_all(symbols):
    """Fetch opening range and previous close for all."""
    results = []
    for sym in symbols:
        row = get_opening_range(sym)
        if row:
            results.append(row)
    return results


if __name__ == "__main__":
    cfg = ConfigParser()
    cfg.read("config.ini")
    telegram_chat_id = cfg["DEFAULT"].get("telegram_chat_id")
    telegram_token = cfg["DEFAULT"].get("telegram_token")

    symbols = get_symbols()
    print(f"✅ Loaded {len(symbols)} symbols (Nifty 200).")
    print("📊 Fetching Opening Range (9:15–9:30) + Previous Day Close...")

    data = fetch_all(symbols)
    df = pd.DataFrame(data)
    df.to_csv("opening_15min_ohlc.csv", index=False)
    print(f"✅ Saved -> opening_15min_ohlc.csv ({len(df)} rows)")