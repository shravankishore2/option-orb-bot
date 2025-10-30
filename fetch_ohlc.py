# fetch_ohlc.py — Opening Range Breakout Tracker + Telegram Alerts
import warnings
warnings.simplefilter("ignore", FutureWarning)

import pandas as pd
import yfinance as yf
from datetime import time
from fetch_symbols import get_symbols
from notifier import format_and_send, load_config

IST = "Asia/Kolkata"

def normalize_index_to_ist(data):
    """Ensure timestamps are in IST timezone."""
    if data.empty:
        return data
    if data.index.tz is None:
        data.index = data.index.tz_localize("UTC")
    data.index = data.index.tz_convert(IST)
    return data


def get_opening_range(symbol):
    """Get OHLC between 9:15–9:30 IST for a given symbol."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if data.empty:
            return None

        data = normalize_index_to_ist(data).reset_index()
        data["Time"] = data["Datetime"].dt.time

        # Opening range = 9:15–9:30
        mask = (data["Time"] >= time(9, 15)) & (data["Time"] < time(9, 30))
        window = data.loc[mask]
        if window.empty:
            return None

        o = float(window.iloc[0]["Open"])
        h = float(window["High"].max())
        l = float(window["Low"].min())
        c = float(window.iloc[-1]["Close"])

        return {
            "symbol": symbol,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "ORH": h,
            "ORL": l
        }
    except Exception as e:
        print(f"⚠️ {symbol}: {e}")
        return None


def check_breakout(symbol, ORH, ORL):
    """Check if the latest 5-min candle closed outside the opening range."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data.empty:
            return None

        data = normalize_index_to_ist(data)
        last_close = float(data["Close"].iloc[-1])
        last_time = data.index[-1].strftime("%H:%M")

        if last_close > ORH:
            signal = "BREAKOUT ↑ (close above ORH)"
        elif last_close < ORL:
            signal = "BREAKDOWN ↓ (close below ORL)"
        else:
            signal = "Inside Range"

        return {
            "symbol": symbol,
            "last_close": last_close,
            "signal": signal,
            "time": last_time
        }
    except Exception as e:
        print(f"⚠️ Breakout check failed for {symbol}: {e}")
        return None


def fetch_all(symbols):
    """Fetch opening range and check breakout on latest 5-min candle."""
    results = []
    for sym in symbols:
        row = get_opening_range(sym)
        if not row:
            continue

        brk = check_breakout(sym, row["ORH"], row["ORL"])
        if brk:
            row.update(brk)
            results.append(row)
    return results


if __name__ == "__main__":
    # Load config (Telegram credentials)
    cfg = load_config()
    if not cfg:
        print("⚠️ Config file not found or invalid.")
        exit()

    telegram_token = cfg.get("telegram_token")
    telegram_chat_id = cfg.get("telegram_chat_id")

    symbols = get_symbols()
    print(f"✅ Loaded {len(symbols)} symbols (Nifty 200).")
    print("📊 Fetching Opening Range (9:15–9:30) and checking for 5-min breakouts...")

    data = fetch_all(symbols)
    df = pd.DataFrame(data)
    df.to_csv("opening_range_breakouts.csv", index=False)
    print(f"✅ Saved -> opening_range_breakouts.csv ({len(df)} rows)")

    # Filter actionable signals only
    actionable = df[df["signal"].str.contains("BREAK", na=False)]
    if actionable.empty:
        msg = "📊 *Opening Range Breakout Update*\n\nNo breakouts detected in the last 5-min candle."
    else:
        msg = "📊 *Opening Range Breakout Signals*\n\n"
        for _, row in actionable.iterrows():
            msg += f"🔹 *{row['symbol']}* → {row['signal']} at ₹{row['last_close']} ({row['time']} IST)\n"

    # Send via Telegram
    sent = format_and_send(telegram_chat_id, [{"symbol": "TG", "signal": msg}], token=telegram_token)
    if sent:
        print("✅ Telegram alert sent successfully.")
    else:
        print("⚠️ Telegram message failed to send. Check bot token/chat ID.")