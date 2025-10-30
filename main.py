# main.py — Opening Range Breakout Strategy (optimized)
# Fetches 9:15–9:30 OHLC once, then checks breakout every 5 minutes till 15:30 IST

import pandas as pd
import time
import datetime
import pytz
from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all
from signal_generator import make_signal
from notifier import load_config, format_and_send
import yfinance as yf

IST = pytz.timezone("Asia/Kolkata")


def get_latest_5min_close(symbol):
    """Fetch the latest 5-min candle close price."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data.empty:
            return None
        data.index = data.index.tz_convert(IST)
        latest = data.iloc[-1]
        return float(latest["Close"])
    except Exception:
        return None


def check_breakouts(opening_df):
    """Check breakout above/below the opening range."""
    signals = []

    for _, row in opening_df.iterrows():
        symbol = row["symbol"]
        latest_close = get_latest_5min_close(symbol)
        if latest_close is None:
            continue

        if latest_close > row["high"]:
            direction = "BUY"
        elif latest_close < row["low"]:
            direction = "SELL"
        else:
            direction = "HOLD"

        signals.append({
            "symbol": symbol,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": latest_close,
            "signal": direction
        })

    return signals


def main():
    print("📊 Starting Opening Range Breakout Scanner...")

    cfg = load_config()
    if not cfg:
        print("⚠️ Config file not found or invalid.")
        return

    telegram_token = cfg.get("telegram_token")
    telegram_chat_id = cfg.get("telegram_chat_id")

    # Load or fetch the opening range
    try:
        opening_df = pd.read_csv("opening_15min_ohlc.csv")
        print("✅ Loaded saved Opening Range (9:15–9:30) data.")
    except FileNotFoundError:
        print("📈 Fetching Opening Range (9:15–9:30) OHLC data...")
        symbols = get_symbols()
        rows = fetch_all(symbols)
        if not rows:
            print("⚠️ No OHLC data found.")
            return
        opening_df = pd.DataFrame(rows)
        opening_df.to_csv("opening_15min_ohlc.csv", index=False)
        print(f"✅ Saved -> opening_15min_ohlc.csv ({len(opening_df)} rows)")

    # Repeatedly check for breakouts every 5 minutes
    while True:
        now = datetime.datetime.now(IST).time()

        if datetime.time(9, 35) <= now <= datetime.time(15, 30):
            print(f"\n🕒 Checking breakouts at {datetime.datetime.now(IST).strftime('%H:%M:%S')} IST...")
            try:
                signals = check_breakouts(opening_df)
                active_signals = [s for s in signals if s["signal"] in ("BUY", "SELL")]

                if active_signals:
                    print(f"✅ Found {len(active_signals)} breakout signals.")
                    pd.DataFrame(active_signals).to_csv("latest_signals.csv", index=False)
                    format_and_send(telegram_chat_id, active_signals, token=telegram_token)
                else:
                    print("ℹ️ No new breakouts this cycle.")
            except Exception as e:
                print(f"⚠️ Error checking breakouts: {e}")
        else:
            print(f"⏸️ Market closed ({now.strftime('%H:%M:%S')} IST). Waiting for next session...")

        time.sleep(300)  # wait 5 minutes


if __name__ == "__main__":
    main()