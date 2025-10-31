# main.py — Opening Range Breakout Strategy (GitHub Actions Optimized)
# Fetches 9:15–9:30 OHLC once per day, then checks breakout once per GitHub Action run.

print("🧠 Running latest version of main.py...")

import pandas as pd
import datetime
import pytz
import yfinance as yf
from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all
from signal_generator import make_signal
from notifier import load_config, format_and_send

IST = pytz.timezone("Asia/Kolkata")


def get_latest_5min_close(symbol):
    """Fetch the latest 5-minute candle close price."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data.empty:
            return None
        data.index = data.index.tz_convert(IST)
        latest = data.iloc[-1]
        return float(latest["Close"])
    except Exception as e:
        print(f"⚠️ Error fetching close for {symbol}: {e}")
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

    today = datetime.datetime.now(IST).date()
    need_refresh = True

    # ---- Load or refresh OHLC data ----
    try:
        opening_df = pd.read_csv("opening_15min_ohlc.csv")
        if "date" in opening_df.columns:
            file_date = pd.to_datetime(opening_df["date"].iloc[0]).date()
            if file_date == today:
                need_refresh = False
                print("✅ Loaded today's saved Opening Range (9:15–9:30) data.")
            else:
                print(f"🔄 Old OHLC found (from {file_date}). Refreshing...")
        else:
            print("⚠️ No date column found in saved file, refreshing...")
    except FileNotFoundError:
        print("📈 No previous file found. Fetching new OHLC data...")

    if need_refresh:
        print("📊 Fetching fresh Opening Range (9:15–9:30) OHLC data...")
        symbols = get_symbols()
        rows = fetch_all(symbols)
        if not rows:
            print("⚠️ No OHLC data found.")
            return
        opening_df = pd.DataFrame(rows)
        opening_df["date"] = today
        opening_df.to_csv("opening_15min_ohlc.csv", index=False)
        print(f"✅ Saved new OHLC file for {today} ({len(opening_df)} rows).")

    # ---- Check breakouts only during market hours ----
    now = datetime.datetime.now(datetime.timezone.utc).astimezone(IST).time()
    print(f"🕒 Current IST time: {now.strftime('%H:%M:%S')}")

    if datetime.time(9, 30) <= now <= datetime.time(15, 30):
        print(f"\n🔍 Checking breakouts at {datetime.datetime.now(IST).strftime('%H:%M:%S')} IST...")
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
        print(f"⏸️ Market closed ({now.strftime('%H:%M:%S')} IST). Exiting...")


if __name__ == "__main__":
    main()