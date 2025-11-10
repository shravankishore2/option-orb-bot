# main.py — Opening Range Breakout Strategy (real-time signal version)
# Works anytime between 9:15–15:30, fetches latest close, compares vs ORH/ORL + 2% move.

print("🧠 Running latest version of main.py...")

import os
import csv
import pandas as pd
import datetime
import pytz
import yfinance as yf

from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all
from signal_generator import generate_option_signals
from notifier import load_config, format_and_send

IST = pytz.timezone("Asia/Kolkata")
OPENING_OHLC_FILE = "opening_15min_ohlc.csv"


def get_latest_5min_close(symbol):
    """Fetch the latest 5-minute close for the symbol."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="1d", interval="5m", progress=False)
        if data.empty:
            return None
        data.index = data.index.tz_convert(IST)
        return float(data["Close"].iloc[-1])
    except Exception:
        return None


def update_latest_closes(opening_df):
    """Attach latest close prices to opening dataframe."""
    updated_rows = []
    for _, row in opening_df.iterrows():
        symbol = row["symbol"]
        latest_close = get_latest_5min_close(symbol)
        if latest_close is None:
            continue
        row["close"] = latest_close
        updated_rows.append(row)
    return pd.DataFrame(updated_rows)


def main():
    print("📊 Starting Opening Range Breakout Scanner...")

    now_ist = datetime.datetime.now(datetime.timezone.utc).astimezone(IST)
    current_time = now_ist.time()

    if not (datetime.time(9, 15) <= current_time <= datetime.time(15, 30)):
        print(f"⏸️ Market closed ({current_time.strftime('%H:%M:%S')} IST). Exiting.")
        return

    today = now_ist.date()

    # Load or refresh OHLC data
    need_refresh = True
    if os.path.exists(OPENING_OHLC_FILE):
        df = pd.read_csv(OPENING_OHLC_FILE)
        if "date" in df.columns:
            file_date = pd.to_datetime(df["date"].iloc[0]).date()
            if file_date == today:
                print("✅ Loaded today's saved Opening Range (9:15–9:30) data.")
                need_refresh = False
            else:
                print(f"🔄 Old OHLC found (from {file_date}). Refreshing...")
        else:
            print("⚠️ No date column in saved file — refreshing data.")

    if need_refresh:
        print("📈 Fetching fresh 9:15–9:30 OHLC data...")
        symbols = get_symbols()
        rows = fetch_all(symbols)
        if not rows:
            print("⚠️ No OHLC data fetched.")
            return
        df = pd.DataFrame(rows)
        df["date"] = today
        df.to_csv(OPENING_OHLC_FILE, index=False)
        print(f"✅ Saved new file ({len(df)} rows).")

    # Update with latest closes
    print("🔁 Fetching latest 5-minute closes for all symbols...")
    updated_df = update_latest_closes(df)
    if updated_df.empty:
        print("⚠️ No updated close data available.")
        return

    # Generate signals (ORB + 2%)
    signals = generate_option_signals(updated_df.to_dict(orient="records"))
    if not signals:
        print("ℹ️ No signals generated this run.")
        return

    # Send via Telegram
    cfg = load_config()
    if not cfg:
        print("⚠️ Config file missing or invalid.")
        return

    telegram_token = cfg.get("telegram_token")
    telegram_chat_id = cfg.get("telegram_chat_id")
    format_and_send(telegram_chat_id, signals, token=telegram_token)

    print(f"✅ Sent {len(signals)} signals successfully at {now_ist.strftime('%H:%M:%S')} IST.")


if __name__ == "__main__":
    main()