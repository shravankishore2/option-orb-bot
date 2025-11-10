# main.py — Opening Range Breakout Strategy (with smart OHLC refresh + dedupe + history)
# Fetches 9:15–9:30 OHLC once per day (saved), then checks every 5-min interval for breakouts.
print("🧠 Running latest version of main.py...")

import os
import csv
import pandas as pd
import datetime
import pytz
import yfinance as yf
from typing import List, Dict

from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all  # should return rows with symbol, open, high, low, close
from signal_generator import generate_option_signals  # applies 2% rule etc.
from notifier import load_config, format_and_send

# Timezone
IST = pytz.timezone("Asia/Kolkata")

# File paths
OPENING_OHLC_FILE = "opening_15min_ohlc.csv"
SENT_LOG_FILE = "sent_notifications.csv"
HISTORY_FILE = "backtest_opening_range.csv"


def now_date_ist():
    """Current IST date"""
    return datetime.datetime.now(datetime.timezone.utc).astimezone(IST).date()


def now_time_ist():
    """Current IST time"""
    return datetime.datetime.now(datetime.timezone.utc).astimezone(IST).time()


def now_time_str():
    return datetime.datetime.now(datetime.timezone.utc).astimezone(IST).strftime("%H:%M:%S")


# -------------------------------------------------------------------------
# ✅ OHLC Fetch Helpers
# -------------------------------------------------------------------------

def ensure_prev_close_in_rows(rows: List[Dict]) -> List[Dict]:
    """Ensure previous day’s close is added for each symbol."""
    enhanced = []
    for r in rows:
        if r.get("prev_close") is not None:
            enhanced.append(r)
            continue

        sym = r.get("symbol")
        prev_close = None
        if sym:
            try:
                ticker = f"{sym}.NS"
                df = yf.download(ticker, period="2d", interval="1d", progress=False)
                if len(df) >= 2:
                    prev_close = float(df["Close"].iloc[-2])
                elif len(df) == 1:
                    prev_close = float(df["Close"].iloc[-1])
            except Exception as e:
                print(f"⚠️ prev_close fetch failed for {sym}: {e}")
        r["prev_close"] = prev_close
        enhanced.append(r)
    return enhanced


def load_opening_df():
    """Load today’s saved OHLC, or fetch it if missing or outdated."""
    today = now_date_ist()

    # ✅ Step 1: Try loading today's file
    if os.path.exists(OPENING_OHLC_FILE):
        try:
            df = pd.read_csv(OPENING_OHLC_FILE)
            if "date" in df.columns:
                file_date = pd.to_datetime(df["date"].iloc[0]).date()
                if file_date == today:
                    print("✅ Loaded today's saved Opening Range (9:15–9:30) data.")
                    return df, False
                else:
                    print(f"🔄 Old OHLC found (from {file_date}). Refreshing for {today}...")
            else:
                print("⚠️ Missing date column in saved file. Refetching...")
        except Exception as e:
            print(f"⚠️ Failed reading {OPENING_OHLC_FILE}: {e}")

    # ✅ Step 2: Only fetch new OHLC if market hours (9:30–15:30)
    now_t = now_time_ist()
    if not (datetime.time(9, 30) <= now_t <= datetime.time(15, 30)):
        print(f"⏸️ Market closed ({now_t}). Skipping OHLC fetch.")
        return None, False

    print(f"📈 Fetching fresh Opening Range (9:15–9:30) OHLC for {today}...")
    symbols = get_symbols()
    if not symbols:
        print("⚠️ No symbols found.")
        return None, False

    rows = fetch_all(symbols)
    if not rows:
        print("⚠️ fetch_all returned no data.")
        return None, False

    # Add prev_close values
    rows = ensure_prev_close_in_rows(rows)
    df = pd.DataFrame(rows)
    df["date"] = today
    df.to_csv(OPENING_OHLC_FILE, index=False)
    print(f"✅ Saved OHLC for {today} ({len(df)} symbols).")

    return df, True


# -------------------------------------------------------------------------
# ✅ Dedupe System
# -------------------------------------------------------------------------

def load_sent_log() -> set:
    """Load or clear sent_notifications.csv for today."""
    seen = set()
    today = now_date_ist().isoformat()

    if not os.path.exists(SENT_LOG_FILE):
        return seen

    try:
        df = pd.read_csv(SENT_LOG_FILE)
        if df.empty:
            return seen

        # If no rows for today, clear the file
        if "date" in df.columns:
            file_dates = pd.to_datetime(df["date"], errors="coerce").dt.date
            if not any(file_dates == now_date_ist()):
                print("🧹 Clearing old sent log (no entries for today).")
                open(SENT_LOG_FILE, "w").close()
                return set()

        for _, row in df.iterrows():
            key = (
                str(row.get("date", "")).strip(),
                str(row.get("symbol", "")).strip().upper(),
                str(row.get("direction", "")).strip().upper(),
            )
            seen.add(key)
    except Exception as e:
        print(f"⚠️ Failed to load sent log: {e}")
    return seen


def append_sent_log(entries):
    """Append new sent signals to sent_notifications.csv."""
    header_needed = not os.path.exists(SENT_LOG_FILE) or os.path.getsize(SENT_LOG_FILE) == 0
    with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "symbol", "direction", "time"])
        if header_needed:
            writer.writeheader()
        writer.writerows(entries)


def append_history(entries):
    """Append signals to backtest_opening_range.csv for historical record."""
    header_needed = not os.path.exists(HISTORY_FILE) or os.path.getsize(HISTORY_FILE) == 0
    with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date", "time", "symbol", "direction",
                "open", "high", "low", "entry_close", "prev_close", "suggested_action"
            ],
        )
        if header_needed:
            writer.writeheader()
        writer.writerows(entries)


def filter_already_sent(signals, seen):
    """Return only signals not sent today."""
    today = now_date_ist().isoformat()
    new = []
    for s in signals:
        symbol = s.get("symbol", "").strip().upper()
        direction = (s.get("signal") or s.get("direction") or "").strip().upper()
        if (today, symbol, direction) not in seen:
            new.append(s)
    return new


# -------------------------------------------------------------------------
# ✅ Main Signal Cycle
# -------------------------------------------------------------------------

def run_cycle():
    opening_df, refreshed = load_opening_df()
    if opening_df is None or opening_df.empty:
        print("⚠️ No opening OHLC available — exiting this run.")
        return

    rows = opening_df.to_dict(orient="records")
    signals = generate_option_signals(rows)

    if not signals:
        print("ℹ️ No signals generated this run.")
        return

    seen = load_sent_log()
    new_signals = filter_already_sent(signals, seen)
    if not new_signals:
        print("ℹ️ All signals already sent today.")
        return

    cfg = load_config()
    if not cfg:
        print("⚠️ config.ini missing.")
        return

    chat_id = cfg.get("telegram_chat_id")
    token = cfg.get("telegram_token")

    sent_ok = format_and_send(chat_id, new_signals, token=token)
    now_date = now_date_ist().isoformat()
    now_t = now_time_str()

    if sent_ok:
        print(f"✅ Sent {len(new_signals)} new signals at {now_t}.")
        sent_rows, hist_rows = [], []
        for s in new_signals:
            direction = (s.get("signal") or s.get("direction") or "").strip().upper()
            sent_rows.append({
                "date": now_date,
                "symbol": s.get("symbol", "").upper(),
                "direction": direction,
                "time": now_t,
            })
            hist_rows.append({
                "date": now_date,
                "time": now_t,
                "symbol": s.get("symbol", "").upper(),
                "direction": direction,
                "open": s.get("open"),
                "high": s.get("high"),
                "low": s.get("low"),
                "entry_close": s.get("close"),
                "prev_close": s.get("prev_close"),
                "suggested_action": s.get("suggested_action", ""),
            })
        append_sent_log(sent_rows)
        append_history(hist_rows)
    else:
        print("⚠️ Telegram send failed — not marking as sent.")


def main():
    now_t = now_time_ist()
    print(f"🕒 Current IST time: {now_t.strftime('%H:%M:%S')}")
    if datetime.time(9, 30) <= now_t <= datetime.time(15, 30):
        run_cycle()
    else:
        print(f"⏸️ Outside market hours ({now_t}). Skipping run.")


if __name__ == "__main__":
    main()