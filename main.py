# main.py — Opening Range Breakout Strategy (with dedupe + history logging)
# Fetches 9:15–9:30 OHLC once per day (saved), then on each run checks latest 5-min
# candles and sends only NEW signals (per-symbol per-day) to Telegram.
print("🧠 Running latest version of main.py...")

import os
import csv
import pandas as pd
import datetime
import pytz
import yfinance as yf

from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all  # should return rows with symbol, open, high, low, close, and prev_close (see note)
from signal_generator import generate_option_signals  # expects rows with prev_close present
from notifier import load_config, format_and_send

IST = pytz.timezone("Asia/Kolkata")

# Files
OPENING_OHLC_FILE = "opening_15min_ohlc.csv"      # daily saved ORB OHLC (with prev_close)
SENT_LOG_FILE = "sent_notifications.csv"         # dedupe log for sent signals (date,symbol,direction,time)
HISTORY_FILE = "backtest_opening_range.csv"      # full historical appended file for backtest/audit


def ensure_prev_close_in_rows(rows):
    """
    If fetch_all does not include prev_close, try to add prev_close by fetching 1d history.
    Each row becomes: symbol, open, high, low, close, prev_close
    """
    enhanced = []
    for r in rows:
        if "prev_close" in r and r["prev_close"] is not None:
            enhanced.append(r)
            continue

        sym = r["symbol"]
        try:
            ticker = f"{sym}.NS"
            df = yf.download(ticker, period="2d", interval="1d", progress=False)
            prev_close = None
            if len(df) >= 2:
                prev_close = float(df["Close"].iloc[-2])
            elif len(df) == 1:
                prev_close = float(df["Close"].iloc[-1])
            r["prev_close"] = prev_close
        except Exception:
            r["prev_close"] = None
        enhanced.append(r)
    return enhanced


def load_opening_df():
    """Load today's opening dataframe if present and fresh. Returns df and bool refreshed"""
    today = datetime.datetime.now(IST).date()
    if os.path.exists(OPENING_OHLC_FILE):
        try:
            df = pd.read_csv(OPENING_OHLC_FILE)
            # if file has 'date' column and matches today, return it
            if "date" in df.columns:
                file_date = pd.to_datetime(df["date"].iloc[0]).date()
                if file_date == today:
                    print("✅ Loaded today's saved Opening Range (9:15–9:30) data.")
                    return df, False  # not refreshed
                else:
                    print(f"🔄 Old OHLC found (from {file_date}). Will refresh for {today}.")
            else:
                print("⚠️ Saved OHLC missing date column — refreshing.")
        except Exception as e:
            print(f"⚠️ Failed reading {OPENING_OHLC_FILE}: {e}")

    # need to fetch new
    print("📈 Fetching fresh Opening Range (9:15–9:30) OHLC data...")
    symbols = get_symbols()
    if not symbols:
        print("⚠️ No symbols found (fetch_symbols).")
        return None, False

    rows = fetch_all(symbols)
    if not rows:
        print("⚠️ fetch_all returned no rows.")
        return None, False

    # ensure prev_close present for each row
    rows = ensure_prev_close_in_rows(rows)
    df = pd.DataFrame(rows)
    df["date"] = today
    df.to_csv(OPENING_OHLC_FILE, index=False)
    print(f"✅ Saved new OHLC file for {today} ({len(df)} rows).")
    return df, True


def load_sent_log():
    """
    Load sent_notifications.csv into a set for quick membership checks.
    Format columns: date,symbol,direction,time
    """
    today = datetime.datetime.now(IST).date().isoformat()
    seen = set()
    # If file missing -> nothing seen
    if not os.path.exists(SENT_LOG_FILE):
        return seen

    try:
        df = pd.read_csv(SENT_LOG_FILE)
        if df.empty:
            return seen
        # If file contains no rows for today -> clear it (start fresh)
        if "date" in df.columns:
            if not any(pd.to_datetime(df["date"]).dt.date == datetime.datetime.now(IST).date()):
                # clear file
                print("🧹 Sent log contains no entries for today — clearing sent log.")
                open(SENT_LOG_FILE, "w").close()
                return set()
        # Build set of (date,symbol,direction)
        for _, row in df.iterrows():
            d = str(row.get("date", "")).strip()
            sym = str(row.get("symbol", "")).strip().upper()
            dirn = str(row.get("direction", "")).strip().upper()
            seen.add((d, sym, dirn))
    except Exception as e:
        print(f"⚠️ Failed to load sent log: {e}")
    return seen


def append_sent_log(entries):
    """
    Append list of dicts to sent_notifications.csv
    each entry: {'date':..., 'symbol':..., 'direction':..., 'time':...}
    """
    header_needed = not os.path.exists(SENT_LOG_FILE) or os.path.getsize(SENT_LOG_FILE) == 0
    with open(SENT_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "symbol", "direction", "time"])
        if header_needed:
            writer.writeheader()
        for e in entries:
            writer.writerow(e)


def append_history(entries):
    """
    Append sent signals to backtest_opening_range.csv (history).
    Keep columns: date,time,symbol,direction,open,high,low,entry_close,prev_close,suggested_action
    """
    header_needed = not os.path.exists(HISTORY_FILE) or os.path.getsize(HISTORY_FILE) == 0
    with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "date", "time", "symbol", "direction",
            "open", "high", "low", "entry_close", "prev_close", "suggested_action"
        ])
        if header_needed:
            writer.writeheader()
        for e in entries:
            writer.writerow(e)


def filter_already_sent(active_signals, seen_set):
    """
    active_signals: list of dicts (symbol,direction,open,high,low,close,prev_close,suggested_action)
    seen_set: set of tuples (date_str, SYMBOL, DIRECTION)
    Returns: new_signals (not yet sent today)
    """
    today_str = datetime.datetime.now(IST).date().isoformat()
    new = []
    for s in active_signals:
        key = (today_str, s["symbol"].strip().upper(), s["signal"].strip().upper() if "signal" in s else s.get("direction","").strip().upper())
        if key in seen_set:
            continue
        new.append(s)
    return new


def build_telegram_payload(signals):
    """
    Build a single message text for signal list using notifier.format_message
    We pass signals as is (each should contain suggested_action).
    """
    return signals


def now_time_str():
    return datetime.datetime.now(IST).strftime("%H:%M:%S")


def run_cycle():
    # Load or refresh opening OHLC (once per day)
    opening_df, refreshed = load_opening_df()
    if opening_df is None or opening_df.empty:
        print("⚠️ No opening OHLC available — exiting this run.")
        return

    # Prepare list of rows (dicts) for signal generator
    rows = opening_df.to_dict(orient="records")

    # generate signals (uses your signal_generator.generate_option_signals which enforces 2% filter)
    signals = generate_option_signals(rows)
    if not signals:
        print("ℹ️ No signals generated by signal_generator this run.")
        return

    # dedupe
    seen = load_sent_log()
    new_signals = filter_already_sent(signals, seen)
    if not new_signals:
        print("ℹ️ All signals already sent today — nothing new to send.")
        return

    # Prepare message and send via notifier (format_and_send expects chat_id and signals)
    cfg = load_config()
    if not cfg:
        print("⚠️ config.ini missing or invalid.")
        return
    telegram_token = cfg.get("telegram_token")
    telegram_chat_id = cfg.get("telegram_chat_id")

    # Optionally transform signals to notifier-friendly structure; here we pass as-is
    payload_signals = build_telegram_payload(new_signals)

    sent_ok = format_and_send(telegram_chat_id, payload_signals, token=telegram_token)
    now_date = datetime.datetime.now(IST).date().isoformat()
    now_t = now_time_str()

    if sent_ok:
        print(f"📨 Sent {len(new_signals)} new signals at {now_t} IST.")
        # record in sent log & history
        sent_rows = []
        hist_rows = []
        for s in new_signals:
            direction = s.get("signal") or s.get("direction") or ""
            sent_rows.append({"date": now_date, "symbol": s["symbol"].strip().upper(), "direction": direction, "time": now_t})
            hist_rows.append({
                "date": now_date,
                "time": now_t,
                "symbol": s["symbol"].strip().upper(),
                "direction": direction,
                "open": s.get("open"),
                "high": s.get("high"),
                "low": s.get("low"),
                "entry_close": s.get("close"),
                "prev_close": s.get("prev_close"),
                "suggested_action": s.get("suggested_action", "")
            })
        append_sent_log(sent_rows)
        append_history(hist_rows)
    else:
        print("⚠️ Telegram send failed; will not mark as sent.")


def main():
    # Run one check cycle (intended to be called by scheduler / GitHub Action every 5 min)
    now_ist = datetime.datetime.now(datetime.timezone.utc).astimezone(IST).time()
    print(f"🕒 Current IST time: {now_ist.strftime('%H:%M:%S')}")
    # Only run checks during market hours (9:30 - 15:30 IST). You can change bounds if you want.
    if datetime.time(9, 30) <= now_ist <= datetime.time(15, 30):
        try:
            run_cycle()
        except Exception as e:
            print(f"⚠️ Error during run_cycle: {e}")
    else:
        print("⏸️ Outside market hours — skipping this run.")


if __name__ == "__main__":
    main()