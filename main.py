    # main.py — Opening Range Breakout Strategy with Live 5-Min Update + Backtest Logging

import os, csv, datetime, pytz, yfinance as yf, pandas as pd, time
from fetch_symbols import get_symbols
from fetch_ohlc import fetch_all     # returns opening 9:15–9:30 data
from signal_generator import generate_option_signals
from notifier import load_config, format_and_send

print("🧠 Running latest version of main.py...")

IST = pytz.timezone("Asia/Kolkata")
OPENING_FILE = "opening_15min_ohlc.csv"
SENT_FILE = "sent_notifications.csv"
BACKTEST_FILE = "backtest_opening_range.csv"


# ---------- helpers ----------
def now_time_str():
    return datetime.datetime.now(IST).strftime("%H:%M:%S")

def today_date():
    return datetime.datetime.now(IST).date()


# ---------- load morning ORB ----------
def load_opening_df():
    """Load today's ORB or fetch new if missing."""
    today = today_date()
    if os.path.exists(OPENING_FILE):
        try:
            df = pd.read_csv(OPENING_FILE)
            if "date" in df.columns and pd.to_datetime(df["date"].iloc[0]).date() == today:
                print("✅ Loaded today's Opening Range (9:15–9:30).")
                return df
        except Exception:
            pass
    print("📈 Fetching fresh 9:15–9:30 ORB data...")
    symbols = get_symbols()
    rows = fetch_all(symbols)
    df = pd.DataFrame(rows)
    df["date"] = today
    df.to_csv(OPENING_FILE, index=False)
    return df


# ---------- get latest 5-min close ----------
def get_latest_close(symbol):
    try:
        data = yf.download(f"{symbol}.NS", period="1d", interval="5m", progress=False)
        if len(data) == 0:
            return None
        return float(data["Close"].iloc[-1])
    except Exception:
        return None


# ---------- sent log ----------
def load_sent():
    if not os.path.exists(SENT_FILE):
        return set()
    df = pd.read_csv(SENT_FILE)
    today = today_date().isoformat()
    seen = set()
    for _, r in df.iterrows():
        seen.add((r["date"], r["symbol"], r["direction"]))
    return seen

def append_sent(entries):
    header = not os.path.exists(SENT_FILE) or os.path.getsize(SENT_FILE) == 0
    with open(SENT_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "symbol", "direction", "time"])
        if header:
            w.writeheader()
        for e in entries:
            w.writerow(e)


# ---------- backtest log ----------
def append_backtest(entries):
    header = not os.path.exists(BACKTEST_FILE) or os.path.getsize(BACKTEST_FILE) == 0
    with open(BACKTEST_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "date", "time", "symbol", "direction",
            "entry_price", "ORH", "ORL", "prev_close"
        ])
        if header:
            w.writeheader()
        for e in entries:
            w.writerow(e)


# ---------- run + send ----------
def run_and_send(signals):
    if not signals:
        print("ℹ️ No signals to send.")
        return
    cfg = load_config()
    if not cfg:
        print("⚠️ Config invalid.")
        return
    token, chat = cfg["telegram_token"], cfg["telegram_chat_id"]
    ok = format_and_send(chat, signals, token=token)
    nowd, nowt = today_date().isoformat(), now_time_str()
    logs = []
    for s in signals:
        logs.append({
            "date": nowd,
            "time": nowt,
            "symbol": s["symbol"],
            "direction": s["signal"],
            "entry_price": s["close"],
            "ORH": s["high"],
            "ORL": s["low"],
            "prev_close": s["prev_close"]
        })
    append_backtest(logs)
    if ok:
        print(f"✅ Sent {len(signals)} signals & logged to backtest_opening_range.csv.")
        append_sent([{"date": nowd, "symbol": s["symbol"], "direction": s["signal"], "time": nowt} for s in signals])
    else:
        print("⚠️ Telegram failed, still logged to backtest.")


# ---------- main cycle ----------
def run_cycle():
    print("📊 Starting ORB Live Scan...")
    df = load_opening_df()
    rows = df.to_dict("records")

    # 🔹 get latest 5m close for each symbol
    print("🔁 Fetching latest 5-minute closes...")
    for r in rows:
        latest = get_latest_close(r["symbol"])
        if latest:
            r["close"] = latest

    signals = generate_option_signals(rows)
    if not signals:
        print("ℹ️ No signals generated this run.")
        return

    seen = load_sent()
    today_s = today_date().isoformat()
    new = [s for s in signals if (today_s, s["symbol"], s["signal"]) not in seen]
    if not new:
        print("ℹ️ All signals already sent today.")
        return

    run_and_send(new)


def main():
    print("🚀 Starting continuous ORB bot (5-min intervals)...\n")
    try:
        while True:
            now_ist = datetime.datetime.now(datetime.timezone.utc).astimezone(IST).time()
            print(f"🕒 Current IST time: {now_ist.strftime('%H:%M:%S')}")
            # Run only during market hours
            if datetime.time(9, 30) <= now_ist <= datetime.time(15, 30):
                run_cycle()
            else:
                print("⏸️ Market closed — waiting for next session.")
            print("💤 Sleeping 5 minutes before next check...\n")
            time.sleep(300)  # 5 minutes
    except KeyboardInterrupt:
        print("\n🛑 Stopped manually.")


if __name__ == "__main__":
    main()