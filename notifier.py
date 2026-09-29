import os
import configparser
import requests
import pandas as pd
from datetime import datetime as _datetime, timedelta, timezone

# Always Indian time: the bot may run on a machine (or CI runner) set to UTC,
# and "today" must roll over at IST midnight, not the server's.
IST = timezone(timedelta(hours=5, minutes=30))


class datetime:
    @staticmethod
    def now():
        return _datetime.now(IST)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")
SENT_NOTIFICATIONS_FILE = os.path.join(BASE_DIR, "sent_notifications.csv")

SENT_COLUMNS = [
    "date",
    "time",
    "symbol",
    "direction",
    "entry_price",
    "current_price",
    "close",
    "prev_close",
    "orh",
    "orl",
    "lot_size",
    "status",
]


def load_telegram_config():
    # Environment first (GitHub Actions secrets), then config.ini (local).
    env_token = os.getenv("TELEGRAM_BOT_TOKEN")
    env_chat = os.getenv("TELEGRAM_CHAT_ID")
    if env_token and env_chat:
        return env_token.strip(), env_chat.strip()

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if "telegram" in config:
        section = config["telegram"]
    else:
        section = config["DEFAULT"]

    bot_token = (
        section.get("telegram_bot_token")
        or section.get("telegram_token")
        or section.get("bot_token")
    )

    chat_id = (
        section.get("telegram_chat_id")
        or section.get("chat_id")
    )

    if not bot_token:
        raise ValueError(
            "Telegram token missing in config.ini. Expected telegram_token or telegram_bot_token"
        )

    if not chat_id:
        raise ValueError(
            "Telegram chat ID missing in config.ini. Expected telegram_chat_id or chat_id"
        )

    return bot_token.strip(), chat_id.strip()


def ensure_sent_notifications_schema():
    if os.path.exists(SENT_NOTIFICATIONS_FILE):
        try:
            df = pd.read_csv(SENT_NOTIFICATIONS_FILE)
        except Exception:
            df = pd.DataFrame(columns=SENT_COLUMNS)
    else:
        df = pd.DataFrame(columns=SENT_COLUMNS)

    for col in SENT_COLUMNS:
        if col not in df.columns:
            if col == "current_price" and "close" in df.columns:
                df[col] = df["close"]
            else:
                df[col] = ""

    df = df[SENT_COLUMNS]
    df.to_csv(SENT_NOTIFICATIONS_FILE, index=False)


def reset_sent_notifications_if_new_day():
    ensure_sent_notifications_schema()

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        df = pd.read_csv(SENT_NOTIFICATIONS_FILE)

        if df.empty or "date" not in df.columns:
            pd.DataFrame(columns=SENT_COLUMNS).to_csv(SENT_NOTIFICATIONS_FILE, index=False)
            print("✅ Reset empty/invalid sent_notifications.csv")
            return

        dates = df["date"].astype(str).str[:10].dropna().unique()

        if today not in dates:
            pd.DataFrame(columns=SENT_COLUMNS).to_csv(SENT_NOTIFICATIONS_FILE, index=False)
            print("✅ New day detected. Reset sent_notifications.csv")
        else:
            print("✅ sent_notifications.csv already belongs to today")

    except Exception as e:
        print(f"⚠️ Could not validate sent_notifications.csv. Resetting. Error: {e}")
        pd.DataFrame(columns=SENT_COLUMNS).to_csv(SENT_NOTIFICATIONS_FILE, index=False)


def already_sent_today(symbol, direction):
    today = datetime.now().strftime("%Y-%m-%d")

    ensure_sent_notifications_schema()

    try:
        df = pd.read_csv(SENT_NOTIFICATIONS_FILE)

        if df.empty:
            return False

        if not {"date", "symbol", "direction", "status"}.issubset(df.columns):
            return False

        df["date"] = df["date"].astype(str).str[:10]
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df["direction"] = df["direction"].astype(str).str.upper().str.strip()
        df["status"] = df["status"].astype(str).str.upper().str.strip()

        match = df[
            (df["date"] == today)
            & (df["symbol"] == str(symbol).upper().strip())
            & (df["direction"] == str(direction).upper().strip())
            & (df["status"] == "SENT")
        ]

        return not match.empty

    except Exception as e:
        print(f"⚠️ Error checking sent notification log: {e}")
        return False


def log_sent_notification(
    symbol,
    direction,
    entry_price="",
    close="",
    prev_close="",
    orh="",
    orl="",
    lot_size="",
    status="SENT",
):
    ensure_sent_notifications_schema()

    row = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "current_price": close,
        "close": close,
        "prev_close": prev_close,
        "orh": orh,
        "orl": orl,
        "lot_size": lot_size,
        "status": status,
    }

    df = pd.read_csv(SENT_NOTIFICATIONS_FILE)

    for col in SENT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[SENT_COLUMNS]

    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(SENT_NOTIFICATIONS_FILE, index=False)


def clean_num(x):
    try:
        if pd.isna(x):
            return ""
        val = float(x)
        if val.is_integer():
            return str(int(val))
        return f"{val:.2f}"
    except Exception:
        return str(x)


def build_message(
    symbol,
    direction,
    entry_price="",
    close="",
    prev_close="",
    orh="",
    orl="",
    lot_size="",
    extras=None,
):
    direction = str(direction).upper().strip()

    emoji = "🟢" if direction == "BUY" else "🔴"

    message = f"""
{emoji} ORB SIGNAL

Symbol: {symbol}
Direction: {direction}

Entry: ₹{clean_num(entry_price)}
Current: ₹{clean_num(close)}
Prev Close: ₹{clean_num(prev_close)}

ORH: ₹{clean_num(orh)}
ORL: ₹{clean_num(orl)}
Lot Size: {lot_size}

Time: {datetime.now().strftime("%H:%M:%S")}
""".strip()

    if extras:
        message += "\n\n" + "\n".join(f"{k}: {v}" for k, v in extras.items())

    return message


def send_telegram_message(message):
    bot_token, chat_id = load_telegram_config()

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
    }

    try:
        response = requests.post(url, data=payload, timeout=15)
    except requests.exceptions.RequestException:
        response = requests.post(url, data=payload, timeout=15)

    if response.status_code != 200:
        print("❌ Telegram API error:")
        print(response.text)
        return False

    return True


def send_signal_notification(
    symbol,
    direction,
    entry_price="",
    close="",
    prev_close="",
    orh="",
    orl="",
    lot_size="",
    extras=None,
):
    reset_sent_notifications_if_new_day()

    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()

    if already_sent_today(symbol, direction):
        print(f"⏭️ Already sent today: {symbol} {direction}")
        return False

    message = build_message(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        close=close,
        prev_close=prev_close,
        orh=orh,
        orl=orl,
        lot_size=lot_size,
        extras=extras,
    )

    sent = send_telegram_message(message)

    if sent:
        log_sent_notification(
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            close=close,
            prev_close=prev_close,
            orh=orh,
            orl=orl,
            lot_size=lot_size,
            status="SENT",
        )

        print(f"✅ Telegram sent and logged: {symbol} {direction}")
        return True

    log_sent_notification(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        close=close,
        prev_close=prev_close,
        orh=orh,
        orl=orl,
        lot_size=lot_size,
        status="FAILED",
    )

    print(f"❌ Telegram failed and logged: {symbol} {direction}")
    return False