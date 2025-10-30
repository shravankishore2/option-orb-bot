# notifier.py — Send trading signals via Telegram

import requests
from datetime import datetime
import os

def format_message(signals):
    """Formats the trading message."""
    if not signals:
        return "📊 *Opening Range Strategy (9:15–9:35)*\n\nNo trading signals generated for today."

    buy_signals = [s for s in signals if s.get("signal") == "BUY"]
    sell_signals = [s for s in signals if s.get("signal") == "SELL"]

    msg = "📊 *Opening Range Strategy (9:15–9:35)*\n\n"
    msg += f"📅 Date: {datetime.now().strftime('%d-%b-%Y')}\n"
    msg += f"🕒 Time: {datetime.now().strftime('%H:%M')}\n\n"

    if buy_signals:
        msg += "🟢 *BUY CALLS*\n"
        for s in buy_signals:
            msg += f"• {s['symbol']} — {s.get('suggested_action', f'BUY {s['symbol']} CALL')}\n"
        msg += "\n"

    if sell_signals:
        msg += "🔴 *BUY PUTS*\n"
        for s in sell_signals:
            msg += f"• {s['symbol']} — {s.get('suggested_action', f'BUY {s['symbol']} PUT')}\n"
        msg += "\n"

    if not buy_signals and not sell_signals:
        msg += "⚪ No actionable trades today.\n"

    msg += "\n— Automated by Python 📈"
    return msg


def send_telegram_message(token, chat_id, text):
    """Send a Telegram message."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")
        return False


def format_and_send(chat_id, signals, token=None):
    """Format message and send via Telegram."""
    message = format_message(signals)
    print("📨 Sending Telegram message...")
    success = send_telegram_message(token, chat_id, message)

    if success:
        print("✅ Telegram message sent successfully.")
    else:
        print("⚠️ Telegram message failed. Saving locally...")
        backup_path = os.path.join(os.getcwd(), "last_telegram_message.txt")
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(message)
        print(f"Message saved to {backup_path}")

    return success


def load_config(path="config.ini"):
    """Load Telegram token and chat ID."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg["DEFAULT"]