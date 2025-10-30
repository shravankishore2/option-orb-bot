# notifier.py — Send trading signals via Telegram

import requests
from datetime import datetime
import os
import configparser


def format_message(signals):
    """Formats the trading message clearly and safely."""
    if not signals:
        return "📊 *Opening Range Strategy (9:15–9:35)*\n\nNo trading signals generated for today."

    buy_signals = [s for s in signals if s.get("signal") == "BUY"]
    sell_signals = [s for s in signals if s.get("signal") == "SELL"]

    msg_lines = [
        "📊 *Opening Range Strategy (9:15–9:35)*",
        "",
        f"📅 Date: {datetime.now().strftime('%d-%b-%Y')}",
        f"🕒 Time: {datetime.now().strftime('%H:%M')}",
        ""
    ]

    # BUY CALLS section
    if buy_signals:
        msg_lines.append("🟢 *BUY CALLS*")
        for s in buy_signals:
            symbol = s.get("symbol", "")
            suggested = s.get("suggested_action", f"BUY {symbol} CALL")
            msg_lines.append(f"• {suggested}")
        msg_lines.append("")

    # BUY PUTS section
    if sell_signals:
        msg_lines.append("🔴 *BUY PUTS*")
        for s in sell_signals:
            symbol = s.get("symbol", "")
            suggested = s.get("suggested_action", f"BUY {symbol} PUT")
            msg_lines.append(f"• {suggested}")
        msg_lines.append("")

    if not buy_signals and not sell_signals:
        msg_lines.append("⚪ No actionable trades today.")
        msg_lines.append("")

    msg_lines.append("— Automated by Python 📈")
    return "\n".join(msg_lines)


def send_telegram_message(token, chat_id, text):
    """Send a Telegram message to a chat/group."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            return True
        else:
            print(f"⚠️ Telegram API error: {response.text}")
            return False
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
        print(f"💾 Message saved to {backup_path}")

    return success


def load_config(path="config.ini"):
    """Load Telegram token and chat ID from config.ini."""
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if "DEFAULT" not in cfg:
        raise ValueError("❌ config.ini missing [DEFAULT] section.")
    return cfg["DEFAULT"]


if __name__ == "__main__":
    # Simple local test
    test_signals = [
        {"symbol": "RELIANCE", "signal": "BUY", "suggested_action": "BUY RELIANCE 2500 CALL"},
        {"symbol": "TCS", "signal": "SELL", "suggested_action": "BUY TCS 3400 PUT"},
    ]
    print(format_message(test_signals))