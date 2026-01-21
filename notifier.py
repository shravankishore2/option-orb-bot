# notifier.py — Send Telegram alerts for ORB signals

import requests
from datetime import datetime
import os
import configparser
import pandas as pd

lots=pd.read_csv('Lot_size.csv')
def get_lot_size(symbol):
    row = lots.loc[lots["symbol"] == symbol, "lot_size"]
    return int(row.iloc[0]) if not row.empty else None


def format_message(signals):
    """Formats Telegram message from signals list."""
    if not signals:
        return "📊 *Opening Range Strategy (9:15–9:30)*\n\nNo trading signals today."

    buy_signals = [s for s in signals if s.get("signal") == "BUY"]
    sell_signals = [s for s in signals if s.get("signal") == "SELL"]

    msg = [
        "📊 *Opening Range Strategy (9:15–9:30)*",
        f"📅 {datetime.now().strftime('%d-%b-%Y')} | 🕒 {datetime.now().strftime('%H:%M')}\n",
    ]
    if buy_signals:
        msg.append("🟢 *BUY CALLS*")
        msg += [(f"• {s.get('signal')}"
                 f" CMP:{s.get('close')},PDC:{s.get('prev_close')}"
                 f"ORH:{s.get('ORH')},ORL:{s.get('ORL')}"
                 f"Lot Size={get_lot_size(s.get('symbol'))}") for s in buy_signals]
        msg.append("")

    if sell_signals:
        msg.append("🔴 *BUY PUTS*")
        msg += [(f"• {s.get('signal')}"
                 f"CMP:{s.get('close')},PDC:{s.get('prev_close')}"
                 f"ORH:{s.get('ORH')},ORL:{s.get('ORL')}") for s in sell_signals]
        msg.append("")

    msg.append("— Automated by Shravan 📈")
    return "\n".join(msg)


def send_telegram_message(token, chat_id, text):
    """Send Telegram message."""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if r.status_code == 200:
            print("✅ Telegram message sent.")
            return True
        print(f"⚠️ Telegram API error: {r.text}")
        return False
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")
        return False


def format_and_send(chat_id, signals, token=None):
    """Format signals + send via Telegram."""
    msg = format_message(signals)
    success = send_telegram_message(token, chat_id, msg)
    if not success:
        backup = "last_telegram_message.txt"
        with open(backup, "w", encoding="utf-8") as f:
            f.write(msg)
        print(f"💾 Saved message locally -> {backup}")
    return success


def load_config(path="config.ini"):
    """Load Telegram token + chat ID from config.ini."""
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg["DEFAULT"]