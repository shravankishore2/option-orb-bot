# notifier.py — Send Telegram alerts for ORB signals

import requests
from datetime import datetime
import configparser
import pandas as pd

# Load lot sizes once
lots = pd.read_csv("Lot_size.csv")


def get_lot_size(symbol):
    row = lots.loc[lots["Symbol"] == symbol, "lot_size"]
    return int(row.iloc[0]) if not row.empty else "N/A"


def format_message(signals):
    """Formats Telegram message from signals list."""

    if not signals:
        return "📊 *Opening Range Strategy (9:20–9:35)*\n\nNo trading signals today."

    buy_signals = [s for s in signals if s.get("signal") == "BUY"]
    sell_signals = [s for s in signals if s.get("signal") == "SELL"]

    msg = [
        "📊 *Opening Range Strategy (9:20–9:35)*",
        f"📅 {datetime.now().strftime('%d-%b-%Y')} | 🕒 {datetime.now().strftime('%H:%M')}\n",
    ]

    # BUY SECTION
    if buy_signals:
        msg.append("🟢 *2% Above PDC*")
        for s in buy_signals:
            msg.append(
                f"• {s.get('symbol')} | {s.get('signal')} "
                f"CMP:{s.get('close'):.2f}, "
                f"PDC:{s.get('prev_close'):.2f}, "
                f"ORH:{s.get('ORH'):.2f}, "
                f"ORL:{s.get('ORL'):.2f}, "
                f"Lot:{get_lot_size(s.get('symbol'))}"
            )
        msg.append("")

    # SELL SECTION
    if sell_signals:
        msg.append("🔴 *2% Below PDC*")
        for s in sell_signals:
            msg.append(
                f"• {s.get('symbol')} | {s.get('signal')} "
                f"CMP:{s.get('close'):.2f}, "
                f"PDC:{s.get('prev_close'):.2f}, "
                f"ORH:{s.get('ORH'):.2f}, "
                f"ORL:{s.get('ORL'):.2f}, "
                f"Lot:{get_lot_size(s.get('symbol'))}"
            )
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


def send_in_chunks(token, chat_id, text, chunk_size=3500):
    """Split long messages into safe chunks."""
    chunks = []

    while len(text) > chunk_size:
        split_idx = text.rfind("\n", 0, chunk_size)
        if split_idx == -1:
            split_idx = chunk_size
        chunks.append(text[:split_idx])
        text = text[split_idx:]

    chunks.append(text)

    success_all = True

    for chunk in chunks:
        success = send_telegram_message(token, chat_id, chunk)
        if not success:
            success_all = False

    return success_all


def format_and_send(chat_id, signals, token=None):
    """Format signals + send via Telegram safely."""
    msg = format_message(signals)

    success = send_in_chunks(token, chat_id, msg)

    if not success:
        backup = "last_telegram_message.txt"
        with open(backup, "w", encoding="utf-8") as f:
            f.write(msg)
        print(f"💾 Saved message locally -> {backup}")

    return success


def load_config(path="config.ini"):
    """Load Telegram token + chat ID."""
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg["DEFAULT"]