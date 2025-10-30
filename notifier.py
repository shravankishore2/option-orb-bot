# notifier.py — Send trading signals via Telegram (fixed, robust)
import requests
from datetime import datetime
import os


def format_message(signals):
    """Formats the trading message (Markdown)."""
    # Defensive: support empty or None
    if not signals:
        return "📊 *Opening Range Strategy (9:15–9:35)*\n\nNo trading signals generated for today."

    # Normalize signal key names (some places used "direction" earlier)
    normalized = []
    for s in signals:
        sig = dict(s)  # copy
        if "direction" in sig and "signal" not in sig:
            sig["signal"] = sig["direction"]
        normalized.append(sig)

    buy_signals = [s for s in normalized if s.get("signal") == "BUY"]
    sell_signals = [s for s in normalized if s.get("signal") == "SELL"]

    # Header
    msg_lines = []
    msg_lines.append("📊 *Opening Range Strategy (9:15–9:35)*")
    msg_lines.append("")
    msg_lines.append(f"📅 Date: {datetime.now().strftime('%d-%b-%Y')}")
    msg_lines.append(f"🕒 Time: {datetime.now().strftime('%H:%M')}")
    msg_lines.append("")

    # BUY CALLS block
    if buy_signals:
        msg_lines.append("🟢 *BUY CALLS*")
        for s in buy_signals:
            symbol = s.get("symbol", "UNKNOWN")
            suggested = s.get("suggested_action")
            if not suggested:
                # default suggestion: nearest 50 strike call
                try:
                    underlying = float(s.get("underlying_price", s.get("close", 0)))
                    strike = int(round(underlying / 50) * 50)
                    suggested = f"BUY {symbol} CALL near {strike}"
                except Exception:
                    suggested = f"BUY {symbol} CALL"
            msg_lines.append(f"• *{symbol}* — {suggested}")
        msg_lines.append("")

    # BUY PUTS block
    if sell_signals:
        msg_lines.append("🔴 *BUY PUTS*")
        for s in sell_signals:
            symbol = s.get("symbol", "UNKNOWN")
            suggested = s.get("suggested_action")
            if not suggested:
                try:
                    underlying = float(s.get("underlying_price", s.get("close", 0)))
                    strike = int(round(underlying / 50) * 50)
                    suggested = f"BUY {symbol} PUT near {strike}"
                except Exception:
                    suggested = f"BUY {symbol} PUT"
            msg_lines.append(f"• *{symbol}* — {suggested}")
        msg_lines.append("")

    if not buy_signals and not sell_signals:
        msg_lines.append("⚪ No actionable trades today.")
        msg_lines.append("")

    msg_lines.append("— Automated by Python 📈")

    # Join with two spaces to keep Markdown readable
    return "\n".join(msg_lines)


def send_telegram_message(token, chat_id, text):
    """Send a Telegram message. Returns True on success."""
    if not token or not chat_id:
        print("⚠️ Telegram token or chat_id missing.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code == 200:
            return True
        else:
            print(f"⚠️ Telegram API returned {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")
        return False


def format_and_send(chat_id, signals, token=None):
    """
    Format the signals and send via Telegram.
    - chat_id: numeric chat id or group id (string or int)
    - signals: list of dicts
    - token: bot token
    Returns True if message sent, False otherwise (and saves a local backup).
    """
    message = format_message(signals)
    print("📨 Sending Telegram message...")
    success = send_telegram_message(token, chat_id, message)

    if success:
        print("✅ Telegram message sent successfully.")
        return True

    # fallback: save message locally for debugging / manual sending
    try:
        backup_path = os.path.join(os.getcwd(), "last_telegram_message.txt")
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(message)
        print(f"Message saved to {backup_path}")
    except Exception as e:
        print(f"⚠️ Failed to save backup message: {e}")

    return False


def load_config(path="config.ini"):
    """Load Telegram token and chat ID from config.ini (DEFAULT section)."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg["DEFAULT"] if "DEFAULT" in cfg else {}