# signal_generator.py — Generate ORB signals with prev-close filter

def make_signal(ohlc_row):
    """
    Generates BUY/SELL signals based on:
      - Breakout above ORH or below ORL
      - Must move ±2% from previous day's close
    """
    symbol = ohlc_row["symbol"]
    o, h, l, c = ohlc_row["open"], ohlc_row["high"], ohlc_row["low"], ohlc_row["close"]
    prev_close = ohlc_row.get("prev_close")

    if not prev_close:
        return None

    if c > h and c >= 1.02 * prev_close:
        direction = "BUY"
    elif c < l and c <= 0.98 * prev_close:
        direction = "SELL"
    else:
        return None

    strike = int(round(c / 50) * 50)
    action = f"BUY {symbol} {'CALL' if direction == 'BUY' else 'PUT'} near {strike} strike"

    return {
        "symbol": symbol,
        "signal": direction,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "prev_close": prev_close,
        "suggested_action": action,
    }


def generate_option_signals(ohlc_rows):
    """Generates valid option trades from a list of OHLC rows."""
    return [sig for row in ohlc_rows if (sig := make_signal(row))]


if __name__ == "__main__":
    sample = [
        {"symbol": "RELIANCE", "open": 2500, "high": 2520, "low": 2490, "close": 2550, "prev_close": 2470},
        {"symbol": "TCS", "open": 3600, "high": 3620, "low": 3590, "close": 3615, "prev_close": 3595},
    ]
    print(generate_option_signals(sample))