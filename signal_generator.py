# signal_generator.py — generates signals based on 9:15–9:30 ORB + prev-day filter

import yfinance as yf


def prev_day_close(symbol):
    """Fetch previous day's close price for a given NSE stock."""
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, period="2d", interval="1d", progress=False)
        if len(data) >= 2:
            return float(data["Close"].iloc[-2])
        elif len(data) == 1:
            return float(data["Close"].iloc[-1])
        else:
            return None
    except Exception as e:
        print(f"⚠️ Error fetching prev close for {symbol}: {e}")
        return None


def make_signal(ohlc_row):
    """
    Generate signal:
    BUY → if close > ORH AND today's close > +2% from prev day close
    SELL → if close < ORL AND today's close < -2% from prev day close
    Otherwise HOLD.
    """
    symbol = ohlc_row["symbol"]
    o = ohlc_row["open"]
    h = ohlc_row["high"]
    l = ohlc_row["low"]
    c = ohlc_row["close"]

    prev_close = prev_day_close(symbol)
    if not prev_close:
        return None

    pct_change = ((c - prev_close) / prev_close) * 100

    if c > h and pct_change >= 2:
        direction = "BUY"
    elif c < l and pct_change <= -2:
        direction = "SELL"
    else:
        direction = "HOLD"

    return {
        "symbol": symbol,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "prev_close": prev_close,
        "pct_change": round(pct_change, 2),
        "signal": direction
    }


def get_option_trade(symbol, direction, close_price):
    """
    Return suggested option trade based on breakout signal.
    """
    if direction == "BUY":
        action = f"BUY {symbol} CALL near {int(round(close_price / 50) * 50)} strike"
    elif direction == "SELL":
        action = f"BUY {symbol} PUT near {int(round(close_price / 50) * 50)} strike"
    else:
        action = "HOLD"

    return {
        "symbol": symbol,
        "direction": direction,
        "underlying_price": close_price,
        "suggested_action": action,
    }


def generate_option_signals(ohlc_rows):
    """
    Given list of OHLC dicts, return trade signals with 2% filter applied.
    """
    trades = []
    for row in ohlc_rows:
        sig = make_signal(row)
        if sig and sig["signal"] in ["BUY", "SELL"]:
            trade = get_option_trade(sig["symbol"], sig["signal"], sig["close"])
            trade.update({
                "pct_change": sig["pct_change"],
                "prev_close": sig["prev_close"]
            })
            trades.append(trade)
    return trades