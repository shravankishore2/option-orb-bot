# signal_generator.py — generates signals based on 9:15–9:35 candle

def make_signal(ohlc_row):
    """
    get ORB and if candle above orb buy elif candle below orb sell else hold
    """
    o = ohlc_row["open"]
    h = ohlc_row["high"]
    l = ohlc_row["low"]
    c = ohlc_row["close"]

    if c > h:
        direction = "BUY"
    elif c < l:
        direction = "SELL"
    else:
        direction = "HOLD"

    return {
        "symbol": ohlc_row["symbol"],
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "signal": direction,
    }


def get_option_trade(symbol, direction, close_price):
    """
    if BUY signal- Call otherwise if sell- put and if hold nothing
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
    Given a list of OHLC dicts, return trade signals for each stock.
    """
    trades = []
    for row in ohlc_rows:
        sig = make_signal(row)
        if sig["signal"] in ["BUY", "SELL"]:
            trade = get_option_trade(sig["symbol"], sig["signal"], sig["close"])
            trades.append(trade)
    return trades