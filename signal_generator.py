# signal_generator.py — generates option signals based on ORB breakout + 2% move

def generate_option_signals(rows):
    """
    Generate BUY/SELL signals based on:
    - Opening Range Breakout (close > ORH or close < ORL)
    - AND at least 2% move from the previous day's close.
    """
    signals = []

    for r in rows:
        symbol = r["symbol"]
        o = r["open"]
        h = r["high"]
        l = r["low"]
        c = r["close"]
        prev = r.get("prev_close")

        if not prev or prev == 0:
            continue  # skip if prev close missing

        direction = None
        suggested_action = None

        # 🟢 BUY condition: breakout above ORH AND +2% from prev close
        if (c >= h * 1.001) and (c >= prev * 1.018):  # small tolerance for rounding
            direction = "BUY"
            suggested_action = f"BUY {symbol}"

        # 🔴 SELL condition: breakdown below ORL AND -2% from prev close
        elif (c <= l * 0.999) and (c <= prev * 0.982):
            direction = "SELL"
            suggested_action = f"SELL {symbol} "

        if direction:
            signals.append({
                "symbol": symbol,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "prev_close": prev,
                "signal": direction,
                "suggested_action": suggested_action
            })
        else:
            # 🔍 Debug: helps identify near-miss conditions
            diff_from_orh = round((c - h) / h * 100, 2)
            diff_from_prev = round((c - prev) / prev * 100, 2)
            if abs(diff_from_orh) < 1.5 or abs(diff_from_prev) < 2.5:
                print(f"ℹ️ {symbol}: Close={c:.2f}, ORH={h:.2f}, ORL={l:.2f}, Prev={prev:.2f} "
                      f"→ ΔORH={diff_from_orh}%, ΔPrev={diff_from_prev}%")

    return signals