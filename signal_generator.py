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
        orh = r["ORH"]
        orl= r["ORL"]
        close = r["close"]
        prev = r.get("prev_close")
        pivot=r.get("pivot")
        r1=r.get("R1")
        s1=r.get("S1")

        if not prev or prev == 0:
            continue  # skip if prev close missing

        direction = None

        # 🟢 BUY condition: breakout above ORH AND +2% from prev close
        if (close >= orh * 1.001) and (close >= prev * 1.018) and (close>=r1):  # small tolerance for rounding
            direction = "BUY"

        # 🔴 SELL condition: breakdown below ORL AND -2% from prev close
        elif (close <= orl * 0.999) and (close <= prev * 0.982) and (close<=s1):
            direction = "SELL"

        if direction:
            signals.append({
                "symbol": symbol,
                "open": o,
                "ORH": orh,
                "ORL": orl,
                "close": close,
                "prev_close": prev,
                "signal": direction,
                "pivot":pivot,
                "R1": r1,
                "S1": s1,
            })
        else:
            # 🔍 Debug: helps identify near-miss conditions
            diff_from_orh = round((close - orh) / orh * 100, 2)
            diff_from_prev = round((close - prev) / prev * 100, 2)
            if abs(diff_from_orh) < 1.5 or abs(diff_from_prev) < 2.5:
                print(f"ℹ️ {symbol}: Close={close:.2f}, ORH={orh:.2f}, ORL={orl:.2f}, Prev={prev:.2f} "
                      f"→ ΔORH={diff_from_orh}%, ΔPrev={diff_from_prev}%")

    return signals