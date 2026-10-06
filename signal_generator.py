# signal_generator.py — the ORB entry rules (shared by the live bot and the replay)
#
# BUY  when close breaks above the opening-range high, has moved at least
#      +1.8% from the previous close, and clears the Fibonacci R1 pivot.
# SELL is the mirror image (below ORL, -1.8%, below S1).
# Every threshold lives in strategy_config.py.

import strategy_config as C


def evaluate(close, orh, orl, prev_close, r1, s1, prev_move=C.PREV_CLOSE_MOVE):
    """Return 'BUY', 'SELL' or None for one observed price.

    prev_move: the minimum move from the previous close (the live rule's 1.8%).
    None drops that condition (research variant `orbital_nomove`, v3 candidate);
    the default is the pre-registered rule and is what the live bot uses."""
    if not prev_close or prev_close <= 0 or r1 is None or s1 is None:
        return None
    up = prev_move is None or close >= prev_close * (1 + prev_move)
    down = prev_move is None or close <= prev_close * (1 - prev_move)

    if close >= orh * (1 + C.BREAKOUT_BUFFER) and up and close >= r1:
        return "BUY"

    if close <= orl * (1 - C.BREAKOUT_BUFFER) and down and close <= s1:
        return "SELL"

    return None


def generate_option_signals(rows, verbose=True):
    """
    Apply the rules to many rows (dicts with symbol, open, ORH, ORL, close,
    prev_close, R1, S1). verbose=False silences the near-miss debug output —
    used by the historical replay, which evaluates this millions of times.
    """
    signals = []

    for r in rows:
        symbol = r["symbol"]
        orh, orl, close = r["ORH"], r["ORL"], r["close"]
        prev = r.get("prev_close")
        pivot = r.get("Pivot", r.get("pivot"))
        r1, s1 = r.get("R1"), r.get("S1")

        direction = evaluate(close, orh, orl, prev, r1, s1)

        if direction:
            signals.append({
                "symbol": symbol, "open": r.get("open"), "ORH": orh, "ORL": orl,
                "close": close, "prev_close": prev, "signal": direction,
                "pivot": pivot, "R1": r1, "S1": s1,
            })
        elif verbose and prev:
            diff_from_orh = round((close - orh) / orh * 100, 2)
            diff_from_prev = round((close - prev) / prev * 100, 2)
            if abs(diff_from_orh) < 1.5 or abs(diff_from_prev) < 2.5:
                print(f"ℹ️ {symbol}: Close={close:.2f}, ORH={orh:.2f}, ORL={orl:.2f}, Prev={prev:.2f} "
                      f"→ ΔORH={diff_from_orh}%, ΔPrev={diff_from_prev}%")

    return signals
