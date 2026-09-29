import pandas as pd
import numpy as np
from datetime import time as dtime

import dhan_client as dhan

# =========================================================
# CONFIG
# =========================================================
CSV_FILE = "backtest_opening_range.csv"
# Dhan serves years of 5-min history (yfinance capped it at ~60 days), so this
# is now a strategy choice rather than a data limit — widen it freely.
LOOKBACK_DAYS = 60
FORCE_EXIT_TIME = dtime(15, 15)
MAX_ENTRY_TIME = dtime(11, 0)
BUY_ONLY = True
ENABLE_TIME_FILTER = True

# Partial profit taking (lock in gains early)
PARTIAL_RR = 0.5
PARTIAL_PCT = 0.5

# ORB-based exit params (from working script)
ORB_TARGET_FACTOR = 0.75
ORB_TRAIL_FACTOR = 0.50
ORB_MIN_PCT = 0.20
STOP_FACTOR = 0.6  # Tight stop = 0.6 * ORB (target RR = 1.25)


# =========================================================
# SINGLE-TRADE ENGINE
# =========================================================
def run_trade(symbol, trade_date, trade_time, direction, entry_price, orh, orl):
    date_str = pd.to_datetime(trade_date).strftime("%Y-%m-%d")

    # Dhan returns IST-indexed OHLCV directly, and serves years of intraday
    # history (yfinance capped 5m data at ~60 days).
    try:
        data = dhan.get_intraday(
            symbol,
            interval=dhan.INTERVAL_5M,
            from_date=date_str,
            to_date=date_str,
        )
    except dhan.DhanError:
        return None

    if data.empty:
        return None

    data = data[data.index.time >= trade_time].copy()
    if len(data) == 0:
        return None

    if direction == "BUY":
        return _walk_buy(data, entry_price, orh, orl)
    else:
        return _walk_sell(data, entry_price, orh, orl)


def _walk_buy(data, entry_price, orh, orl):
    orb_range = orh - orl
    if orb_range / entry_price * 100 < ORB_MIN_PCT:
        return None

    initial_stop = entry_price - orb_range * STOP_FACTOR
    risk = entry_price - initial_stop
    if risk <= 0:
        return None

    target = entry_price + orb_range * ORB_TARGET_FACTOR

    for current_time, candle in data.iterrows():
        high = float(candle["High"])
        low = float(candle["Low"])
        close = float(candle["Close"])

        # Hard stop at ORL
        if low <= initial_stop:
            exit_price = initial_stop
            exit_reason = "STOP"
            break

        # Target
        if high >= target:
            exit_price = target
            exit_reason = "TARGET"
            break

        # Time exit
        if current_time.time() > FORCE_EXIT_TIME:
            exit_price = close
            exit_reason = "TIME_EXIT"
            break
    else:
        last = data.iloc[-1]
        exit_price = float(last["Close"])
        exit_reason = "FINAL_CLOSE"

    pnl = exit_price - entry_price
    rr = pnl / risk if risk != 0 else 0
    mfe = (target - entry_price) / risk
    mae = (initial_stop - entry_price) / risk

    return {
        "entry": round(entry_price, 2),
        "exit": round(exit_price, 2),
        "risk": round(risk, 2),
        "pnl": round(pnl, 2),
        "rr": round(rr, 2),
        "exit_reason": exit_reason,
    }


def _walk_sell(data, entry_price, orh, orl):
    orb_range = orh - orl
    if orb_range / entry_price * 100 < ORB_MIN_PCT:
        return None

    initial_stop = entry_price + orb_range * STOP_FACTOR
    risk = initial_stop - entry_price
    if risk <= 0:
        return None

    target = entry_price - orb_range * ORB_TARGET_FACTOR

    for current_time, candle in data.iterrows():
        high = float(candle["High"])
        low = float(candle["Low"])
        close = float(candle["Close"])

        # Hard stop at ORL
        if high >= initial_stop:
            exit_price = initial_stop
            exit_reason = "STOP"
            break

        # Target
        if low <= target:
            exit_price = target
            exit_reason = "TARGET"
            break

        # Time exit
        if current_time.time() > FORCE_EXIT_TIME:
            exit_price = close
            exit_reason = "TIME_EXIT"
            break
    else:
        last = data.iloc[-1]
        exit_price = float(last["Close"])
        exit_reason = "FINAL_CLOSE"

    pnl = entry_price - exit_price
    rr = pnl / risk if risk != 0 else 0
    mfe = (entry_price - target) / risk
    mae = (entry_price - initial_stop) / risk

    return {
        "entry": round(entry_price, 2),
        "exit": round(exit_price, 2),
        "risk": round(risk, 2),
        "pnl": round(pnl, 2),
        "rr": round(rr, 2),
        "exit_reason": exit_reason,
    }


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    df = pd.read_csv(CSV_FILE)
    df.columns = [c.strip().lower() for c in df.columns]

    df["date"] = pd.to_datetime(df["date"])
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S").dt.time

    cutoff = pd.Timestamp.today() - pd.Timedelta(days=LOOKBACK_DAYS)
    df = df[df["date"] >= cutoff].copy()
    print(f"Loaded {len(df)} trades within last {LOOKBACK_DAYS} days")

    if BUY_ONLY:
        df = df[df["direction"].str.upper() == "BUY"]
        print(f"After BUY filter: {len(df)} trades")

    if ENABLE_TIME_FILTER:
        df = df[df["time"] < MAX_ENTRY_TIME]
        print(f"After time filter: {len(df)} trades")

    if len(df) == 0:
        print("No trades")
        exit()

    results = []
    total = len(df)

    for _, trade in df.iterrows():
        symbol = str(trade["symbol"]).upper().strip()
        direction = str(trade["direction"]).upper().strip()
        entry_price = float(trade["entry_price"])
        orh = float(trade["orh"])
        orl = float(trade["orl"])
        trade_date = trade["date"]
        trade_time = trade["time"]

        out = run_trade(symbol, trade_date, trade_time, direction, entry_price, orh, orl)

        if out is None:
            continue

        out["symbol"] = symbol
        out["date"] = trade_date
        out["direction"] = direction
        results.append(out)

        print(f"{len(results)}/{total} {symbol:<12} RR={out['rr']:.2f} PNL={out['pnl']:.2f} {out['exit_reason']}")

    if not results:
        print("No results")
        exit()

    rdf = pd.DataFrame(results)
    rdf.to_csv("backtest_results_temp.csv", index=False)

    win_rate = (rdf["pnl"] > 0).mean() * 100
    avg_rr = rdf["rr"].mean()
    median_rr = rdf["rr"].median()
    total_rr = rdf["rr"].sum()
    total_pnl = rdf["pnl"].sum()
    pos = rdf[rdf["rr"] > 0]["rr"].sum()
    neg = rdf[rdf["rr"] < 0]["rr"].sum()
    profit_factor = pos / abs(neg) if neg != 0 else float("inf")

    wins = rdf[rdf["rr"] > 0]
    losses = rdf[rdf["rr"] < 0]
    avg_win_rr = wins["rr"].mean() if len(wins) else 0
    avg_loss_rr = losses["rr"].mean() if len(losses) else 0
    best_rr = rdf["rr"].max()
    worst_rr = rdf["rr"].min()
    rr_std = rdf["rr"].std()
    expectancy = (win_rate / 100) * avg_win_rr + (1 - win_rate / 100) * avg_loss_rr if avg_loss_rr != 0 else 0

    signs = (rdf["pnl"] > 0).astype(int)
    consec = 0
    max_consec_win = 0
    max_consec_loss = 0
    for s in signs:
        if s == 1:
            consec = consec + 1 if consec >= 0 else 1
            max_consec_win = max(max_consec_win, consec)
        else:
            consec = consec - 1 if consec <= 0 else -1
            max_consec_loss = max(max_consec_loss, -consec)

    sharpe = (avg_rr / rr_std * (252 ** 0.5)) if rr_std != 0 else 0

    print(f"\n{'=' * 60}")
    print(f"  BACKTEST SUMMARY  |  {len(rdf)} trades")
    print(f"{'=' * 60}")
    print(f"  Win Rate        : {win_rate:>6.2f}%     ({len(wins)} W / {len(losses)} L)")
    print(f"  Total P&L       : {total_pnl:>+8.2f} pts")
    print(f"  Avg RR          : {avg_rr:>+8.2f}")
    print(f"  Median RR       : {median_rr:>+8.2f}")
    print(f"  Total RR        : {total_rr:>+8.2f}")
    print(f"  RR Std Dev      : {rr_std:>8.2f}")
    print(f"  Profit Factor   : {profit_factor:>8.2f}")
    print(f"  Expectancy      : {expectancy:>+8.2f}")
    print(f"{'─' * 60}")
    print(f"  Avg Win RR      : {avg_win_rr:>+8.2f}     Best  : {best_rr:>+8.2f}")
    print(f"  Avg Loss RR     : {avg_loss_rr:>+8.2f}     Worst : {worst_rr:>+8.2f}")
    print(f"  Max Consec W    : {max_consec_win:>8}     Consec L: {max_consec_loss:>8}")
    print(f"  Sharpe (RR)     : {sharpe:>8.2f}")
    print(f"{'─' * 60}")
    print(f"  Exit breakdown:")
    for reason, count in rdf["exit_reason"].value_counts().items():
        pct = count / len(rdf) * 100
        wr = wins[wins["exit_reason"] == reason]["rr"].count() / count * 100 if count else 0
        print(f"    {reason:<12} {count:>4} ({pct:>5.1f}%)  WR: {wr:>5.1f}%")
    print(f"{'=' * 60}")
    print(f"  Saved → backtest_results_temp.csv")
    print(f"{'=' * 60}")
