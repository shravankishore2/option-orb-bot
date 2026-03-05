import pandas as pd
import yfinance as yf
import pytz
import os
import time

IST = pytz.timezone("Asia/Kolkata")


# ---------------------------------------------------
# TRAILING STOP EXIT LOGIC
# ---------------------------------------------------
def get_trailing_stop_exit(symbol, trade_date, trade_time, direction, entry, orh, orl):
    """
    Trailing stop exit logic.

    Stop logic (NEW):
    ─────────────────────────────────────────────────
    BUY:  Hard stop = ORH
          If price falls back below ORH, breakout has
          failed → exit immediately with small loss.
          Trailing stop then moves UP as price rises,
          always 2% below the highest high seen.

    SELL: Hard stop = ORL
          If price rises back above ORL, breakdown has
          failed → exit immediately with small loss.
          Trailing stop moves DOWN as price falls,
          always 2% above the lowest low seen.

    Additional exits:
      → 3:15 PM: close the trade regardless
    ─────────────────────────────────────────────────
    Returns: (exit_price, exit_reason)
    """

    date_str    = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    next_day    = pd.to_datetime(trade_date) + pd.Timedelta(days=1)

    try:
        data = yf.download(
            f"{symbol}.NS",
            start=date_str,
            end=next_day.strftime("%Y-%m-%d"),
            interval="5m",
            progress=False,
            auto_adjust=False
        )
    except Exception:
        return None, None, None, None

    if data.empty:
        return None, None, None, None

    try:
        data.index = data.index.tz_convert(IST)
    except Exception:
        return None, None, None, None

    # Day high and low across the full session
    day_high = round(float(data["High"].max()), 2)
    day_low  = round(float(data["Low"].min()), 2)

    # Only candles AFTER signal time
    data_after = data[data.index.time >= trade_time]

    if data_after.empty:
        return None, None, None, None

    orb_range  = orh - orl
    exit_time  = pd.Timestamp("15:15:00").time()

    # ── Minimum ORB range filter ──────────────────────
    # If ORB range is less than 0.5% of entry price,
    # the setup has no momentum — skip it entirely
    orb_range_pct = (orb_range / entry) * 100
    if orb_range_pct < 0.5:
        return None, "ORB Too Narrow", None, None

    # ── Fixed trail: always 2% behind highest price ──
    # Consistent across all stocks regardless of ORB size
    trail_pct = 0.02

    # ─── BUY ───────────────────────────────────────
    if direction == "BUY":

        # Hard floor: ORH — if price falls back below the
        # breakout level, the trade idea is invalidated.
        # Much tighter than ORL, minimises losses.
        initial_stop   = orh
        trailing_stop  = initial_stop
        highest_high   = entry          # tracks best price seen

        for idx, candle in data_after.iterrows():
            candle_time = idx.time()
            high        = float(candle["High"])
            low         = float(candle["Low"])
            close       = float(candle["Close"])

            # ── Step 1: update trailing stop if price moved up ──
            if high > highest_high:
                highest_high  = high
                new_stop      = highest_high * (1 - trail_pct)

                # Trailing stop only ever moves UP, never down
                # And never below the hard floor (ORH)
                trailing_stop = max(trailing_stop, new_stop, initial_stop)

            # ── Step 2: check if this candle hits the stop ──
            effective_stop = max(trailing_stop, initial_stop)

            if low <= effective_stop:
                return round(effective_stop, 2), "Trailing Stop", day_high, day_low

            # ── Step 3: time exit ──
            if candle_time >= exit_time:
                return round(close, 2), "EOD 3:15 PM", day_high, day_low

        return round(float(data_after["Close"].iloc[-1]), 2), "Last Candle", day_high, day_low

    # ─── SELL ──────────────────────────────────────
    else:

        # Hard floor: ORL — if price rises back above the
        # breakdown level, the trade idea is invalidated.
        initial_stop   = orl
        trailing_stop  = initial_stop
        lowest_low     = entry          # tracks best price seen (going down)

        for idx, candle in data_after.iterrows():
            candle_time = idx.time()
            high        = float(candle["High"])
            low         = float(candle["Low"])
            close       = float(candle["Close"])

            # ── Step 1: update trailing stop if price moved down ──
            if low < lowest_low:
                lowest_low    = low
                new_stop      = lowest_low * (1 + trail_pct)

                # Trailing stop only ever moves DOWN, never up
                # And never above the hard floor (ORL)
                trailing_stop = min(trailing_stop, new_stop, initial_stop)

            # ── Step 2: check stop ──
            effective_stop = min(trailing_stop, initial_stop)

            if high >= effective_stop:
                return round(effective_stop, 2), "Trailing Stop", day_high, day_low

            # ── Step 3: time exit ──
            if candle_time >= exit_time:
                return round(close, 2), "EOD 3:15 PM", day_high, day_low

        return round(float(data_after["Close"].iloc[-1]), 2), "Last Candle", day_high, day_low


# ---------------------------------------------------
# MAIN BACKTEST
# ---------------------------------------------------
def backtest_intraday(
    input_file="backtest_opening_range.csv",
    output_file="backtest_results_intraday.csv"
):
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return

    df = pd.read_csv(input_file)

    if df.empty:
        print("❌ No trades found in CSV")
        return

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    required_cols = ["date", "time", "symbol", "direction",
                     "entry_price", "orh", "orl", "prev_close"]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        print(f"❌ Missing columns: {', '.join(missing)}")
        return

    df["date"] = pd.to_datetime(df["date"])
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S").dt.time

    # yfinance 2-month limit
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=55)
    df     = df[df["date"] >= cutoff]

    if df.empty:
        print("❌ No trades within yfinance 2-month data limit")
        return

    # ── Hard ORB time filter ──────────────────────────
    # ORB signals are only valid in the first hour after
    # opening range forms. Anything after 10:30 AM is
    # not an ORB trade — it's just afternoon chop.
    orb_cutoff = pd.Timestamp("15:00:00").time()
    before     = len(df)
    df         = df[df["time"] <= orb_cutoff]
    dropped    = before - len(df)

    if dropped > 0:
        print(f"⏰ Dropped {dropped} signals fired after 10:30 AM (not valid ORB trades)")

    if df.empty:
        print("❌ No trades within valid ORB window (9:30 – 10:30 AM)")
        return

    print(f"📊 Processing {len(df)} trades with trailing stop logic...\n")

    results        = []
    processed      = 0
    skipped_narrow = 0

    for _, row in df.iterrows():

        symbol      = str(row["symbol"]).strip().upper()
        direction   = str(row["direction"]).strip().upper()
        entry       = float(row["entry_price"])
        orh         = float(row["orh"])
        orl         = float(row["orl"])
        prev_close  = float(row["prev_close"])
        trade_date  = row["date"]
        trade_time  = row["time"]

        exit_price, exit_reason, day_high, day_low = get_trailing_stop_exit(
            symbol     = symbol,
            trade_date = trade_date,
            trade_time = trade_time,
            direction  = direction,
            entry      = entry,
            orh        = orh,
            orl        = orl
        )

        if exit_price is None:
            if exit_reason == "ORB Too Narrow":
                print(f"⏭️  Skipped (ORB too narrow): {symbol} {trade_date.date()}")
                skipped_narrow += 1
            else:
                print(f"⚠️  No data: {symbol} {trade_date.date()} — skipping")
            continue

        # PnL
        if direction == "BUY":
            pnl = ((exit_price - entry) / entry) * 100
        else:
            pnl = ((entry - exit_price) / entry) * 100

        result = "WIN" if pnl > 0 else "LOSS"
        processed += 1

        print(
            f"{'✅' if result == 'WIN' else '❌'} "
            f"{processed}/{len(df)} | {symbol} {direction} {trade_date.date()} | "
            f"Entry ₹{entry} → Exit ₹{exit_price} | "
            f"{exit_reason} | {result} ({pnl:+.2f}%)"
        )

        results.append({
            "date":        trade_date.date(),
            "time":        trade_time,
            "symbol":      symbol,
            "direction":   direction,
            "result":      result,
            "entry":       round(entry, 2),
            "orh":         round(orh, 2),
            "orl":         round(orl, 2),
            "prev_close":  round(prev_close, 2),
            "day_high":    day_high,
            "day_low":     day_low,
            "exitprice":   round(exit_price, 2),
            "exit_reason": exit_reason,
            "PnL":         round(pnl, 2)
        })

        time.sleep(0.3)

    if not results:
        print("\n❌ No trades processed successfully")
        return

    out = pd.DataFrame(results)
    out["cumulative_pnl"] = out["PnL"].cumsum()

    out.to_csv(output_file, index=False)

    print(f"\n✅ Saved → {output_file}")
    print("\n" + "=" * 60)
    print("📈 BACKTEST SUMMARY")
    print("=" * 60)
    print(f"Total Signals:        {len(df) + dropped}")
    print(f"Dropped (after 10:30):{dropped}")
    print(f"Skipped (ORB narrow): {skipped_narrow}")
    print(f"Trades Executed:      {len(out)}")
    print(f"Wins:                 {(out['PnL'] > 0).sum()}")
    print(f"Losses:               {(out['PnL'] < 0).sum()}")
    print(f"Win Rate:             {(out['PnL'] > 0).mean() * 100:.2f}%")
    print(f"Avg PnL per Trade:    {out['PnL'].mean():.2f}%")
    print(f"Best Trade:           {out['PnL'].max():.2f}%")
    print(f"Worst Trade:          {out['PnL'].min():.2f}%")
    print(f"Total Cumulative PnL: {out['cumulative_pnl'].iloc[-1]:.2f}%")
    print("=" * 60)

    print("\n📊 Exit Reason Breakdown:")
    print(out["exit_reason"].value_counts().to_string())

    print("\n📋 Sample Trades:")
    print(out.head(10).to_string(index=False))

    return out


if __name__ == "__main__":
    backtest_intraday()