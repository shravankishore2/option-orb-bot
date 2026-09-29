import pandas as pd
import os

import dhan_client as dhan

LOOKBACK_DAYS = 55


# ---------------------------------------------------
# TRAILING STOP EXIT LOGIC (FIXED VERSION)
# ---------------------------------------------------
def get_trailing_stop_exit(symbol, trade_date, trade_time, direction, entry, orh, orl,
                           data=None):
    """Walk the day forward and return (exit_price, reason, day_high, day_low).

    Pass `data` (that day's IST-indexed OHLCV) to reuse candles you already
    hold — the historical labeller does this to avoid an API call per trade.
    """
    date_str = pd.to_datetime(trade_date).strftime("%Y-%m-%d")

    if data is None:
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
            return None, None, None, None

    if data.empty:
        return None, None, None, None

    data_after = data[data.index.time >= trade_time]

    if data_after.empty:
        return None, None, None, None

    orb_range = orh - orl
    exit_time = pd.Timestamp("15:15:00").time()

    # ORB narrow filter
    orb_range_pct = (orb_range / entry) * 100
    if orb_range_pct < 0.5:
        return None, "ORB Too Narrow", None, None

    orb_range = orh - orl

    # Target projected beyond breakout — proper R:R
    # BUY: 50% of ORB range above ORH, SELL: 50% below ORL
    if direction == "BUY":
        midpoint = orh + (orb_range * 0.75)
    else:
        midpoint = orl - (orb_range * 0.75)

    # ---------------------------------------------------
    # BUY TRADE
    # ---------------------------------------------------
    if direction == "BUY":

        highest_high = entry

        for idx, candle in data_after.iterrows():

            candle_time = idx.time()
            high = float(candle["High"])
            low = float(candle["Low"])
            close = float(candle["Close"])

            if high > highest_high:
                highest_high = high

            trailing_stop = highest_high - (orb_range * 0.25)

            exit_candidates = []

            # Hard floor — exit immediately if price breaks below ORL
            if low <= orl:
                day_high = round(highest_high, 2)
                day_low = round(float(data_after["Low"].min()), 2)
                return round(orl, 2), "Hard Stop (ORL)", day_high, day_low

            if high >= midpoint:
                exit_candidates.append(("Target (Midpoint)", midpoint))

            if low <= trailing_stop:
                exit_candidates.append(("Trailing Stop", trailing_stop))

            if candle_time >= exit_time:
                exit_candidates.append(("EOD 3:15 PM", close))

            if exit_candidates:

                reason, exit_price = max(
                    exit_candidates,
                    key=lambda x: x[1]
                )

                day_high = round(highest_high, 2)
                day_low = round(float(data_after["Low"].min()), 2)

                return round(exit_price, 2), reason, day_high, day_low

        return (
            round(float(data_after["Close"].iloc[-1]), 2),
            "Last Candle",
            round(highest_high, 2),
            round(float(data_after["Low"].min()), 2)
        )


    # ---------------------------------------------------
    # SELL TRADE
    # ---------------------------------------------------
    else:

        lowest_low = entry

        for idx, candle in data_after.iterrows():

            candle_time = idx.time()
            high = float(candle["High"])
            low = float(candle["Low"])
            close = float(candle["Close"])

            if low < lowest_low:
                lowest_low = low

            trailing_stop = lowest_low + (orb_range * 0.25)

            exit_candidates = []

            # Hard ceiling — exit immediately if price breaks above ORH
            if high >= orh:
                day_high = round(float(data_after["High"].max()), 2)
                day_low = round(lowest_low, 2)
                return round(orh, 2), "Hard Stop (ORH)", day_high, day_low

            if low <= midpoint:
                exit_candidates.append(("Target (Midpoint)", midpoint))

            if high >= trailing_stop:
                exit_candidates.append(("Trailing Stop", trailing_stop))

            if candle_time >= exit_time:
                exit_candidates.append(("EOD 3:15 PM", close))

            if exit_candidates:

                reason, exit_price = min(
                    exit_candidates,
                    key=lambda x: x[1]
                )

                day_high = round(float(data_after["High"].max()), 2)
                day_low = round(lowest_low, 2)

                return round(exit_price, 2), reason, day_high, day_low

        return (
            round(float(data_after["Close"].iloc[-1]), 2),
            "Last Candle",
            round(float(data_after["High"].max()), 2),
            round(lowest_low, 2)
        )


# ---------------------------------------------------
# MAIN BACKTEST ENGINE
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

    df.columns = [c.strip().lower() for c in df.columns]

    required_cols = [
        "date",
        "time",
        "symbol",
        "direction",
        "entry_price",
        "orh",
        "orl",
        "prev_close"
    ]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        print(f"❌ Missing columns: {', '.join(missing)}")
        return

    df["date"] = pd.to_datetime(df["date"])
    df["time"] = pd.to_datetime(
        df["time"],
        format="%H:%M:%S"
    ).dt.time

    # Dhan serves years of 5-min history, so this is a strategy choice now,
    # not the old yfinance 2-month data limit.
    cutoff = pd.Timestamp.today() - pd.Timedelta(days=LOOKBACK_DAYS)
    df = df[df["date"] >= cutoff]

    if df.empty:
        print(f"❌ No trades in the last {LOOKBACK_DAYS} days")
        return


    # ORB time filter
    orb_cutoff = pd.Timestamp("10:30:00").time()

    before = len(df)
    df = df[df["time"] <= orb_cutoff]

    dropped = before - len(df)

    if dropped > 0:
        print(f"⏰ Dropped {dropped} signals fired after 10:30 AM")


    print(f"\n📊 Processing {len(df)} trades...\n")

    results = []
    processed = 0
    skipped_narrow = 0


    for _, row in df.iterrows():

        symbol = str(row["symbol"]).upper().strip()
        direction = str(row["direction"]).upper().strip()

        entry = float(row["entry_price"])
        orh = float(row["orh"])
        orl = float(row["orl"])
        prev_close = float(row["prev_close"])

        trade_date = row["date"]
        trade_time = row["time"]

        exit_price, exit_reason, day_high, day_low = \
            get_trailing_stop_exit(
                symbol,
                trade_date,
                trade_time,
                direction,
                entry,
                orh,
                orl
            )

        if exit_price is None:

            if exit_reason == "ORB Too Narrow":
                skipped_narrow += 1
            else:
                print(f"⚠️ Missing data: {symbol}")

            continue


        if direction == "BUY":
            pnl = ((exit_price - entry) / entry) * 100
        else:
            pnl = ((entry - exit_price) / entry) * 100

        result = "WIN" if pnl > 0 else "LOSS"

        processed += 1

        print(
            f"{'✅' if result=='WIN' else '❌'} "
            f"{processed}/{len(df)} | "
            f"{symbol} {direction} | "
            f"{trade_date.date()} | "
            f"{exit_reason} | "
            f"{pnl:+.2f}%"
        )


        results.append({

            "date": trade_date.date(),
            "time": trade_time,
            "symbol": symbol,
            "direction": direction,
            "result": result,

            "entry_price": round(entry, 2),
            "orh": round(orh, 2),
            "orl": round(orl, 2),

            "prev_close": round(prev_close, 2),

            "day_high": day_high,
            "day_low": day_low,

            "exit_price": round(exit_price, 2),

            "exit_reason": exit_reason,

            "PnL_%": round(pnl, 2)

        })


    if not results:
        print("❌ No trades processed")
        return

    out = pd.DataFrame(results)

    # ---------------------------------------------------
    # MERGE WITH OLD BACKTEST RESULTS INSTEAD OF WIPING
    # ---------------------------------------------------

    if os.path.exists(output_file):
        try:
            old = pd.read_csv(output_file)

            if not old.empty:
                old.columns = [c.strip() for c in old.columns]

                for col in out.columns:
                    if col not in old.columns:
                        old[col] = ""

                for col in old.columns:
                    if col not in out.columns:
                        out[col] = ""

                out = out[old.columns]

                combined = pd.concat([old, out], ignore_index=True)

                combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                combined["time"] = combined["time"].astype(str).str.strip()
                combined["symbol"] = combined["symbol"].astype(str).str.upper().str.strip()
                combined["direction"] = combined["direction"].astype(str).str.upper().str.strip()

                combined = combined.drop_duplicates(
                    subset=["date", "time", "symbol", "direction"],
                    keep="last"
                )

                out = combined

        except Exception as e:
            print(f"⚠️ Could not merge old results. Saving only new results. Error: {e}")

    # ---------------------------------------------------
    # RECALCULATE EQUITY CURVE ON FULL COMBINED HISTORY
    # ---------------------------------------------------

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["time"] = out["time"].astype(str)

    out = out.sort_values(["date", "time", "symbol"]).reset_index(drop=True)

    out["PnL_%"] = pd.to_numeric(out["PnL_%"], errors="coerce").fillna(0)

    capital = 100
    equity_curve = []

    for pnl in out["PnL_%"]:
        capital *= (1 + pnl / 100)
        equity_curve.append(round(capital, 4))

    out["equity_curve"] = equity_curve
    out["cumulative_pnl"] = out["equity_curve"] - 100

    out["date"] = out["date"].dt.strftime("%Y-%m-%d")

    out.to_csv(output_file, index=False)


    print(f"\n✅ Saved → {output_file}")

    print("\n📈 BACKTEST SUMMARY")

    print(f"Total Signals: {before}")
    print(f"Dropped After 10:30: {dropped}")
    print(f"Skipped Narrow ORB: {skipped_narrow}")
    print(f"Total Saved Trades: {len(out)}")

    print(f"Wins: {(out['PnL_%']>0).sum()}")
    print(f"Losses: {(out['PnL_%']<0).sum()}")

    print(f"Win Rate: {(out['PnL_%']>0).mean()*100:.2f}%")

    print(f"Average Trade: {out['PnL_%'].mean():.2f}%")

    print(f"Best Trade: {out['PnL_%'].max():.2f}%")

    print(f"Worst Trade: {out['PnL_%'].min():.2f}%")

    print(
        f"Total Cumulative PnL: "
        f"{out['cumulative_pnl'].iloc[-1]:.2f}%"
    )


    print("\n📊 Exit Breakdown:")

    print(out["exit_reason"].value_counts())


    return out


if __name__ == "__main__":
    backtest_intraday()