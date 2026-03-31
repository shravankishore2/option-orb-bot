import pandas as pd
import yfinance as yf
import pytz
import os
import time

IST = pytz.timezone("Asia/Kolkata")


# ---------------------------------------------------
# TRAILING STOP EXIT LOGIC (FIXED VERSION)
# ---------------------------------------------------
def get_trailing_stop_exit(symbol, trade_date, trade_time, direction, entry, orh, orl):

    date_str = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    next_day = pd.to_datetime(trade_date) + pd.Timedelta(days=1)

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

    if direction == "BUY":
        midpoint = entry + orb_range / 2
    else:
        midpoint = entry - orb_range / 2


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

            trailing_stop = highest_high - ((highest_high - orh) * 0.10)

            exit_candidates = []

            if midpoint > entry and high >= midpoint:
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

            trailing_stop = lowest_low + ((orl - lowest_low) * 0.10)

            exit_candidates = []

            if midpoint < entry and low <= midpoint:
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

    cutoff = pd.Timestamp.today() - pd.Timedelta(days=55)
    df = df[df["date"] >= cutoff]

    if df.empty:
        print("❌ No trades within yfinance 2-month data limit")
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

        time.sleep(0.25)


    if not results:
        print("❌ No trades processed")
        return


    out = pd.DataFrame(results)

    capital = 100

    equity_curve = []

    for pnl in out["PnL_%"]:
        capital *= (1 + pnl / 100)
        equity_curve.append(capital)

    out["equity_curve"] = equity_curve
    out["cumulative_pnl"] = out["equity_curve"] - 100

    out.to_csv(output_file, index=False)


    print(f"\n✅ Saved → {output_file}")

    print("\n📈 BACKTEST SUMMARY")

    print(f"Total Signals: {before}")
    print(f"Dropped After 10:30: {dropped}")
    print(f"Skipped Narrow ORB: {skipped_narrow}")
    print(f"Trades Executed: {len(out)}")

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