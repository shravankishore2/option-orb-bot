import pandas as pd
import yfinance as yf
import datetime
import pytz
import os
import time

IST = pytz.timezone("Asia/Kolkata")


def get_intraday_move(symbol, trade_date, trade_time, direction, orh, orl):
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
        return None

    if data.empty:
        return None

    # Convert index to IST
    data.index = data.index.tz_convert(IST)

    # Keep only after signal time
    data_after = data[data.index.time >= trade_time]
    if data_after.empty:
        return None

    entry_open = float(data_after["Open"].iloc[0])
    high_after = float(data_after["High"].max())
    low_after = float(data_after["Low"].min())

    # BUY LOGIC
    if direction == "BUY":
        if high_after > entry_open:
            exit_price = high_after
        else:
            exit_price = low_after

        # Loss limited by ORL
        exit_price = max(exit_price, orl)

    # SELL LOGIC
    else:
        if low_after < entry_open:
            exit_price = low_after
        else:
            exit_price = high_after

        # Loss limited by ORH
        exit_price = min(exit_price, orh)

    return float(exit_price)


def backtest_intraday(trade_file="backtest_opening_range.csv"):
    if not os.path.exists(trade_file):
        print(f"❌ File not found: {trade_file}")
        return

    df = pd.read_csv(trade_file)
    if df.empty:
        print("⚠️ No trade data available.")
        return

    # Normalize columns
    df.columns = [c.strip().lower() for c in df.columns]
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S").dt.time

    print(f"📊 Loaded {len(df)} trades for intraday backtest.\n")

    results = []

    for _, row in df.iterrows():
        symbol = str(row["symbol"]).upper()
        date = row["date"]
        time_signal = row["time"]
        direction = row["direction"]
        entry = float(row["entry_price"])
        orh = float(row["orh"])
        orl = float(row["orl"])

        exit_price = get_intraday_move(symbol, date, time_signal, direction, orh, orl)
        time.sleep(0.3)

        if exit_price is None:
            continue

        # PnL calculation
        if direction == "BUY":
            pnl = ((exit_price - entry) / entry) * 100
        else:
            pnl = ((entry - exit_price) / entry) * 100

        result_label = "WIN" if pnl > 0 else "LOSS"

        results.append({
            "Date": date,
            "Time": time_signal,
            "Symbol": symbol,
            "Direction": direction,
            "Entry": entry,
            "Exit": round(exit_price, 2),
            "PnL_%": round(pnl, 2),
            "Result": result_label
        })

    if not results:
        print("⚠️ No valid trades found.")
        return

    out = pd.DataFrame(results)

    # ADD cumulative PnL
    out["Cumulative_PnL"] = out["PnL_%"].cumsum()

    out.to_csv("backtest_results_intraday.csv", index=False)

    print("✅ Saved: backtest_results_intraday.csv")
    print(out.head())

    # SUMMARY
    print("\n📈 Backtest Summary:")
    print(f"Trades tested: {len(out)}")
    print(f"Wins: {(out['PnL_%'] > 0).sum()}")
    print(f"Losses: {(out['PnL_%'] < 0).sum()}")
    print(f"Win Rate: {(out['PnL_%'] > 0).mean() * 100:.2f}%")
    print(f"Average PnL: {out['PnL_%'].mean():.2f}%")
    print(f"Cumulative PnL: {out['Cumulative_PnL'].iloc[-1]:.2f}%")



if __name__ == "__main__":
    backtest_intraday()