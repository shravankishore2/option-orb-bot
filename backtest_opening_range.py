# backtest_opening_range.py — Analyze historical ORB trades from trade_history.csv
# Calculates profit/loss based on next-day close prices.

import pandas as pd
import yfinance as yf
import datetime
import pytz
import os

IST = pytz.timezone("Asia/Kolkata")


def get_next_day_close(symbol, trade_date):
    """
    Fetch the next day's closing price for a given symbol.
    Returns None if unavailable.
    """
    try:
        ticker = f"{symbol}.NS"
        start = (pd.to_datetime(trade_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        end = (pd.to_datetime(trade_date) + pd.Timedelta(days=2)).strftime("%Y-%m-%d")

        data = yf.download(ticker, start=start, end=end, interval="1d", progress=False)
        if data.empty:
            return None
        return float(data["Close"].iloc[-1])
    except Exception as e:
        print(f"⚠️ Error fetching next-day close for {symbol} ({trade_date}): {e}")
        return None


def backtest_from_history(trade_file="trade_history.csv"):
    """
    Run a backtest on logged ORB trades using next-day close as exit price.
    """
    if not os.path.exists(trade_file):
        print(f"❌ Trade history file '{trade_file}' not found.")
        return

    df = pd.read_csv(trade_file)
    if df.empty:
        print("⚠️ Trade history is empty.")
        return

    print(f"📊 Loaded {len(df)} historical trades from {trade_file}.")

    df["Next_Close"] = None
    df["PnL_%"] = None
    df["Result"] = None

    results = []

    for i, row in df.iterrows():
        symbol = row["Symbol"]
        trade_date = row["Date"]
        signal = row["Signal"]
        entry_price = float(row["Price"])

        next_close = get_next_day_close(symbol, trade_date)
        if next_close is None:
            continue

        if signal == "BUY":
            pnl = ((next_close - entry_price) / entry_price) * 100
        elif signal == "SELL":
            pnl = ((entry_price - next_close) / entry_price) * 100
        else:
            continue

        result = "WIN ✅" if pnl > 0 else "LOSS ❌"

        results.append({
            "Date": trade_date,
            "Symbol": symbol,
            "Signal": signal,
            "Entry_Price": entry_price,
            "Next_Day_Close": next_close,
            "PnL_%": round(pnl, 2),
            "Result": result
        })

    if not results:
        print("⚠️ No valid trades with next-day data found.")
        return

    result_df = pd.DataFrame(results)
    result_df.to_csv("backtest_results.csv", index=False)
    print(f"✅ Saved results -> backtest_results.csv ({len(result_df)} trades)")

    # Summary stats
    total_trades = len(result_df)
    wins = (result_df["PnL_%"] > 0).sum()
    losses = (result_df["PnL_%"] < 0).sum()
    avg_pnl = result_df["PnL_%"].mean()

    print("\n📈 Backtest Summary:")
    print(f"Total Trades: {total_trades}")
    print(f"Wins: {wins} | Losses: {losses}")
    print(f"Win Rate: {wins / total_trades * 100:.2f}%")
    print(f"Average PnL: {avg_pnl:.2f}%")

    print("\nTop 10 Trades:")
    print(result_df.sort_values("PnL_%", ascending=False).head(10)[
        ["Date", "Symbol", "Signal", "PnL_%", "Result"]
    ])


if __name__ == "__main__":
    backtest_from_history()