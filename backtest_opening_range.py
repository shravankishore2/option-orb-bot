# backtest_opening_range.py
# Backtest the 9:15–9:35 Opening Range Strategy using 5-minute candles (up to 60 days)

import yfinance as yf
import pandas as pd
from datetime import time
from fetch_symbols import get_symbols


def get_intraday_data(symbol, days=60):
    ticker = f"{symbol}.NS"
    try:
        data = yf.download(ticker, period=f"{days}d", interval="5m", progress=False)
        if data.empty:
            return None

        # --- FIX: Flatten multi-level columns ---
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [c[0] for c in data.columns]

        # Normalize column case
        data.columns = [str(col).capitalize() for col in data.columns]

        data.index = data.index.tz_convert("Asia/Kolkata")
        data["Date"] = data.index.date
        data["Time"] = data.index.time
        return data

    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        return None


def backtest_symbol(symbol, days=60):
    print(f"🔹 Backtesting {symbol}...")
    df = get_intraday_data(symbol, days)
    if df is None or df.empty:
        print(f"⚠️ No data for {symbol}")
        return None

    results = []
    for date, group in df.groupby("Date"):
        # Select 9:15–9:35 range
        window = group[(group["Time"] >= time(9, 15)) & (group["Time"] < time(9, 35))]
        if window.empty:
            continue

        # Extract scalar OHLC values
        o = float(window["Open"].iloc[0])
        h = float(window["High"].max())
        l = float(window["Low"].min())
        c = float(window["Close"].iloc[-1])

        # Direction based on 15-min range
        if c > o:
            direction = "BUY"
        elif c < o:
            direction = "SELL"
        else:
            direction = "HOLD"

        after = group[group["Time"] >= time(9, 35)]
        if after.empty:
            continue

        entry_price = float(after["Open"].iloc[0])
        exit_price = float(group["Close"].iloc[-1])

        pnl = (exit_price - entry_price) if direction == "BUY" else (entry_price - exit_price)
        results.append({
            "Date": date,
            "Symbol": symbol,
            "Direction": direction,
            "Entry": entry_price,
            "Exit": exit_price,
            "PnL": pnl
        })

    if not results:
        print(f"⚠️ No valid days for {symbol}")
        return None

    return pd.DataFrame(results)


def main():
    symbols = get_symbols()
    all_results = []

    for sym in symbols:
        df = backtest_symbol(sym, days=60)
        if df is not None:
            all_results.append(df)

    if not all_results:
        print("❌ No results generated.")
        return

    combined = pd.concat(all_results)
    combined.to_csv("backtest_opening_range.csv", index=False)

    print("\n✅ Backtest complete. Saved -> backtest_opening_range.csv")

    # Summary
    summary = combined.groupby("Symbol")["PnL"].sum().sort_values(ascending=False)
    print("\n🏁 Summary (Total P&L over period):")
    print(summary)
    print("\n💰 Total Portfolio P&L:", round(combined["PnL"].sum(), 2))


if __name__ == "__main__":
    main()