# equity_curve_intraday.py — Plot equity curve from backtest_results_intraday.csv

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

FILE = "backtest_results_intraday.csv"   # <-- change if file name differs
OUTPUT = "equity_curve_intraday.png"

def plot_equity_curve():
    # Load CSV
    try:
        df = pd.read_csv(FILE)
    except FileNotFoundError:
        print(f"❌ File '{FILE}' not found.")
        return

    # Clean column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Ensure required column exists
    if "pnl_%" not in df.columns:
        print("❌ Column 'PnL_%' not found in file.")
        print("Available columns:", df.columns.tolist())
        return

    # Extract returns
    returns = df["pnl_%"].astype(float).tolist()

    if len(returns) == 0:
        print("⚠️ No returns found in file.")
        return

    # Compute cumulative returns
    cumulative = np.cumsum(returns)

    # === PLOT ===
    plt.figure(figsize=(10, 5))
    plt.plot(cumulative, linewidth=2.5, color="#1f77b4", marker="", label="Cumulative Return")

    plt.title("Equity Curve — Intraday ORB Strategy", fontsize=15, fontweight="bold")
    plt.xlabel("Trade Number", fontsize=12)
    plt.ylabel("Cumulative PnL (%)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.5)

    # Final value annotation
    final_val = cumulative[-1]
    plt.text(len(cumulative) - 1, final_val,
             f"{final_val:.2f}%", fontsize=11,
             color="#d62728", weight="bold", ha="right", va="bottom")

    # Save
    plt.tight_layout()
    plt.savefig(OUTPUT, dpi=300)
    plt.show()

    print(f"✅ Equity curve saved as '{OUTPUT}'")
    print(f"📊 Final cumulative PnL: {final_val:.2f}%")

if __name__ == "__main__":
    plot_equity_curve()