# plot_portfolio_curve.py — final clean version (aggregated & auto-scaled)
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

BACKTEST_FILE = "backtest_results_intraday.csv"
OUTPUT_FILE = "portfolio_curve.png"

def plot_portfolio_curve(file_path=BACKTEST_FILE):
    # Load data
    df = pd.read_csv(file_path)

    if "Date" not in df.columns or "PnL_%" not in df.columns:
        print("❌ 'Date' or 'PnL_%' column missing in file.")
        return

    # Parse & clean
    df["Date"] = pd.to_datetime(df["Date"]).dt.date  # only date, no time
    df = df.sort_values("Date")

    # Aggregate daily total PnL
    daily_pnl = df.groupby("Date", as_index=False)["PnL_%"].sum()
    daily_pnl["Cumulative_PnL"] = daily_pnl["PnL_%"].cumsum()

    # === Plot Setup ===
    fig, ax1 = plt.subplots(figsize=(8.5, 5))

    # Daily bar chart
    ax1.bar(range(len(daily_pnl)), daily_pnl["PnL_%"], width=0.6,
            color="#5AA9E6", alpha=0.6, label="Daily PnL (%)", zorder=2)
    ax1.set_xlabel("Date", fontsize=11)
    ax1.set_ylabel("Daily PnL (%)", fontsize=11, color="#007BFF")
    ax1.tick_params(axis="y", labelcolor="#007BFF")

    # Cumulative line
    ax2 = ax1.twinx()
    ax2.plot(range(len(daily_pnl)), daily_pnl["Cumulative_PnL"],
             color="#FF7F0E", linewidth=2.5, marker="o",
             label="Cumulative PnL (%)", zorder=3)
    ax2.set_ylabel("Cumulative PnL (%)", fontsize=11, color="#FF7F0E")
    ax2.tick_params(axis="y", labelcolor="#FF7F0E")

    # Grid, labels, and limits
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.set_ylim(min(0, daily_pnl["PnL_%"].min() - 0.5),
                 daily_pnl["PnL_%"].max() + 0.5)
    ax2.set_ylim(min(0, daily_pnl["Cumulative_PnL"].min() - 0.5),
                 daily_pnl["Cumulative_PnL"].max() + 0.5)

    # === Show only existing dates (no missing days) ===
    ax1.set_xticks(range(len(daily_pnl)))
    ax1.set_xticklabels(
        pd.to_datetime(daily_pnl["Date"]).dt.strftime("%b %d"),
        rotation=45, ha="right"
    )

    # Combined legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    # Title and layout
    plt.title("Portfolio Performance: Daily & Cumulative PnL", fontsize=13, weight="bold")
    plt.tight_layout()
    plt.savefig(OUTPUT_FILE, dpi=300)
    plt.show()

    final_pnl = daily_pnl["Cumulative_PnL"].iloc[-1]
    print(f"✅ Plot saved as '{OUTPUT_FILE}'")
    print(f"📊 Final Cumulative Portfolio PnL: {final_pnl:.2f}%")
    print(f"📈 Average PnL per Day: {final_pnl/len(daily_pnl):.2f}%")
    print(daily_pnl)


if __name__ == "__main__":
    plot_portfolio_curve()
