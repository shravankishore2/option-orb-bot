"""
cross_reference.py
------------------
Merges orb_signals_scored.csv with backtest_results_intraday.csv
to validate whether the ML classifier is actually filtering well.
"""

import pandas as pd

SCORED_FILE  = "orb_signals_scored.csv"
RESULTS_FILE = "backtest_results_intraday.csv"
THRESHOLD    = 0.30

scored  = pd.read_csv(SCORED_FILE)
results = pd.read_csv(RESULTS_FILE)

scored.columns  = [c.strip().lower() for c in scored.columns]
results.columns = [c.strip().lower() for c in results.columns]

scored["date"]      = pd.to_datetime(scored["date"]).dt.strftime("%Y-%m-%d")
scored["symbol"]    = scored["symbol"].str.upper().str.strip()
scored["direction"] = scored["direction"].str.upper().str.strip()

results["date"]      = pd.to_datetime(results["date"]).dt.strftime("%Y-%m-%d")
results["symbol"]    = results["symbol"].str.upper().str.strip()
results["direction"] = results["direction"].str.upper().str.strip()

merged = pd.merge(
    results[["date", "symbol", "direction", "exit_reason", "pnl_%"]],
    scored[["date", "symbol", "direction", "target_prob"]],
    on=["date", "symbol", "direction"],
    how="inner"
)

print(f"Overlapping trades (in both files): {len(merged)}\n")

if merged.empty:
    print("❌ No overlap found. Check that dates/symbols match between files.")
else:
    merged["go"] = merged["target_prob"] >= THRESHOLD
    merged["is_target"] = merged["exit_reason"] == "Target (Midpoint)"

    go   = merged[merged["go"]]
    skip = merged[~merged["go"]]

    print(f"{'='*45}")
    print(f"  THRESHOLD: {THRESHOLD:.0%}")
    print(f"{'='*45}")

    print(f"\n✅ GO trades:   {len(go)}")
    if len(go):
        print(f"   Target hits: {go['is_target'].sum()} ({go['is_target'].mean()*100:.1f}%)")
        print(f"   Avg PnL:     {go['pnl_%'].mean():+.3f}%")
        print(f"   Win rate:    {(go['pnl_%']>0).mean()*100:.1f}%")

    print(f"\n❌ SKIP trades: {len(skip)}")
    if len(skip):
        print(f"   Target hits: {skip['is_target'].sum()} ({skip['is_target'].mean()*100:.1f}%)")
        print(f"   Avg PnL:     {skip['pnl_%'].mean():+.3f}%")
        print(f"   Win rate:    {(skip['pnl_%']>0).mean()*100:.1f}%")

    print(f"\n{'='*45}")
    print("  THRESHOLD SWEEP")
    print(f"{'='*45}")
    print(f"{'Threshold':>10} {'GO':>5} {'Target%':>8} {'AvgPnL':>8} {'WinRate':>8}")
    print("-"*45)

    for t in [0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80]:
        subset = merged[merged["target_prob"] >= t]
        if len(subset) == 0:
            continue
        print(
            f"{t:>10.0%} "
            f"{len(subset):>5} "
            f"{subset['is_target'].mean()*100:>7.1f}% "
            f"{subset['pnl_%'].mean():>+7.3f}% "
            f"{(subset['pnl_%']>0).mean()*100:>7.1f}%"
        )

    print(f"\n{'='*45}")
    print("  EXIT REASON BREAKDOWN — GO trades")
    print(f"{'='*45}")
    if len(go):
        print(go["exit_reason"].value_counts().to_string())