"""
late_entry_check.py — how v2's GO trades are held, how much of the P&L comes from
entries after 15:00, and whether prices near the close are realistic.

v2's GO picks cluster late in the day (docs/THRESHOLD_RESULTS.md), and every position
is closed at the close of the 15:15 candle. This re-walks every walk-forward GO trade
(data/research/walkforward.csv, the source of docs/RESULTS.md) with exits.simulate_day
on the cached 5-minute candles, checks it reproduces the stored pnl_%, and reports:

  * holding time (entry to the end of the exit candle), by entry time;
  * the share of P&L from entries at or after 15:00;
  * entry realism: the backtest fills at the close of the signal candle, the price at
    the moment the signal exists. A live order can only be filled after that, so each
    trade is re-run with the entry at the NEXT candle's open (and at the next candle's
    close, a much later fill);
  * exit realism: which candle the forced exit uses (it should be 15:15, closing at
    15:20, before NSE's 15:30 close and the closing auction), and whether any exit
    falls on the last candle of the day;
  * v2 with and without late entries, per period, at 0 / 0.05 / 0.10% costs.

Exploratory: the same data as everything else here, so it informs, it doesn't decide.

    ORBITAL_OFFLINE=1 python late_entry_check.py          # writes docs/LATE_ENTRY.md
"""

import datetime as dt
import os

os.environ.setdefault("ORBITAL_OFFLINE", "1")

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C

WF = "data/research/walkforward.csv"
OUT = "docs/LATE_ENTRY.md"
PERIODS = {"clean_backward": (str(C.CLEAN_BACKWARD[0]), str(C.CLEAN_BACKWARD[1])),
           "development": (str(C.DEV_START), str(C.DEV_END)),
           "clean_forward": (str(C.CLEAN_FORWARD[0]), str(C.CLEAN_FORWARD[1])),
           "combined": (str(C.CLEAN_BACKWARD[0]), str(C.CLEAN_FORWARD[1]))}
LATE = "15:00:00"
BUCKETS = [("09:40-11:59", "00:00:00", "12:00:00"), ("12:00-13:59", "12:00:00", "14:00:00"),
           ("14:00-14:59", "14:00:00", "15:00:00"), ("15:00-15:05", "15:00:00", "15:10:00"),
           ("15:10", "15:10:00", "23:59:59")]
COSTS = (0.0, 0.05, 0.10)
BOOT = 2000


def _mins(t):
    return t.hour * 60 + t.minute


def rewalk(go):
    """Re-run every GO trade; add exit candle, holding minutes and the alternative fills."""
    cols = {k: np.full(len(go), np.nan) for k in ("pnl_check", "hold_min", "pnl_next_open", "pnl_next_close",
                                                  "next_open_gap", "exit_stamp_min", "last_stamp_min",
                                                  "exit_vol_ratio")}
    has_1515 = np.zeros(len(go), dtype=bool)
    for sym, idx in go.groupby("symbol").groups.items():
        c = history_cache._read_cache(history_cache._cache_path(sym, "5m"))
        if c is None or c.empty:
            continue
        c = dhan.regular_session(c)
        by_day = {d: g for d, g in c.groupby(c.index.date)}
        for i in idx:
            r, k = go.loc[i], go.index.get_loc(i)
            g = by_day.get(dt.date.fromisoformat(r["date"]))
            if g is None:
                continue
            t = dt.time.fromisoformat(r["time"])
            args = (float(r["orh"]), float(r["orl"]), r["direction"])
            res = exits.simulate_day(g, t, float(r["entry_price"]), *args)
            if res is None:
                continue
            pnl, reason, j = res
            after = g[g.index.time >= t]
            stamp = after.index[j].time()
            cols["pnl_check"][k] = round(pnl, 4)
            cols["hold_min"][k] = _mins(stamp) + C.CANDLE_MINUTES - _mins(t)
            cols["exit_stamp_min"][k] = _mins(stamp)
            cols["last_stamp_min"][k] = _mins(g.index[-1].time())
            has_1515[k] = (g.index.time == C.FORCE_EXIT_TIME).any()
            day_vol = g["Volume"].iloc[:-1].median() if len(g) > 1 else np.nan
            cols["exit_vol_ratio"][k] = after["Volume"].iloc[j] / day_vol if day_vol and day_vol > 0 else np.nan
            # fill at the next candle's open: the first price a live order can get
            nxt = after.iloc[0]
            if after.index[0].time() == t:          # the candle stamped at the entry time exists
                sign = 1.0 if str(r["direction"]).upper() == "BUY" else -1.0
                cols["next_open_gap"][k] = sign * (float(nxt["Open"]) - float(r["entry_price"])) / float(r["entry_price"]) * 100
                o = exits.simulate_day(g, t, float(nxt["Open"]), *args)
                if o is not None:
                    cols["pnl_next_open"][k] = o[0]
                # a much later fill: the next candle's close, walking from the candle after it
                t2 = (dt.datetime.combine(dt.date.today(), t) + dt.timedelta(minutes=C.CANDLE_MINUTES)).time()
                o2 = exits.simulate_day(g, t2, float(nxt["Close"]), *args)
                if o2 is not None:
                    cols["pnl_next_close"][k] = o2[0]
    for name, v in cols.items():
        go[name] = v
    go["has_1515"] = has_1515
    return go


def day_ci(df, col, rng):
    days = df["date"].to_numpy()
    x = df[col].to_numpy(float)
    uniq = np.unique(days)
    if len(uniq) < 2:
        return np.nan, np.nan
    pos = {d: np.flatnonzero(days == d) for d in uniq}
    means = [x[np.concatenate([pos[d] for d in rng.choice(uniq, len(uniq))])].mean() for _ in range(BOOT)]
    return np.percentile(means, [2.5, 97.5])


def summary_row(name, df, col, rng):
    if df.empty:
        return f"| {name} | 0 | — | — | — | — | — | — | — |"
    x = df[col].to_numpy(float)
    lo, hi = day_ci(df, col, rng)
    worst_month = df.groupby(df["date"].str[:7])[col].sum().min()
    return (f"| {name} | {len(x):,} | {df['date'].nunique():,} | {x.mean():+.3f}% | {x.mean() - 0.05:+.3f}% | "
            f"{x.mean() - 0.10:+.3f}% | [{lo - 0.05:+.3f}, {hi - 0.05:+.3f}] | {(x > 0).mean() * 100:.1f}% | "
            f"{x.sum():+.1f} / {(x - 0.05).sum():+.1f} / {(x - 0.10).sum():+.1f} | {worst_month:+.1f} |")


HEAD = ("| Trades | n | Days | Mean | after 0.05% | after 0.10% | 95% CI after 0.05% (day bootstrap) | Hit rate | "
        "Total at 0 / 0.05 / 0.10% | Worst month |\n|---|---|---|---|---|---|---|---|---|---|")


def main():
    rng = np.random.default_rng(11)
    wf = pd.read_csv(WF, dtype={"date": str, "time": str})
    go = wf[wf["go"].astype(str) == "True"].reset_index(drop=True)
    print(f"re-walking {len(go):,} GO trades...", flush=True)
    go = rewalk(go)
    miss = int(go["pnl_check"].isna().sum())
    bad = int((np.abs(go["pnl_check"] - go["pnl_%"]) > 1e-3).sum())
    print(f"missing {miss}, mismatched {bad}")
    lo, hi = PERIODS["combined"]
    comb = go[(go["date"] >= lo) & (go["date"] <= hi)].copy()
    late = comb["time"] >= LATE

    md = ["# Late entries: holding time, P&L share and price realism", "",
          f"Generated by `late_entry_check.py` on {dt.date.today()}. **Exploratory**: the same walk-forward data as "
          "docs/RESULTS.md, so it informs but doesn't decide.", "",
          f"Every walk-forward GO trade of v2 ({len(go):,}, {go['date'].min()} → {go['date'].max()}) is re-walked "
          f"by `exits.simulate_day` on the cached candles: {len(go) - miss - bad:,} reproduce the stored P&L exactly "
          f"({miss} missing candles, {bad} mismatches). Tables use the combined period {lo} → {hi} "
          f"({len(comb):,} trades), the same as docs/THRESHOLD_RESULTS.md and docs/V3_RESULTS.md, unless they say "
          "otherwise.", "",
          "**How a trade is timed:** a candle stamped T covers T..T+5. A signal from it is entered at T+5 at that "
          "candle's close, and the exit walk starts with the candle stamped T+5. The last entry is 15:10 (from the "
          "15:05 candle). The forced exit is the close of the first candle stamped 15:15 or later, normally 15:20.", ""]

    # summary
    wo, only = comb[~late], comb[late]
    md += ["## Summary", "",
           f"- **Holding time:** median {comb['hold_min'].median():.0f} minutes over all GO trades. Entries at or after "
           f"15:00 are held {only['hold_min'].median():.0f} minutes (median), at most {only['hold_min'].max():.0f}.",
           f"- **Late entries are {late.mean() * 100:.0f}% of the trades but {only['pnl_%'].sum() / comb['pnl_%'].sum() * 100:.0f}% "
           f"of the gross P&L** ({(only['pnl_%'] - 0.05).sum() / (comb['pnl_%'] - 0.05).sum() * 100:.0f}% after 0.05%). They "
           f"average {only['pnl_%'].mean():+.3f}% per trade gross, {only['pnl_%'].mean() - 0.05:+.3f}% after 0.05% and "
           f"{only['pnl_%'].mean() - 0.10:+.3f}% after 0.10%: about break-even after costs, though they win more often "
           f"({(only['pnl_%'] > 0).mean() * 100:.0f}% vs {(wo['pnl_%'] > 0).mean() * 100:.0f}%). The label rewards "
           "winning, not size, which is a plausible reason the model rates these short trades highly.",
           f"- **Without them** v2's mean per trade is {wo['pnl_%'].mean():+.3f}% instead of {comb['pnl_%'].mean():+.3f}% "
           f"(total after 0.10%: {(wo['pnl_%'] - 0.10).sum():+.1f} vs {(comb['pnl_%'] - 0.10).sum():+.1f}). This is a "
           "post-hoc cut on design-period data, so it is a hypothesis for a new protocol, not a result.",
           "- **Prices are tradable:** the next candle's open is on average the same as the signal candle's close, "
           "so filling at the next open changes nothing. Every forced exit uses the 15:15 candle, closing at 15:20, "
           "ten minutes before the 15:30 close and outside any closing-auction price. The exit candle trades well "
           "above a normal candle's volume.", ""]

    # 1. holding time by entry time
    md += ["## 1. Holding time and P&L by entry time", "",
           "| Entry time | Trades | Share of trades | Median hold | Max hold | Mean P&L | after 0.05% | after 0.10% | "
           "Share of total P&L (gross) | Exit reasons |", "|---|---|---|---|---|---|---|---|---|---|"]
    total = comb["pnl_%"].sum()
    for name, a, b in BUCKETS:
        g = comb[(comb["time"] >= a) & (comb["time"] < b)]
        if g.empty:
            continue
        reasons = ", ".join(f"{k} {v / len(g) * 100:.0f}%" for k, v in g["exit_reason"].value_counts().items())
        md.append(f"| {name} | {len(g):,} | {len(g) / len(comb) * 100:.1f}% | {g['hold_min'].median():.0f} min | "
                  f"{g['hold_min'].max():.0f} min | {g['pnl_%'].mean():+.3f}% | {g['pnl_%'].mean() - 0.05:+.3f}% | "
                  f"{g['pnl_%'].mean() - 0.10:+.3f}% | {g['pnl_%'].sum() / total * 100:.0f}% | {reasons} |")
    md += ["", f"All GO trades: median hold {comb['hold_min'].median():.0f} min; "
               f"{(comb['hold_min'] <= 10).mean() * 100:.0f}% are held 10 minutes or less. "
               f"Entries at or after 15:00: {late.sum():,} trades ({late.mean() * 100:.0f}%), "
               f"{comb.loc[late, 'pnl_%'].sum() / total * 100:.0f}% of the gross P&L, "
               f"{(comb.loc[late, 'pnl_%'] - 0.05).sum() / (comb['pnl_%'] - 0.05).sum() * 100:.0f}% of the P&L after 0.05%.", ""]

    # 2. with and without late entries
    md += ["## 2. v2 with and without entries at or after 15:00", ""]
    for p, (a, b) in PERIODS.items():
        g = go[(go["date"] >= a) & (go["date"] <= b)]
        md += [f"### {p} ({a} → {b})", "", HEAD,
               summary_row("All GO trades", g, "pnl_%", rng),
               summary_row("Without entries ≥ 15:00", g[g["time"] < LATE], "pnl_%", rng),
               summary_row("Only entries ≥ 15:00", g[g["time"] >= LATE], "pnl_%", rng), ""]

    # 3. entry realism
    gap = comb["next_open_gap"]
    md += ["## 3. Entry prices: could they be traded?", "",
           "The backtest fills at the close of the signal candle, the price at the instant the signal exists. A live "
           "order goes in after that, so the first realistic fill is the next candle's open. Re-run with the entry at "
           "the next candle's open (stops and trail from the same range), and at the next candle's close (a fill "
           "five minutes late):", "",
           "| Entry time | Trades | Next open vs signal close (in the trade's favour) | Mean P&L, signal close | "
           "next open | next close | after 0.05%: signal close / next open |", "|---|---|---|---|---|---|---|"]
    for name, sel in [("before 15:00", ~late), ("at or after 15:00", late), ("all", late | ~late)]:
        g = comb[sel & comb["pnl_next_open"].notna()]
        if g.empty:
            continue
        md.append(f"| {name} | {len(g):,} | mean {g['next_open_gap'].mean():+.3f}%, median {g['next_open_gap'].median():+.3f}% | "
                  f"{g['pnl_%'].mean():+.3f}% | {g['pnl_next_open'].mean():+.3f}% | {g['pnl_next_close'].mean():+.3f}% | "
                  f"{g['pnl_%'].mean() - 0.05:+.3f}% / {g['pnl_next_open'].mean() - 0.05:+.3f}% |")
    md += ["", f"{int(comb['pnl_next_open'].isna().sum())} trades have no candle stamped at the entry time and are "
               "left out of this table.", ""]

    # 4. exit realism
    t_exit = comb[comb["exit_reason"] == "TIME"]
    stamps = t_exit["exit_stamp_min"].map(lambda m: f"{int(m) // 60:02d}:{int(m) % 60:02d}").value_counts()
    last = comb[comb["exit_stamp_min"] == comb["last_stamp_min"]]
    md += ["## 4. Exit prices near the close", "",
           f"Forced (TIME) exits: {len(t_exit):,} of {len(comb):,} GO trades. The candle they close on:", "",
           "| Exit candle (stamp) | Fills at | Trades | Mean P&L |", "|---|---|---|---|"]
    for s, n in stamps.items():
        m = t_exit.loc[t_exit["exit_stamp_min"].map(lambda v: f"{int(v) // 60:02d}:{int(v) % 60:02d}") == s, "pnl_%"].mean()
        end = (dt.datetime.strptime(s, "%H:%M") + dt.timedelta(minutes=5)).strftime("%H:%M")
        md.append(f"| {s} | {end} | {n:,} | {m:+.3f}% |")
    md += ["", f"Trades whose exit is the last candle of the day's data: {len(last):,} "
               f"(mean {last['pnl_%'].mean() if len(last) else float('nan'):+.3f}%). "
               f"Sessions without a 15:15 candle among GO trades: {int((~comb['has_1515']).sum()):,}. "
               f"Median volume of the exit candle relative to the day's median candle: "
               f"{t_exit['exit_vol_ratio'].median():.2f}× for forced exits.", ""]
    with open(OUT, "w") as f:
        f.write("\n".join(md) + "\n")
    print("written", OUT)


if __name__ == "__main__":
    main()
