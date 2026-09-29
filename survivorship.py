"""
Point-in-time universe — survivorship.py
-----------------------------------------
The replay used TODAY's Nifty 200 list for every past date. That quietly
includes stocks that only joined the index because they went up, and drops
the ones that left because they went down (survivorship bias).

NSE doesn't publish historical constituent files in a fetchable form, so this
reconstructs membership from the index's own mechanics:

  * Nifty 200 reconstitutes semi-annually, effective the last trading day of
    MARCH and SEPTEMBER, using the six months of data ending 31 Jan / 31 Jul.
  * Constituents are, to a first approximation, the largest and most liquid
    NSE names. Free-float market cap needs share counts we don't have, so we
    rank by six-month MEDIAN daily traded value (close x volume) — a proxy.
    The median beat the mean (82% vs 77% overlap with the real list): a few
    days of frenzied trading shouldn't buy a stock a place in a market-cap index.

The proxy is validated against the one real list we have (the Sep-2025
reconstitution, saved as data/ind_nifty200list.csv). Residual bias that CANNOT
be removed here: companies that merged or delisted (e.g. HDFC Ltd, Mindtree)
are absent from Dhan's instrument master, so they can't be ranked or traded.

Usage:
    python survivorship.py fetch      # daily candles for every NSE equity (~12 min)
    python survivorship.py build      # membership table + validation
    python survivorship.py intraday   # 5-min candles for every ever-member (their spans only)
"""

import sys
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

import dhan_client as dhan
import history_cache

BASE_DIR = Path(__file__).resolve().parent
MASTER = BASE_DIR / "data" / "dhan_nse_equities.csv"
ACTUAL_LIST = BASE_DIR / "data" / "ind_nifty200list.csv"
MEMBERSHIP_FILE = BASE_DIR / "data" / "nifty200_membership.csv"

DAILY_START = dt.date(2020, 7, 1)   # six months before the first rebalance we need
INDEX_SIZE = 200


def all_symbols(shares_only=False):
    """Every NSE cash symbol. shares_only drops ETFs, REITs and InvITs — the
    Nifty indices hold ordinary equity shares only (Dhan type "ES")."""
    master = pd.read_csv(MASTER)
    if shares_only and "INSTRUMENT_TYPE" in master:
        master = master[master["INSTRUMENT_TYPE"] == "ES"]
    return sorted(master["UNDERLYING_SYMBOL"].astype(str).str.upper().str.strip().unique())


def fetch(end=None):
    end = end or dt.date.today()
    symbols = all_symbols()
    ok = 0

    for i, sym in enumerate(symbols, 1):
        try:
            frame = history_cache.get_daily_candles(sym, DAILY_START, end)
            ok += int(not frame.empty)
        except dhan.DhanError as e:
            print(f"⚠️ {sym}: {e}")

        if i % 200 == 0:
            print(f"   … {i}/{len(symbols)} ({ok} with data)", flush=True)

    print(f"✅ daily candles cached for {ok}/{len(symbols)} symbols")


def rebalance_dates(start, end):
    """(effective_date, lookback_start, lookback_end) for each Mar/Sep rebalance."""
    out = []
    for year in range(start.year - 1, end.year + 1):
        for month, look_end in ((3, dt.date(year, 1, 31)), (9, dt.date(year, 7, 31))):
            # last calendar day of the month — the next trading day takes over
            effective = (dt.date(year, month + 1, 1) - dt.timedelta(days=1))
            look_start = look_end - dt.timedelta(days=183)
            out.append((effective, look_start, look_end))
    return [r for r in out if r[0] <= end + dt.timedelta(days=200)]


def traded_value_table(symbols):
    """Daily close x volume for every symbol, as one wide frame."""
    cols = {}
    for sym in symbols:
        path = history_cache._cache_path(sym, "1d")
        frame = history_cache._read_cache(path)
        if frame is None or frame.empty:
            continue
        s = frame["Close"] * frame["Volume"]
        s.index = [d.date() for d in s.index]
        cols[sym] = s[~pd.Index(s.index).duplicated(keep="last")]
    return pd.DataFrame(cols).sort_index()


def build(start=dt.date(2021, 1, 1), end=None):
    end = end or dt.date.today()
    symbols = all_symbols(shares_only=True)

    print("🧮 building traded-value table...")
    tv = traded_value_table(symbols)
    print(f"   {tv.shape[1]} symbols x {tv.shape[0]} sessions")

    rows = []
    for effective, look_start, look_end in rebalance_dates(start, end):
        window = tv[(tv.index >= look_start) & (tv.index <= look_end)]
        if window.empty:
            continue

        # a name needs most of the window to be eligible (new listings excluded)
        coverage = window.notna().mean()
        eligible = window.loc[:, coverage >= 0.8]

        ranked = eligible.median().sort_values(ascending=False)
        members = list(ranked.index[:INDEX_SIZE])

        for rank, sym in enumerate(members, 1):
            rows.append({"effective": effective.isoformat(), "symbol": sym, "rank": rank})

    table = pd.DataFrame(rows)
    table.to_csv(MEMBERSHIP_FILE, index=False)

    periods = table["effective"].nunique()
    print(f"💾 {MEMBERSHIP_FILE.name}: {periods} rebalances, "
          f"{table['symbol'].nunique()} distinct symbols ever in the index")

    validate(table)
    return table


SNAPSHOTS = {                       # real NSE constituent files we have
    "2025-09-30": BASE_DIR / "data" / "ind_nifty200list_2025-09.csv",
    "2026-03-31": BASE_DIR / "data" / "ind_nifty200list.csv",
}


def validate(table):
    """Compare the proxy against every real constituent list we hold."""
    from fetch_symbols import ALIASES, PLACEHOLDERS
    print("\n🔎 VALIDATION vs real Nifty 200 lists:")
    scores = {}
    for effective, path in SNAPSHOTS.items():
        if not path.exists():
            continue
        raw = pd.read_csv(path)["Symbol"].astype(str).str.upper().str.strip()
        actual = {ALIASES.get(x, x) for x in raw if x not in PLACEHOLDERS}
        proxy = set(table.loc[table["effective"] == effective, "symbol"])
        if not proxy:
            continue
        hit = len(actual & proxy)
        scores[effective] = hit / len(actual)
        print(f"   {effective}: overlap {hit}/{len(actual)} = {hit/len(actual)*100:.1f}%   "
              f"(missed {len(actual - proxy)}, extra {len(proxy - actual)})")
    return scores


def members_on(table, day):
    """Set of symbols in the (proxy) index on a given trading day."""
    day = pd.to_datetime(day).date().isoformat()
    past = table[table["effective"] < day]
    if past.empty:
        return set()
    latest = past["effective"].max()
    return set(past.loc[past["effective"] == latest, "symbol"])


def member_spans(table, end):
    """{symbol: (first_day, last_day)} each symbol spent in the index."""
    eff = sorted(table["effective"].unique())
    nxt = {e: (eff[i + 1] if i + 1 < len(eff) else end.isoformat()) for i, e in enumerate(eff)}
    spans = {}
    for e, g in table.groupby("effective"):
        lo = (pd.to_datetime(e) + pd.Timedelta(days=1)).date()
        hi = min(pd.to_datetime(nxt[e]).date(), end)
        for sym in g["symbol"]:
            a, b = spans.get(sym, (lo, hi))
            spans[sym] = (min(a, lo), max(b, hi))
    return spans


def fetch_intraday(start=dt.date(2021, 1, 1), end=None, workers=3):
    """5-min candles for every symbol that was ever a member, over its span only
    (plus 70 days before, for the ATR / volume-norm look-backs).

    Requests are latency-bound, so a few threads help; dhan_client's rate
    limiter is shared and thread-safe, so the 5 req/s cap still holds.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    end = end or dt.date.today()
    spans = member_spans(load_membership(), end)
    jobs = [(sym, max(start, lo - dt.timedelta(days=70)), hi)
            for sym, (lo, hi) in sorted(spans.items())]
    jobs = [j for j in jobs if j[1] <= j[2]]

    def one(job):
        sym, lo, hi = job
        try:
            return not history_cache.get_candles(sym, lo, hi).empty
        except dhan.DhanError as e:
            print(f"⚠️ {sym}: {e}", flush=True)
            return False

    ok = done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for fut in as_completed([pool.submit(one, j) for j in jobs]):
            ok += int(fut.result())
            done += 1
            if done % 25 == 0:
                print(f"   … {done}/{len(jobs)} ({ok} with data)", flush=True)

    import mine_features
    mine_features.load_nifty(start, end)
    print(f"✅ intraday cached for {ok}/{len(jobs)} ever-members, plus NIFTY 50")


def load_membership():
    return pd.read_csv(MEMBERSHIP_FILE)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"fetch": fetch, "build": build, "intraday": fetch_intraday}[cmd]()
