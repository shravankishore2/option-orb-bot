"""
Feature mining — mine_features.py
---------------------------------
Computes the 19 context features (plus forward-path labels) for every
historical signal, using the SAME engine as the live bot (features.py).

Usage:
    python mine_features.py                         # historical_orb_signals.csv
    python mine_features.py --signals X.csv --out Y.csv
    python mine_features.py --universe pit          # point-in-time breadth/sector
    python mine_features.py --legacy                # reproduce the 4-Sep version
"""

import argparse
import datetime as dt
from collections import defaultdict

import pandas as pd

import dhan_client as dhan
import history_cache
from features import (FEATURES, LABELS, MarketContext, build_symbol_history,
                      compute_features, forward_labels, returns_column)

SIGNALS_FILE = "data/research/signals_orbital.csv"
OUTPUT_FILE = "data/research/features_orbital.csv"
SECTOR_FILE = "data/ind_nifty200list.csv"
SECTOR_EXTRA = "data/sectors.csv"            # optional wider symbol->industry map

NIFTY_SECURITY_ID = "13"

__all__ = ["FEATURES", "LABELS", "load_nifty", "sector_map", "build"]


def load_nifty(start, end):
    """NIFTY 50 5-min candles, cached like any symbol."""
    path = history_cache._cache_path("NIFTY_INDEX", "5m")
    cached = history_cache._read_cache(path)

    if history_cache._covers(cached, start, end):
        return dhan.regular_session(cached)

    frames = [] if cached is None else [cached]
    window = start
    while window <= end:
        stop = min(window + dt.timedelta(days=dhan.MAX_INTRADAY_DAYS - 1), end)
        payload = dhan._post("/charts/intraday", {
            "securityId": NIFTY_SECURITY_ID, "exchangeSegment": "IDX_I",
            "instrument": "INDEX", "interval": "5", "oi": False,
            "fromDate": window.isoformat(),
            "toDate": (stop + dt.timedelta(days=1)).isoformat(),   # see dhan_client.get_intraday
        })
        chunk = dhan._to_frame(payload)
        if not chunk.empty:
            chunk = chunk[(chunk.index.date >= window) & (chunk.index.date <= stop)]
        if not chunk.empty:
            frames.append(chunk)
        window = stop + dt.timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    frame = pd.concat(frames)
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    history_cache._write_cache(path, frame)
    return dhan.regular_session(frame)


def sector_map():
    out = {}
    import os
    for path in (SECTOR_EXTRA, SECTOR_FILE):
        if os.path.exists(path):
            t = pd.read_csv(path)
            sym = t["Symbol"].astype(str).str.upper().str.strip()
            out.update(dict(zip(sym, t["Industry"].astype(str))))
    return out


def build(signals_file=SIGNALS_FILE, output_file=OUTPUT_FILE, legacy=False, universe="signals"):
    signals = pd.read_csv(signals_file)
    signals["_day"] = pd.to_datetime(signals["date"]).dt.date
    start, end = signals["_day"].min(), signals["_day"].max()

    sectors = sector_map()

    by_symbol = defaultdict(list)
    for row in signals.to_dict("records"):
        by_symbol[row["symbol"]].append(row)

    members = None
    if universe == "pit":
        import survivorship
        table = survivorship.load_membership()
        members = lambda day: survivorship.members_on(table, day)
        context_symbols = sorted(set(table["symbol"]) | set(by_symbol))
    else:
        context_symbols = sorted(by_symbol)

    print("📈 Loading NIFTY 50 for market context...", flush=True)
    nifty = load_nifty(start - dt.timedelta(days=5), end)

    print(f"🧮 Pass 1/2 — market context over {len(context_symbols)} symbols...", flush=True)
    # Keep only a compact return column per symbol — holding five years of
    # candles for ~400 symbols at once would need several GB.
    returns = {}
    for i, sym in enumerate(context_symbols, 1):
        try:
            c = history_cache.get_candles(sym, start, end, interval=dhan.INTERVAL_5M)
        except dhan.DhanError:
            continue
        if not c.empty:
            returns[sym] = returns_column(c)
        if i % 100 == 0:
            print(f"   … {i}/{len(context_symbols)}", flush=True)
    ctx = MarketContext(None, sectors, nifty=nifty, members=members, returns=returns)
    print(f"   {ctx.matrix.shape[0]:,} stamps x {ctx.matrix.shape[1]} symbols", flush=True)

    print("🧮 Pass 2/2 — per-signal features + forward labels...", flush=True)
    records = []
    for i, symbol in enumerate(sorted(by_symbol), 1):
        try:
            c = history_cache.get_candles(symbol, start, end, interval=dhan.INTERVAL_5M)
        except dhan.DhanError:
            continue
        if c.empty:
            continue
        daily = history_cache.get_daily_candles(symbol, start, end)
        hist = build_symbol_history(c, daily, legacy=legacy)
        by_day = {d: g for d, g in c.groupby(c.index.date)}
        sector = sectors.get(symbol, "UNKNOWN")

        for sig in by_symbol[symbol]:
            g = by_day.get(sig["_day"])
            if g is None or g.empty:
                continue
            feats = compute_features(sig, g, hist, ctx, sector)
            if feats is None:
                continue
            labels = forward_labels(sig, g, legacy=legacy)
            if labels is None:
                continue
            records.append({"date": sig["date"], "time": sig["time"], "symbol": symbol,
                            "direction": sig["direction"], **feats, **labels})

        if i % 50 == 0:
            print(f"   … {i}/{len(by_symbol)} ({len(records):,} rows)", flush=True)

    frame = pd.DataFrame(records)
    frame.to_csv(output_file, index=False)
    print(f"\n💾 {output_file} — {len(frame):,} rows, "
          f"{len(FEATURES)} features + {len(LABELS)} forward labels")
    return frame


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default=SIGNALS_FILE)
    ap.add_argument("--out", default=OUTPUT_FILE)
    ap.add_argument("--legacy", action="store_true")
    ap.add_argument("--universe", choices=["signals", "pit"], default="signals")
    a = ap.parse_args()
    build(a.signals, a.out, legacy=a.legacy, universe=a.universe)
