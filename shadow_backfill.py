"""
shadow_backfill.py — replay past sessions into the shadow log (source = "replay").

For sessions before shadow tracking existed. Each session runs through the
LIVE engine every 5 minutes, exactly as paper_trade.py does, on candles
fetched from Dhan once per symbol and then served only up to each cycle time
— so features, scores and labels are what the live bot would have recorded.
Rows are marked source="replay" so the dashboard and the challenger can tell
them apart from live ones.

    python shadow_backfill.py 2026-09-23 2026-10-01
    python shadow_backfill.py 2026-09-30 2026-10-01 --check   # also diff vs live_decisions.csv
"""

import argparse
import datetime as dt
import os

import pandas as pd

import dhan_client as dhan
import registry
import shadow
from fetch_symbols import get_symbols
from live_engine import DhanSource, LiveEngine, completed, load_snapshot
from mine_features import sector_map

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class DayCacheSource(DhanSource):
    """DhanSource that fetches a past session once per symbol and serves it
    only up to `now` (completed candles), cycle after cycle."""

    def __init__(self):
        self._day = {}

    def today(self, symbol, day, now):
        key = (symbol, day)
        if key not in self._day:
            self._day[key] = dhan.get_intraday(symbol, interval=dhan.INTERVAL_5M, from_date=day, to_date=day)
        return completed(self._day[key], now)

    def nifty_today(self, day, now):
        key = ("NIFTY_INDEX", day)
        if key not in self._day:
            self._day[key] = super().nifty_today(day, dt.datetime.combine(day, dt.time(23, 59), tzinfo=IST))
        return completed(self._day[key], now)


def cycle_times(day):
    t = dt.datetime.combine(day, dt.time(9, 40, 20), tzinfo=IST)
    while t.time() <= dt.time(15, 16):
        yield t
        t += dt.timedelta(minutes=5)


def replay(day, book, champ, base, symbols, sectors):
    source = DayCacheSource()
    engine = LiveEngine(source, champ.model, champ.features, champ.threshold, symbols, sectors,
                        version=champ.version, baseline=base)
    engine.prepare(day, snapshot=load_snapshot())
    book.load(day)
    decisions = []
    for now in cycle_times(day):
        new = engine.cycle(now)
        decisions += new
        book.record(new, now, source="replay")
        book.update(engine.today_candles, now)
    for close in (dt.time(15, 20, 20), dt.time(15, 40, 20)):     # main.py's closing passes
        now = dt.datetime.combine(day, close, tzinfo=IST)
        book.update({s: source.today(s, day, now) for s in book.open_symbols()}, now,
                    final=close >= dt.time(15, 40))
    return decisions


def check(day, decisions):
    live = pd.read_csv(os.path.join(BASE_DIR, "live_decisions.csv"), dtype={"date": str})
    live = live[live["date"] == day.isoformat()]
    if live.empty:
        return "no live decisions that day"
    rep = pd.DataFrame(decisions)
    k = ["symbol", "direction"]
    m = live.merge(rep, on=k, how="outer", suffixes=("_live", "_replay"), indicator=True)
    both = m[m["_merge"] == "both"]
    same_time = (both["time_live"] == both["time_replay"]).mean() * 100
    same_dec = (both["decision_live"] == both["decision_replay"]).mean() * 100
    max_ds = (both["score_live"] - both["score_replay"]).abs().max()
    return (f"live {len(live)} / replay {len(rep)} signals, {len(both)} in both: "
            f"{same_time:.0f}% same entry time, {same_dec:.0f}% same decision, max score diff {max_ds:.4f}; "
            f"only live {sum(m['_merge'] == 'left_only')}, only replay {sum(m['_merge'] == 'right_only')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)

    done = shadow._read(shadow.SIGNALS_FILE)
    have = set(done["date"]) if not done.empty else set()
    champ, base = registry.champion(), registry.baseline()
    symbols, sectors, book = get_symbols(), sector_map(), shadow.ShadowBook()

    day = start
    while day <= end:
        if day.weekday() < 5 and day.isoformat() not in have:
            decisions = replay(day, book, champ, base, symbols, sectors)
            labelled = sum(1 for r in book.closed.values())
            if not decisions:
                print(f"  {day}: no signals (holiday?)")
            else:
                go = sum(d["decision"] == "GO" for d in decisions)
                print(f"  {day}: {len(decisions)} signals, GO {go}, labelled {labelled}, "
                      f"open {len(book.open_symbols())}", flush=True)
                if a.check:
                    print(f"         {check(day, decisions)}")
        elif day.isoformat() in have:
            print(f"  {day}: already in the shadow log, skipped")
        day += dt.timedelta(days=1)


if __name__ == "__main__":
    main()
