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

Variants only (a session whose signals were logged but whose shadow-variant rows weren't):
recompute every variant's decision for that day's LOGGED live signals from their logged inputs
(x_* columns), with the live engine's own variant code. Writes only to --out, never to the shadow
log. --compare diffs the result with the variant rows already logged for that day (the validation
rule in CLAUDE.md: a missed session is backfilled only if a replay of a logged session matches its
live rows exactly).

    python shadow_backfill.py 2026-10-07 2026-10-07 --variants-only --out /tmp/v.csv --compare
"""

import argparse
import datetime as dt
import os

import pandas as pd

import dhan_client as dhan
import registry
import shadow
from fetch_symbols import get_symbols
from features import MODEL_FEATURES
from live_engine import DhanSource, LiveEngine, completed, load_snapshot, score_frame
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


VARIANT_KEYS = ["version", "score", "threshold", "go"]


def variant_rows(day, signals, variants, champ, base, source="live"):
    """Each variant's decision for every signal logged on `day`, recomputed from the logged
    inputs with LiveEngine's own variant code. Returns (rows, champion score check)."""
    sig = signals[(signals["date"] == day.isoformat()) & (signals["source"] == source)] \
        .drop_duplicates("signal_id").reset_index(drop=True)
    if sig.empty:
        return [], "no logged signals"
    frame = pd.DataFrame({f: pd.to_numeric(sig[f"x_{f}"], errors="coerce") for f in MODEL_FEATURES})
    scores = score_frame(champ.model, champ.features, frame)
    base_scores = score_frame(base.model, base.features, frame)
    same = int((pd.Series(scores, dtype=float).round(4) == pd.to_numeric(sig["score"]).astype(float).round(4)).sum())
    check = f"champion scores recomputed from the logged inputs: {same}/{len(sig)} equal the logged score"
    eng = LiveEngine(None, champ.model, champ.features, champ.threshold, [], {}, version=champ.version,
                     baseline=base, variants=variants)
    vs = eng._variant_scores(frame, base_scores, scores)
    rows = []
    for i, r in sig.iterrows():
        for name, d in eng._variant_decisions(i, {"time": r["time"], "symbol": r["symbol"]}, vs).items():
            rows.append({"signal_id": r["signal_id"], "date": r["date"], "time": r["time"], "variant": name,
                         **d, "logged_at": r["logged_at"], "source": r["source"]})
    return rows, check


def compare_variants(rows, logged, day):
    """Match rate per variant: a row matches when version, score, threshold and go are equal."""
    rep = pd.DataFrame(rows)
    live = logged[logged["date"] == day.isoformat()]
    out = {}
    for name in sorted(set(rep["variant"]) | set(live["variant"])):
        a = rep[rep["variant"] == name].set_index("signal_id")
        b = live[live["variant"] == name].drop_duplicates("signal_id").set_index("signal_id")
        if b.empty:
            out[name] = f"no live rows to compare with ({len(a)} replayed): cannot be validated"
            continue
        both = a.index.intersection(b.index)
        eq = sum(str(a.at[s, "version"]) == str(b.at[s, "version"])
                 and abs(float(a.at[s, "score"]) - float(b.at[s, "score"])) < 1e-9
                 and abs(float(a.at[s, "threshold"]) - float(b.at[s, "threshold"])) < 1e-9
                 and str(a.at[s, "go"]) == str(b.at[s, "go"]) for s in both)
        out[name] = (f"{eq}/{len(b)} live rows matched exactly ({eq / len(b) * 100:.1f}%); "
                     f"replayed {len(a)}, live-only {len(b.index.difference(a.index))}, "
                     f"replay-only {len(a.index.difference(b.index))}")
    return out


def variants_only(start, end, out, compare):
    signals = shadow._read(shadow.SIGNALS_FILE)
    logged = shadow._read(shadow.LIVE_DIR / shadow.VARIANTS_FILE.name)
    champ, base, variants = registry.champion(), registry.baseline(), registry.shadow_variants()
    allrows = []
    day = start
    while day <= end:
        rows, check = variant_rows(day, signals, variants, champ, base)
        print(f"  {day}: {len(rows)} variant rows; {check}")
        if compare and rows:
            for name, res in compare_variants(rows, logged, day).items():
                print(f"     {name}: {res}")
        allrows += rows
        day += dt.timedelta(days=1)
    pd.DataFrame(allrows, columns=shadow.VARIANT_COLUMNS).to_csv(out, index=False)
    print(f"written {out} (the shadow log is untouched)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--variants-only", action="store_true", help="recompute variant rows for logged signals")
    ap.add_argument("--out", help="with --variants-only: the file to write (never the shadow log)")
    ap.add_argument("--compare", action="store_true", help="with --variants-only: diff against the logged rows")
    a = ap.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    if a.variants_only:
        if not a.out or os.path.abspath(a.out) == os.path.abspath(str(shadow.LIVE_DIR / shadow.VARIANTS_FILE.name)):
            raise SystemExit("--variants-only needs --out, and it may not be the shadow log")
        return variants_only(start, end, a.out, a.compare)

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
