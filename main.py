# main.py — ORBITAL live bot
#
# Every 5 minutes during the session it asks live_engine for new signals on
# COMPLETED candles, scores them with the trained model, and sends only the GO
# decisions to Telegram — with the stop and trail the backtest assumed.
#
# Every signal, GO and NO-GO, is also logged and paper-tracked to its outcome
# by shadow.py (the dashboard's Tracker and Scorecard). The champion model
# decides GO; the frozen v2 baseline scores every signal alongside it
# (registry.py).
#
# Signals only: ORBITAL sends alerts; it has no order path (dhan_client
# refuses every non-market-data endpoint, and tests enforce it).
#
#   python main.py            run continuously (local)
#   python main.py --session  run today's session, snapshot after the close, exit (systemd timer)
#   python main.py --once     run one cycle and exit
#   python main.py --dry-run  decide and log, but never send Telegram messages

import argparse
import csv
import datetime as dt
import os
import sys
import time

import pandas as pd

import dhan_client as dhan
import registry
import shadow
import strategy_config as C
from fetch_symbols import get_symbols
from live_engine import (DhanSource, LiveEngine, load_snapshot, MODEL_FILE,
                         SNAPSHOT_FILE, latest_completed_session, take_session_snapshot)
from mine_features import sector_map
from notifier import (load_telegram_config, reset_sent_notifications_if_new_day,
                      send_signal_notification, send_telegram_message)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SENT_FILE = os.path.join(BASE_DIR, "sent_notifications.csv")
BACKTEST_FILE = os.path.join(BASE_DIR, "backtest_opening_range.csv")
DECISIONS_FILE = os.path.join(BASE_DIR, "live_decisions.csv")
LOT_FILE = os.path.join(BASE_DIR, "Lot_size.csv")

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# First decision is possible once the 09:35 candle has closed; last entry 15:10.
SESSION_START = dt.time(9, 40)
SESSION_END = dt.time(15, 16)

# Candles close on 5-minute boundaries; give Dhan a few seconds to publish.
PUBLISH_DELAY = dt.timedelta(seconds=20)

# After the last cycle the 15:15 candle (the forced exit) is still forming.
# From CLOSING_PASS_AFTER the shadow tracker fetches the symbols that still
# have open paper positions (only those) to complete them; from FINAL_PASS_AFTER
# anything still open is closed at its last candle, as the backtest does.
CLOSING_PASS_AFTER = dt.time(15, 20)
FINAL_PASS_AFTER = dt.time(15, 40)

# If no symbol has a single candle this long after the open, the exchange is
# closed (holiday) — stop for the day instead of polling empty data.
HOLIDAY_CHECK_AFTER = dt.time(9, 50)

BACKTEST_COLUMNS = ["date", "time", "symbol", "direction",
                    "entry_price", "ORH", "ORL", "prev_close"]
DECISION_COLUMNS = ["date", "time", "symbol", "direction", "entry_price", "ORH", "ORL",
                    "prev_close", "score", "threshold", "decision", "age_min",
                    "stop", "trail_distance", "force_exit", "decided_at"]


def now_ist():
    return dt.datetime.now(IST)


def lot_sizes():
    """F&O lot sizes (blank for stocks without derivatives)."""
    from fetch_symbols import ALIASES
    try:
        t = pd.read_csv(LOT_FILE)
        syms = t["Symbol"].astype(str).str.upper().str.strip().map(lambda s: ALIASES.get(s, s))
        return dict(zip(syms, t["lot_size"]))
    except Exception:
        return {}


def append_rows(path, columns, rows):
    header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if header:
            w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})


def decided_today(day):
    """Signals already decided today — so a restart never re-sends or re-scores."""
    if not os.path.exists(DECISIONS_FILE):
        return set()
    try:
        d = pd.read_csv(DECISIONS_FILE, dtype=str)
    except Exception:
        return set()
    d = d[d["date"] == day.isoformat()]
    return set(zip(d["date"], d["symbol"], d["direction"]))


def update_current_prices(day):
    """Refresh current_price on today's sent signals (dashboard P&L column)."""
    if not os.path.exists(SENT_FILE):
        return
    try:
        df = pd.read_csv(SENT_FILE)
        today = df["date"].astype(str).str[:10] == day.isoformat()
        if not today.any():
            return
        prices = dhan.get_ltp(df.loc[today, "symbol"].astype(str).unique())
        for i in df.index[today]:
            p = prices.get(str(df.at[i, "symbol"]).upper())
            if p:
                df.at[i, "current_price"] = p
        df.to_csv(SENT_FILE, index=False)
    except Exception as e:
        print(f"⚠️ could not refresh current prices: {e}")


def alert(message, dry_run):
    print(message)
    if dry_run:
        return
    try:
        send_telegram_message(message)
    except Exception as e:
        print(f"⚠️ alert not delivered: {e}")


def handle(decisions, lots, dry_run):
    append_rows(DECISIONS_FILE, DECISION_COLUMNS, decisions)

    go = [d for d in decisions if d["decision"] == "GO"]
    for d in decisions:
        tag = {"GO": "✅ GO  ", "SKIP": "❌ SKIP", "STALE": "⏰ STALE"}[d["decision"]]
        print(f"   {tag} {d['symbol']:<12} {d['direction']:<4} @ {d['entry_price']:<9} "
              f"score {d['score']:.3f} (thr {d['threshold']:.3f})")

    if not go:
        return

    if not dry_run:                      # the signal log records what was actually sent
        append_rows(BACKTEST_FILE, BACKTEST_COLUMNS, go)

    for d in go:
        if dry_run:
            print(f"   [dry-run] would send {d['symbol']} {d['direction']}")
            continue
        send_signal_notification(
            symbol=d["symbol"], direction=d["direction"],
            entry_price=d["entry_price"], close=d["entry_price"],
            prev_close=d["prev_close"], orh=d["ORH"], orl=d["ORL"],
            lot_size=lots.get(d["symbol"], ""),
            extras={"Score": f"{d['score']:.2f} (GO ≥ {d['threshold']:.2f})",
                    "Stop": f"₹{d['stop']}",
                    "Trail": f"₹{d['trail_distance']} behind best price",
                    "Exit by": d["force_exit"]},
        )


def track(book, decisions, engine, now):
    """Shadow-log new signals and walk every open one forward. Never fatal."""
    try:
        book.record(decisions, now)
        labels = book.update(engine.today_candles, now)
        if labels:
            print(f"   🧾 {len(labels)} paper outcome(s) labelled: " + ", ".join(
                f"{l['signal_id'].split('|')[1]} {l['status']} {float(l['pnl_pct']):+.2f}%" for l in labels[:6]))
    except Exception as e:                       # tracking must never stop the signal bot
        print(f"⚠️ shadow tracking failed this cycle: {type(e).__name__}: {e}")


def closing_pass(book, engine, now):
    """After the close: complete open paper positions from their own candles."""
    try:
        if book.day != now.date():
            book.load(now.date())
        symbols = book.open_symbols()
        if not symbols:
            return 0
        final = now.time() >= FINAL_PASS_AFTER
        candles = {}
        for sym in symbols:
            try:
                candles[sym] = engine.source.today(sym, now.date(), now)
            except dhan.DhanError as e:
                if "credentials" in str(e):
                    raise
        labels = book.update(candles, now, final=final)
        print(f"🧾 closing pass ({'final' if final else 'waiting for the 15:15 candle'}): "
              f"{len(symbols)} open, {len(labels)} labelled, {len(book.open_symbols())} still open")
        return len(labels)
    except dhan.DhanError as e:
        print(f"⚠️ closing pass failed: {e}")
        return 0


def run_cycle(engine, lots, dry_run, now=None, book=None):
    now = now or now_ist()
    day = now.date()

    if engine.day != day:
        print(f"🗓️  Preparing {day} — loading history for {len(engine.symbols)} symbols...")
        reset_sent_notifications_if_new_day()
        ready = engine.prepare(day, snapshot=load_snapshot())
        engine.mark_seen(decided_today(day))
        src = engine.prev_sources
        print(f"✅ {ready} symbols ready — previous close from daily bar {src['daily']}, "
              f"snapshot {src['snapshot']}, candles {src['candles']}")
        if engine.prepare_errors:
            sym, err = next(iter(engine.prepare_errors.items()))
            print(f"⚠️  {len(engine.prepare_errors)} symbols not prepared, e.g. {sym}: {err}")
        if ready == 0:
            engine.day = None            # prepare again next cycle
            alert("⚠️ ORBITAL could not prepare any symbol today (see the log). "
                  "It will retry next cycle.", dry_run)
            return
        if src["candles"]:
            alert(f"⚠️ ORBITAL: {src['candles']} symbols are using an APPROXIMATE previous close "
                  f"(no daily bar, no snapshot). Run `python main.py --snapshot` outside market "
                  f"hours to avoid this.", dry_run)

    print(f"🔁 {now.strftime('%H:%M:%S')} cycle")
    decisions = engine.cycle(now)

    if not decisions:
        print("   no new signals")
    handle(decisions, lots, dry_run)
    if book is not None:
        track(book, decisions, engine, now)

    if not dry_run:
        update_current_prices(day)


def ensure_snapshot(symbols, now):
    """Outside market hours, store the latest session's official OHLC once."""
    try:
        session = latest_completed_session(now)
        if session is None:
            return
        have = {d for d, _ in load_snapshot()}
        if session not in have:
            n = take_session_snapshot(symbols, session)
            print(f"📸 Stored official OHLC for {session} ({n} symbols) → {SNAPSHOT_FILE.name}")
    except dhan.DhanError as e:
        print(f"⚠️ snapshot failed: {e}")


def next_boundary(now):
    minute = (now.minute // C.CANDLE_MINUTES + 1) * C.CANDLE_MINUTES
    base = now.replace(minute=0, second=0, microsecond=0)
    return base + dt.timedelta(minutes=minute) + PUBLISH_DELAY


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="one cycle, then exit")
    ap.add_argument("--dry-run", action="store_true", help="never send Telegram")
    ap.add_argument("--session", action="store_true",
                    help="run today's session, store the closing snapshot after 15:40, then exit")
    ap.add_argument("--snapshot", action="store_true",
                    help="store the latest session's official OHLC, then exit (run after 15:40)")
    a = ap.parse_args()

    if a.snapshot:
        ensure_snapshot(get_symbols(), now_ist())
        return 0

    print("🚀 ORBITAL live bot" + ("  [DRY RUN — nothing is sent]" if a.dry_run else ""))

    if a.dry_run:
        # Keep practice decisions out of the real log: the real bot reads it to
        # avoid re-sending, so a morning dry run would silence an afternoon run.
        global DECISIONS_FILE
        DECISIONS_FILE = os.path.join(BASE_DIR, "live_decisions_dryrun.csv")

    if not os.path.exists(MODEL_FILE):
        print(f"❌ No model at {MODEL_FILE}. Train one: python walk_forward.py --train-live")
        return 1

    baseline = registry.baseline()
    champ = registry.champion()
    print(f"🧠 Champion {champ.version}: trained through {champ.trained_through or '?'}, "
          f"threshold {champ.threshold:.3f}, {len(champ.features)} features")
    print(f"🧊 Baseline {baseline.version} (frozen) scores every signal alongside it")
    try:                                         # shadow variants decide nothing; never fatal
        variants = registry.shadow_variants()
        print("👥 Shadow variants (logged only): " + ", ".join(f"{v['name']} (GO >= {v['threshold']:.3f})"
                                                           for v in variants))
    except Exception as e:                       # noqa: BLE001
        variants = []
        print(f"⚠️ shadow variants not loaded: {type(e).__name__}: {e}")

    if not a.dry_run:
        try:
            load_telegram_config()
        except Exception as e:
            print(f"❌ Telegram config invalid: {e}")
            return 1

    engine = LiveEngine(DhanSource(), champ.model, champ.features, champ.threshold,
                        get_symbols(), sector_map(), version=champ.version, baseline=baseline,
                        variants=variants)
    lots = lot_sizes()
    if a.dry_run:
        dry = shadow.LIVE_DIR / "dryrun"
        book = shadow.ShadowBook(dry / "shadow_signals.csv", dry / "shadow_outcomes.csv", dry / "tracker.json")
    else:
        book = shadow.ShadowBook()

    while True:
        now = now_ist()
        in_session = now.weekday() < 5 and SESSION_START <= now.time() <= SESSION_END

        if in_session:
            try:
                run_cycle(engine, lots, a.dry_run, now, book)
                # Only a holiday if stocks WERE prepared and none printed a candle —
                # zero prepared stocks is a data problem, not a closed exchange.
                if (now.time() >= HOLIDAY_CHECK_AFTER and engine.prev
                        and engine.symbols_with_data == 0):
                    print("🏖️  No stock has printed a candle today — exchange holiday. Stopping.")
                    return 0
            except dhan.DhanError as e:
                if "credentials" in str(e):
                    alert("⛔ ORBITAL stopped: no usable Dhan token. On the VM the token comes "
                          "from the shared token service — check it; locally, update config.ini.",
                          a.dry_run)
                    return 2
                print(f"⚠️ data error this cycle, will retry: {e}")
        else:
            print(f"⏸️  {now.strftime('%a %H:%M')} — outside the trading window")
            if now.weekday() < 5 and now.time() >= CLOSING_PASS_AFTER:
                closing_pass(book, engine, now)
            if now.time() >= dt.time(15, 40) or now.time() < dt.time(9, 0):
                ensure_snapshot(engine.symbols, now)
                if a.session and now.time() >= dt.time(15, 40):
                    print("✅ Session finished and closing prices stored. Exiting.")
                    return 0

        if a.once:
            return 0

        wake = next_boundary(now_ist())
        time.sleep(max(5, (wake - now_ist()).total_seconds()))


if __name__ == "__main__":
    sys.exit(main())
