"""
integrity.py — post-session check that every shadow variant logged a row for every signal.

On 2026-10-08 every variant silently failed to load and the session logged none of them; nobody
noticed until the next day. This runs after the bot exits (orbital-integrity.timer, 15:55 IST,
Mon-Fri) and checks, for the day's live signals in data/live/shadow_signals.csv:

  * every variant in registry.EXPECTED_VARIANTS loads now (a load failure is reported);
  * each logged exactly one row per signal in data/live/shadow_variants.csv (same signal ids,
    no duplicates).

Any problem is sent through the existing notifier (Telegram) and printed (the journal). The
result goes to data/live/integrity.json, which the Scorecard shows. It reads the logs and writes
only that JSON; it never repairs anything.

    python integrity.py [--date YYYY-MM-DD]
"""

import argparse
import datetime as dt
import json
import os
import subprocess
import time

import registry
import shadow

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
KEEP = 30                                         # entries kept in integrity.json


def status_file():
    return shadow.LIVE_DIR / "integrity.json"


def check(day, signals_file=None, variants_file=None, expected=None):
    """{date, signals, rows: {variant: n}, problems: [...], ok}. day: YYYY-MM-DD. By default the
    expected variants are those already live on `day` (registry.VARIANTS_EXPECTED_FROM)."""
    if expected is None:
        expected = [n for n in registry.EXPECTED_VARIANTS if registry.VARIANTS_EXPECTED_FROM.get(n, "") <= day]
    problems = []
    try:
        loaded = {v["name"] for v in registry.shadow_variants()}
    except Exception as e:                        # noqa: BLE001
        loaded = set()
        problems.append(f"the variants could not be loaded at all: {type(e).__name__}: {e}")
    for name in expected:
        if loaded and name not in loaded:
            problems.append(f"{name} fails to load")
    s = shadow._read(signals_file or shadow.SIGNALS_FILE)
    v = shadow._read(variants_file or (shadow.LIVE_DIR / shadow.VARIANTS_FILE.name))
    sig = s[(s["date"] == day) & (s["source"] == "live")] if not s.empty else s
    ids = set(sig["signal_id"]) if not sig.empty else set()
    rows = {}
    if ids:
        today = v[(v["date"] == day) & (v["source"] == "live")] if not v.empty else v
        for name in expected:
            got = today[today["variant"] == name] if not today.empty else today
            rows[name] = len(got)
            got_ids = set(got["signal_id"]) if len(got) else set()
            if not len(got):
                problems.append(f"{name}: no rows for {len(ids)} signals")
            elif got_ids != ids or len(got) != len(ids):
                miss, extra = len(ids - got_ids), len(got_ids - ids)
                dup = len(got) - len(got_ids)
                problems.append(f"{name}: {len(got)} rows for {len(ids)} signals"
                                + (f", {miss} signals without a row" if miss else "")
                                + (f", {extra} rows for unknown signals" if extra else "")
                                + (f", {dup} duplicates" if dup else ""))
    return {"date": day, "signals": len(ids), "rows": rows, "problems": problems, "ok": not problems,
            "checked_at": dt.datetime.now(IST).isoformat(timespec="seconds")}


def message(r):
    if r["ok"]:
        if not r["signals"]:
            return f"ORBITAL integrity {r['date']}: no live signals logged (no session?)"
        return (f"ORBITAL integrity {r['date']}: OK, {r['signals']} signals, every variant logged "
                f"{r['signals']} rows ({', '.join(r['rows'])})")
    return f"ORBITAL integrity {r['date']}: PROBLEM\n- " + "\n- ".join(r["problems"])


def record(r):
    p = status_file()
    try:
        h = json.loads(p.read_text())
    except (FileNotFoundError, ValueError):
        h = []
    h = [e for e in h if e.get("date") != r["date"]] + [r]
    h = sorted(h, key=lambda e: e["date"])[-KEEP:]
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(h, indent=1))
    os.replace(tmp, p)


def last():
    try:
        h = json.loads(status_file().read_text())
        return h[-1] if h else None
    except (FileNotFoundError, ValueError):
        return None


def alert(text, send=None):
    """Through the existing notifier; a failure to send is itself logged, never raised."""
    try:
        if send is None:
            from notifier import send_telegram_message as send
        send(text)
        return True
    except Exception as e:                        # noqa: BLE001
        print(f"⚠️ integrity alert not delivered: {type(e).__name__}: {e}")
        return False


def wait_for_bot(limit_s=1800):
    """The check runs after the session: wait (up to 30 min) while orbital.service is active."""
    end = time.time() + limit_s
    while time.time() < end:
        r = subprocess.run(["systemctl", "is-active", "orbital.service"], capture_output=True, text=True)
        if r.stdout.strip() != "active":
            return True
        time.sleep(30)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="session to check (default: today, IST)")
    ap.add_argument("--no-wait", action="store_true")
    a = ap.parse_args()
    day = a.date or dt.datetime.now(IST).date().isoformat()
    if not a.no_wait and not wait_for_bot():
        r = {"date": day, "signals": 0, "rows": {}, "ok": False, "checked_at": dt.datetime.now(IST).isoformat(timespec="seconds"),
             "problems": ["the bot was still running 30 minutes after the check started; not checked"]}
    else:
        r = check(day)
    record(r)
    text = message(r)
    print(text)
    if not r["ok"]:
        alert(text)
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
