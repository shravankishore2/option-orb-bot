"""
shadow.py — every signal, GO and NO-GO, tracked to its outcome (paper only).

The model only sends GO signals, but judging the filter needs what the SKIPPED
ones went on to do too. Every signal the scanner generates is logged here at
signal time, with both models' scores and the feature vector the models saw,
then walked through the session by the backtest's own exit engine
(exits.simulate) on the candles the bot already fetches each cycle.

Files (data/live/, git-ignored):
  shadow_signals.csv    append-only, written at signal time: decision, scores,
                        threshold, model versions, features (x_*). Never edited.
  shadow_outcomes.csv   append-only, one row per signal, written ONCE when its
                        outcome is complete (stop, trail, target, the 15:15 exit,
                        or the end of the session). That row is the label. It also
                        keeps the stop in force at the exit (exit_stop), so the
                        tracker can show it after the position has closed.
  tracker.json          today's signals with their live state, rewritten every
                        cycle; the dashboard pushes it to the browser.

No-lookahead rules, enforced in code and tests:
  * features come from live_engine at signal time and are never recomputed;
  * a label is written only once its exit candle has COMPLETED (exit candle
    end <= now), and `labelled_at` records when;
  * training reads labels only from shadow_outcomes.csv (labelled_rows()), so
    an open position can never become training data.
"""

import csv
import datetime as dt
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

import exits
import strategy_config as C
from features import FEATURES as CONTEXT_FEATURES, GEOMETRY

BASE_DIR = Path(__file__).resolve().parent
LIVE_DIR = BASE_DIR / "data" / "live"
SIGNALS_FILE = LIVE_DIR / "shadow_signals.csv"
OUTCOMES_FILE = LIVE_DIR / "shadow_outcomes.csv"
TRACKER_FILE = LIVE_DIR / "tracker.json"

CANDLE = dt.timedelta(minutes=C.CANDLE_MINUTES)
MIN_SAMPLE = 30                     # below this a hit rate is too noisy to read

FEATURE_COLUMNS = list(dict.fromkeys(list(GEOMETRY) + list(CONTEXT_FEATURES)))
SIGNAL_COLUMNS = (
    ["signal_id", "date", "time", "symbol", "direction", "entry_price", "ORH", "ORL", "prev_close",
     "decision", "model_go", "model_version", "score", "threshold",
     "baseline_version", "baseline_score", "baseline_threshold", "baseline_go",
     "stop", "trail_distance", "target", "force_exit", "decided_at", "logged_at", "source"]
    + [f"x_{f}" for f in FEATURE_COLUMNS])
# New columns go at the END: a file written before they existed is migrated once
# (header rewritten, old rows left blank in the new columns), never reordered.
OUTCOME_COLUMNS = ["signal_id", "date", "status", "exit_reason", "exit_candle", "exit_time",
                   "exit_price", "pnl_pct", "profit", "mfe_pct", "mae_pct", "labelled_at", "source",
                   "exit_stop"]

# exits.simulate reason -> tracker status
STATUS = {"STOP": "stopped", "TRAIL": "trailed", "TARGET": "target", "TIME": "eod", "LAST": "eod"}
STATUS_LABEL = {"open": "open", "stopped": "stopped out", "trailed": "stopped out (trailing stop)",
                "target": "hit target", "eod": "closed at end of day"}


def signal_id(date, symbol, direction):
    return f"{date}|{symbol}|{direction}"


def _migrate_header(path, columns):
    """A file written before `columns` gained new trailing columns gets the new
    header; its rows are kept as they are (blank in the new columns)."""
    with open(path, newline="") as f:
        have = next(csv.reader(f), [])
    if have == columns:
        return
    if have != columns[:len(have)]:
        raise ValueError(f"{path}: columns {have} are not a prefix of {columns}")
    old = pd.read_csv(path, dtype=str, keep_default_na=False)
    tmp = path.with_suffix(".tmp")
    old.reindex(columns=columns).to_csv(tmp, index=False)
    os.replace(tmp, path)


def _append(path, columns, rows):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    header = not path.exists() or path.stat().st_size == 0
    if not header:
        _migrate_header(path, columns)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if header:
            w.writeheader()
        for r in rows:
            w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in columns})


def _read(path):
    if not Path(path).exists() or Path(path).stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, dtype={"date": str, "time": str, "signal_id": str})


def _atomic_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, default=str))
    os.replace(tmp, path)


def _when(day, hhmmss, tz):
    return dt.datetime.combine(day, dt.datetime.strptime(hhmmss, "%H:%M:%S").time(), tzinfo=tz)


def walk(sig, candles, now):
    """Run the exit engine over a signal's completed candles up to `now`.

    Returns a dict: closed (bool), reason, pnl_pct, current/exit price, stop in
    force, best/worst move (%; favourable positive), exit candle — or None when
    no candle after entry has completed yet.
    """
    entry_t = dt.datetime.strptime(sig["time"], "%H:%M:%S").time()
    after = candles[candles.index.time >= entry_t]
    after = after[after.index + CANDLE <= now]               # completed only, whatever the source
    if after.empty:
        return None

    entry, orh, orl = float(sig["entry_price"]), float(sig["ORH"]), float(sig["ORL"])
    long = str(sig["direction"]).upper() == "BUY"
    sign = 1.0 if long else -1.0
    hi, lo, cl = (after[c].to_numpy(float) for c in ("High", "Low", "Close"))
    state = {}
    res = exits.simulate(hi, lo, cl, [t.time() for t in after.index],
                         entry, orh, orl, sig["direction"], state=state)
    if res is None:                                          # zero-width range: never tradable
        return None
    pnl, reason, i = res
    k = i + 1                                                # candles that belong to the trade
    best = (hi[:k].max() - entry) if long else (entry - lo[:k].min())
    worst = (lo[:k].min() - entry) if long else (entry - hi[:k].max())
    return {
        "closed": reason != "LAST",
        "reason": reason,
        "pnl_pct": float(pnl),
        "price": float(entry * (1 + sign * pnl / 100)),      # exit price, or the latest close if open
        "stop": float(state["stop"]),
        "target": state.get("target"),
        "mfe_pct": float(best / entry * 100),
        "mae_pct": float(worst / entry * 100),
        "exit_candle": after.index[i],
    }


class ShadowBook:
    """Today's shadow signals and their state. One per session."""

    def __init__(self, signals_file=None, outcomes_file=None, tracker_file=None):
        self.signals_file = Path(signals_file or SIGNALS_FILE)
        self.outcomes_file = Path(outcomes_file or OUTCOMES_FILE)
        self.tracker_file = Path(tracker_file or TRACKER_FILE)
        self.day = None
        self.signals = {}            # signal_id -> signal row (as logged)
        self.closed = {}             # signal_id -> outcome row
        self.live = {}               # signal_id -> latest walk() for open positions

    # -- session bookkeeping
    def load(self, day):
        """Today's signals and outcomes from disk (a restart loses nothing)."""
        self.day, self.live = day, {}
        s, o = _read(self.signals_file), _read(self.outcomes_file)
        d = day.isoformat()
        self.signals = {r["signal_id"]: r for r in s[s["date"] == d].to_dict("records")} if not s.empty else {}
        self.closed = {r["signal_id"]: r for r in o[o["date"] == d].to_dict("records")} if not o.empty else {}

    def record(self, decisions, now, source="live"):
        """Log new signals at signal time. Returns how many were new."""
        if self.day != now.date():
            self.load(now.date())
        rows = []
        for d in decisions:
            sid = signal_id(d["date"], d["symbol"], d["direction"])
            if sid in self.signals:
                continue
            feats = d.get("features") or {}
            row = {**{k: v for k, v in d.items() if k != "features"},
                   "signal_id": sid, "model_go": bool(d["score"] >= d["threshold"]),
                   "logged_at": now.isoformat(timespec="seconds"), "source": source,
                   **{f"x_{f}": feats.get(f) for f in FEATURE_COLUMNS}}
            self.signals[sid] = row
            rows.append(row)
        _append(self.signals_file, SIGNAL_COLUMNS, rows)
        return len(rows)

    def open_symbols(self):
        return sorted({r["symbol"] for sid, r in self.signals.items() if sid not in self.closed})

    # -- every cycle
    def update(self, candles_by_symbol, now, final=False):
        """Walk every open signal forward. Label the ones whose outcome is complete.

        final=True (after the close): a position still open is closed at its
        last candle — the backtest's "LAST" exit. Returns the labels written.
        """
        if self.day != now.date():
            self.load(now.date())
        new = []
        for sid, sig in self.signals.items():
            if sid in self.closed:
                continue
            g = candles_by_symbol.get(sig["symbol"])
            if g is None or g.empty:
                continue
            w = walk(sig, g, now)
            if w is None:
                continue
            if w["closed"] or final:
                new.append(self._label(sid, sig, w, now))
            else:
                self.live[sid] = w
        _append(self.outcomes_file, OUTCOME_COLUMNS, new)
        self.write_tracker(now)
        return new

    def _label(self, sid, sig, w, now):
        exit_end = w["exit_candle"] + CANDLE
        entry_at = _when(now.date(), sig["time"], now.tzinfo)
        # The label may only exist once its outcome is complete and observable.
        if exit_end > now:
            raise AssertionError(f"{sid}: label before its exit candle completed ({exit_end} > {now})")
        if w["exit_candle"] < entry_at:
            raise AssertionError(f"{sid}: exit candle before entry")
        row = {
            "signal_id": sid, "date": sig["date"], "status": STATUS[w["reason"]],
            "exit_reason": w["reason"], "exit_candle": w["exit_candle"].strftime("%H:%M:%S"),
            "exit_time": exit_end.strftime("%H:%M:%S"), "exit_price": round(w["price"], 2),
            "pnl_pct": round(w["pnl_pct"], 4), "profit": int(w["pnl_pct"] > 0),
            "mfe_pct": round(w["mfe_pct"], 4), "mae_pct": round(w["mae_pct"], 4),
            "labelled_at": now.isoformat(timespec="seconds"), "source": sig.get("source", "live"),
            "exit_stop": round(w["stop"], 4),            # the stop in force when it closed
        }
        self.closed[sid] = row
        self.live.pop(sid, None)
        return row

    # -- dashboard
    def tracker_rows(self):
        out = []
        for sid, s in sorted(self.signals.items(), key=lambda kv: (kv[1]["time"], kv[1]["symbol"])):
            o, w = self.closed.get(sid), self.live.get(sid)
            entry = float(s["entry_price"])
            row = {
                "id": sid, "symbol": s["symbol"], "time": s["time"][:5], "direction": s["direction"],
                "score": float(s["score"]), "threshold": float(s["threshold"]),
                "go": str(s["model_go"]) in ("True", "true", "1"), "decision": s["decision"],
                "baseline_go": str(s.get("baseline_go")) in ("True", "true", "1"),
                "entry": entry, "stop": _num(s.get("stop")), "target": _num(s.get("target")),
                "status": "open", "status_label": STATUS_LABEL["open"],
                "price": None, "pnl_pct": None,
                # initial_stop: at entry; trail_stop: the stop in force now (open) or at the
                # exit (closed) -- the same `stop` exits.simulate carries through the walk.
                "initial_stop": _num(s.get("stop")), "trail_stop": _num(s.get("stop")), "exit_stop": None,
                "best_pct": None, "worst_pct": None, "exit_time": None,
                "model_version": s.get("model_version"),
            }
            if o is not None:
                stop_at_exit = _exit_stop(o)
                row.update(status=o["status"], status_label=STATUS_LABEL[o["status"]],
                           price=_num(o["exit_price"]), pnl_pct=_num(o["pnl_pct"]),
                           best_pct=_num(o["mfe_pct"]), worst_pct=_num(o["mae_pct"]),
                           exit_time=str(o["exit_time"])[:5], trail_stop=stop_at_exit,
                           exit_stop=stop_at_exit)
            elif w is not None:
                row.update(price=w["price"], pnl_pct=w["pnl_pct"], trail_stop=w["stop"],
                           best_pct=w["mfe_pct"], worst_pct=w["mae_pct"])
            out.append(row)
        return out

    def write_tracker(self, now):
        _atomic_json(self.tracker_file, {
            "version": now.isoformat(timespec="seconds") + f"/{len(self.closed)}",
            "day": self.day.isoformat() if self.day else None,
            "as_of": now.isoformat(timespec="seconds"),
            "rows": self.tracker_rows(),
        })


def _exit_stop(outcome):
    """The stop in force at the exit. Labels written before exit_stop was recorded
    fall back to the exit price for stop and trail exits, which exits.simulate fills
    exactly at the stop; for other exits it is unknown (None)."""
    v = _num(outcome.get("exit_stop"))
    if v is None and outcome.get("exit_reason") in ("STOP", "TRAIL"):
        v = _num(outcome.get("exit_price"))
    return v


def _num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(v) else v


# ---------------------------------------------------------------- reading back
def labelled_rows(signals_file=None, outcomes_file=None):
    """Completed shadow signals as training/evaluation rows (features + label).

    Only signals with an outcome row are returned — an open position has no
    label and can never be used.
    """
    s, o = _read(signals_file or SIGNALS_FILE), _read(outcomes_file or OUTCOMES_FILE)
    if s.empty or o.empty:
        return pd.DataFrame()
    s = s.drop_duplicates("signal_id")
    df = s.merge(o.drop(columns=["date", "source"]).drop_duplicates("signal_id"), on="signal_id", how="inner")
    df = df.rename(columns={f"x_{f}": f for f in FEATURE_COLUMNS})
    df = df.rename(columns={"ORH": "orh", "ORL": "orl", "pnl_pct": "pnl_%"})
    for c in FEATURE_COLUMNS + ["pnl_%", "score", "threshold", "baseline_score", "baseline_threshold"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["profit"] = (df["pnl_%"] > 0).astype(int)
    for c in ("model_go", "baseline_go"):
        df[c] = df[c].astype(str).isin(["True", "true", "1"])
    return df


def check_labels_are_complete(df):
    """Leakage check on shadow rows: every label was written after its exit,
    and every exit is after its entry. Raises on the first violation."""
    if df.empty:
        return
    for r in df.itertuples():
        labelled = dt.datetime.fromisoformat(r.labelled_at)
        exit_end = dt.datetime.combine(dt.date.fromisoformat(r.date),
                                       dt.time.fromisoformat(r.exit_time), tzinfo=labelled.tzinfo)
        entry = dt.datetime.combine(dt.date.fromisoformat(r.date),
                                    dt.time.fromisoformat(r.time), tzinfo=labelled.tzinfo)
        if labelled < exit_end:
            raise AssertionError(f"{r.signal_id}: labelled at {labelled} before its exit {exit_end}")
        if exit_end <= entry:
            raise AssertionError(f"{r.signal_id}: exit {exit_end} not after entry {entry}")


def scorecard(df, by="model_go", costs=C.COST_SENSITIVITY):
    """GO vs NO-GO outcomes, per day and cumulative, for a filter column.

    by: "model_go" (the live champion's decision) or "baseline_go" (frozen v2).
    """
    if df.empty:
        return {"days": [], "total": None}

    def block(g):
        go, no = g[g[by]], g[~g[by]]

        def side(x):
            n = len(x)
            return {"n": n, "too_few": n < MIN_SAMPLE,
                    "hit_rate": float((x["pnl_%"] > 0).mean() * 100) if n else None,
                    "avg_pnl": float(x["pnl_%"].mean()) if n else None,
                    "net": {str(c): (float(x["pnl_%"].mean() - c) if n else None) for c in costs}}
        return {"go": side(go), "nogo": side(no),
                "missed_winners": int((no["pnl_%"] > 0).sum()),
                "avoided_losers": int((no["pnl_%"] < 0).sum()),
                "missed_winner_pnl": float(no.loc[no["pnl_%"] > 0, "pnl_%"].mean()) if (no["pnl_%"] > 0).any() else None,
                "avoided_loser_pnl": float(no.loc[no["pnl_%"] < 0, "pnl_%"].mean()) if (no["pnl_%"] < 0).any() else None,
                "replayed": bool((g["source"] == "replay").any()) if "source" in g else False}

    days = [{"date": d, **block(g)} for d, g in sorted(df.groupby("date"), reverse=True)]
    return {"days": days, "total": block(df), "first": df["date"].min(), "last": df["date"].max()}
