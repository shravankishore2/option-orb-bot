"""
live_engine.py — the live decision engine.

Each cycle it:
  1. takes every symbol's COMPLETED 5-minute candles for today (a candle still
     forming is dropped — its close isn't known yet);
  2. runs the same replay_day() the backtest uses, so a signal fires live on
     exactly the candle it would have fired on in the backtest;
  3. computes the 28 model inputs with the same features.py the model was
     trained on, scores the signal, and decides GO / SKIP / STALE.

Only the FIRST signal per (symbol, direction, day) is ever scored — the same
population the model was trained and evaluated on. A SKIP is final for the day.

The data source is pluggable: DhanSource for live trading, CacheSource for the
paper test that replays past sessions cycle by cycle through this exact code.
"""

import datetime as dt
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

import dhan_client as dhan
import exits
import history_cache
import strategy_config as C
from build_historical_signals import prev_session, replay_day
from features import FEATURES as CONTEXT_FEATURES
from features import MarketContext, build_symbol_history, compute_features, geometry_features

BASE_DIR = Path(__file__).resolve().parent
MODEL_FILE = BASE_DIR / "models" / "orbital_model.pkl"
MODEL_META = BASE_DIR / "models" / "orbital_model.json"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
CANDLE = dt.timedelta(minutes=C.CANDLE_MINUTES)

# A signal older than this when first seen is logged but never sent: acting on
# a breakout from half an hour ago is not the trade that was backtested.
MAX_SIGNAL_AGE = dt.timedelta(minutes=10)

HISTORY_DAYS = 45       # intraday look-back for the opening-range volume norm
DAILY_DAYS = 40         # daily look-back for ATR(14) and the previous session

# Official OHLC of the latest completed session, snapshotted outside market
# hours. Dhan publishes the DAILY bar late (the 22-Sep bar was still missing at
# 03:00 on the 23rd); without this the bot would use the day-before-yesterday's
# close for the ±1.8% filter and the pivots.
SNAPSHOT_FILE = BASE_DIR / "data" / "session_ohlc.csv"


STALE_CLOSE_SHARE = 0.10   # more closes than this equal to the previous session's = not final yet


def stale_close_share(rows, previous):
    """Share of symbols whose stored close equals the previous session's close. Dhan's
    quote close is not final right after the session: on 2026-09-30, 10-01 and 10-06
    about half of the 15:40 closes were still the previous session's, while every high
    and low was right; by the next morning they are final."""
    common = [s for s in rows if s in previous]
    if not common:
        return 0.0
    return sum(abs(rows[s] - previous[s]) < 0.011 for s in common) / len(common)


def take_session_snapshot(symbols, session_date, path=SNAPSHOT_FILE):
    """Store the quote feed's OHLC (official close) for `session_date`, replacing any
    earlier snapshot of that session. Taken at 15:40 by the session run and again before
    the next open (main.py --resnapshot, 08:50), when the close is final."""
    q = dhan.get_session_ohlc(symbols)
    rows = pd.DataFrame([{"session_date": session_date.isoformat(), "symbol": s,
                          "open": o, "high": h, "low": l, "close": c}
                         for s, (o, h, l, c) in q.items()])
    old = pd.read_csv(path, dtype={"session_date": str}) if Path(path).exists() else pd.DataFrame()
    if not old.empty:
        keep_from = (session_date - dt.timedelta(days=10)).isoformat()
        old = old[(old["session_date"] != session_date.isoformat()) & (old["session_date"] >= keep_from)]
    try:                                          # a check only: never stops the snapshot
        if not old.empty and not rows.empty:
            prior = old[old["session_date"] < session_date.isoformat()]
            if not prior.empty:
                last = prior[prior["session_date"] == prior["session_date"].max()]
                share = stale_close_share(dict(zip(rows["symbol"], rows["close"])), dict(zip(last["symbol"], last["close"])))
                if share > STALE_CLOSE_SHARE:
                    print(f"⚠️ snapshot {session_date}: {share:.0%} of closes equal the previous session's — "
                          f"the quote close isn't final yet; the pre-open re-snapshot replaces it")
    except Exception as e:                        # noqa: BLE001
        print(f"⚠️ stale-close check skipped: {type(e).__name__}: {e}")
    pd.concat([old, rows], ignore_index=True).to_csv(path, index=False)
    return len(rows)


def load_snapshot(path=SNAPSHOT_FILE):
    """{(session_date, symbol): (open, high, low, close)} for every stored session."""
    if not Path(path).exists():
        return {}
    t = pd.read_csv(path, dtype={"session_date": str})
    return {(pd.to_datetime(r.session_date).date(), r.symbol): (r.open, r.high, r.low, r.close)
            for r in t.itertuples()}


def latest_completed_session(now):
    """The most recent session that has finished by `now` (from NIFTY candles)."""
    payload = dhan._post("/charts/intraday", {
        "securityId": "13", "exchangeSegment": "IDX_I", "instrument": "INDEX",
        "interval": "5", "oi": False,
        "fromDate": (now.date() - dt.timedelta(days=10)).isoformat(),
        "toDate": (now.date() + dt.timedelta(days=1)).isoformat(),
    })
    f = dhan.regular_session(dhan._to_frame(payload))
    days = sorted(set(f.index.date)) if not f.empty else []
    if days and days[-1] == now.date() and now.time() < dt.time(15, 40):
        days = days[:-1]              # today's session isn't finished yet
    return days[-1] if days else None


def with_latest_session(daily, past, day, snapshot):
    """Make sure `daily` holds the latest completed session before `day`.

    Returns (daily, source) where source says where that session's bar came
    from: "daily" (published), "snapshot" (official close, quote feed) or
    "candles" (last resort: 5-min candles — the close is the last trade, not
    NSE's official close, so it is logged as approximate).
    """
    if past is None or past.empty:
        return daily, "daily"
    latest = max(d for d in past.index.date if d < day) if any(d < day for d in past.index.date) else None
    have = daily.index[-1].date() if not daily.empty else None
    if latest is None or (have is not None and have >= latest):
        return daily, "daily"

    g = past[past.index.date == latest]
    if snapshot:
        o, h, l, c = snapshot
        source = "snapshot"
    else:
        o, h, l, c = (float(g.iloc[0]["Open"]), float(g["High"].max()),
                      float(g["Low"].min()), float(g.iloc[-1]["Close"]))
        source = "candles"
    # Same time zone object as the table it joins: mixing Asia/Kolkata with a
    # fixed +05:30 offset makes pandas fall back to a plain object index.
    tz = getattr(daily.index, "tz", None) or "Asia/Kolkata"
    row = pd.DataFrame({"Open": [o], "High": [h], "Low": [l], "Close": [c],
                        "Volume": [float(g["Volume"].sum())]},
                       index=pd.DatetimeIndex([pd.Timestamp(latest).tz_localize(tz)], name="Datetime"))
    return (row if daily.empty else pd.concat([daily, row])), source


def completed(candles, now):
    """Keep only candles that have finished by `now`."""
    if candles is None or candles.empty:
        return candles
    ends = candles.index + CANDLE
    return candles[ends <= now]


# ------------------------------------------------------------------ data sources
class DhanSource:
    """Live data. History is served from the local cache, topped up incrementally."""

    def history(self, symbol, start, end):
        return history_cache.get_candles(symbol, start, end, interval=dhan.INTERVAL_5M)

    def daily(self, symbol, start, end):
        return history_cache.get_daily_candles(symbol, start, end)

    def today(self, symbol, day, now):
        return completed(dhan.get_intraday(symbol, interval=dhan.INTERVAL_5M,
                                           from_date=day, to_date=day), now)

    def nifty_today(self, day, now):
        payload = dhan._post("/charts/intraday", {
            "securityId": "13", "exchangeSegment": "IDX_I", "instrument": "INDEX",
            "interval": "5", "oi": False,
            "fromDate": day.isoformat(),
            "toDate": (day + dt.timedelta(days=1)).isoformat(),   # see dhan_client.get_intraday
        })
        f = dhan._to_frame(payload)
        if not f.empty:
            f = f[f.index.date == day]
        return completed(dhan.regular_session(f), now)


class CacheSource:
    """Past sessions from the local cache — for the paper test. No API calls.

    Each symbol's file is read once and kept only for [start, end].
    """

    def __init__(self, start=None, end=None):
        self.start, self.end = start, end
        self._frames = {}

    def _frame(self, symbol, tag):
        key = (symbol, tag)
        if key not in self._frames:
            c = history_cache._read_cache(history_cache._cache_path(symbol, tag))
            if c is not None and tag.endswith("m"):
                c = dhan.regular_session(c)
            if c is not None and self.start is not None:
                c = c[(c.index.date >= self.start) & (c.index.date <= self.end)]
            self._frames[key] = c if c is not None else pd.DataFrame()
        return self._frames[key]

    def history(self, symbol, start, end):
        c = self._frame(symbol, "5m")
        return c[(c.index.date >= start) & (c.index.date <= end)] if not c.empty else c

    def daily(self, symbol, start, end):
        c = self._frame(symbol, "1d")
        return c[(c.index.date >= start) & (c.index.date <= end)] if not c.empty else c

    def today(self, symbol, day, now):
        c = self._frame(symbol, "5m")
        return completed(c[c.index.date == day], now) if not c.empty else c

    def nifty_today(self, day, now):
        return self.today("NIFTY_INDEX", day, now)


# ------------------------------------------------------------------ model
def load_bundle(path=MODEL_FILE, meta=MODEL_META):
    """(model, feature_list, threshold, metadata)."""
    with open(path, "rb") as f:
        bundle = pickle.load(f)
    info = json.loads(Path(meta).read_text()) if Path(meta).exists() else {}
    return bundle["model"], bundle["features"], float(bundle["threshold"]), info


def feature_frame(rows):
    """Signal rows -> every model input (geometry + context), as training builds them.

    Values are left un-filled (NaN where unknown); models fill with 0, as in training.
    """
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    df, base = geometry_features(df)
    df = df.loc[:, ~df.columns.duplicated()]
    cols = list(dict.fromkeys(base + list(CONTEXT_FEATURES)))
    return df.reindex(columns=cols).apply(pd.to_numeric, errors="coerce")


def score_frame(model, feature_list, frame):
    X = frame.reindex(columns=feature_list).fillna(0)
    return model.predict_proba(X)[:, 1]


def score_rows(model, feature_list, rows):
    """Score signal rows with the same feature engineering as training."""
    return score_frame(model, feature_list, feature_frame(rows))


# ------------------------------------------------------------------ engine
class LiveEngine:
    """Completed candles -> rules -> features -> scores.

    `model`/`feature_list`/`threshold` are the champion, whose GO decisions are
    sent. `baseline` (a registry.Model, optional) is the frozen v2 model that
    scores every signal alongside it. Each decision carries both scores, both
    versions and the feature vector as of signal time.
    """

    def __init__(self, source, model, feature_list, threshold, symbols, sectors,
                 version=C.MODEL_VERSION, baseline=None, variants=None):
        self.source = source
        self.model = model
        self.features = feature_list
        self.threshold = threshold
        self.version = version
        self.baseline = baseline
        self.variants = list(variants or [])    # shadow only: logged, never decide (registry.shadow_variants)
        self.today_candles = {}           # symbol -> completed candles, refreshed every cycle
        self.symbols = list(symbols)
        self.sectors = sectors
        self.day = None
        self.seen = set()
        self.symbols_with_data = None

    # -- once per session
    def prepare(self, day, snapshot=None):
        """Load each symbol's past: previous session, ATR, opening-range volume norm."""
        self.day = day
        self.seen = set()
        self.prev, self.hist_candles, self.daily = {}, {}, {}
        self.prev_sources = {"daily": 0, "snapshot": 0, "candles": 0}
        self.prepare_errors = {}             # symbol -> first error, surfaced by main.py
        snapshot = snapshot or {}

        for sym in self.symbols:
            try:
                daily = self.source.daily(sym, day - dt.timedelta(days=DAILY_DAYS),
                                          day - dt.timedelta(days=1))
                past = self.source.history(sym, day - dt.timedelta(days=HISTORY_DAYS),
                                           day - dt.timedelta(days=1))
            except dhan.DhanError as e:
                if "credentials" in str(e):
                    raise
                self.prepare_errors[sym] = str(e)[:160]
                continue

            if daily is None or daily.empty:
                self.prepare_errors[sym] = "no daily history"
                continue
            daily = daily[daily.index.date < day]
            latest = [d for d in (past.index.date if past is not None and not past.empty else []) if d < day]
            snap = snapshot.get((max(latest) if latest else None, sym))
            daily, source = with_latest_session(daily, past, day, snap)
            self.prev_sources[source] += 1
            prev = prev_session(daily, day)
            if prev is None:
                self.prepare_errors[sym] = "no previous session"
                continue

            self.prev[sym] = prev
            self.daily[sym] = daily
            self.hist_candles[sym] = past if past is not None else pd.DataFrame()

        return len(self.prev)

    def mark_seen(self, keys):
        """Signals already decided earlier today (e.g. before a restart)."""
        self.seen.update(keys)

    # -- every cycle
    def cycle(self, now):
        """Return a list of decision dicts for signals first seen this cycle."""
        day = now.date()
        if day != self.day:
            self.prepare(day)

        today = {}
        for sym in self.prev:
            try:
                today[sym] = self.source.today(sym, day, now)
            except dhan.DhanError as e:
                if "credentials" in str(e):
                    raise
                today[sym] = None

        # How many symbols have printed a candle today — 0 well after the open
        # means an exchange holiday (main.py stops for the day).
        self.symbols_with_data = sum(1 for g in today.values() if g is not None and not g.empty)
        self.today_candles = today        # the tracker marks open positions from these

        fresh = []
        for sym, g in today.items():
            if g is None or g.empty:
                continue
            ph, pl, pc = self.prev[sym]
            for sig in replay_day(sym, day, g, ph, pl, pc):
                key = (sig["date"], sig["symbol"], sig["direction"])
                if key not in self.seen:
                    fresh.append((sig, g))

        if not fresh:
            return []

        nifty = self.source.nifty_today(day, now)
        ctx = MarketContext({s: g for s, g in today.items() if g is not None},
                            self.sectors, nifty=nifty)

        rows, meta = [], []
        for sig, g in fresh:
            sym = sig["symbol"]
            hist_c = self.hist_candles.get(sym)
            both = pd.concat([hist_c, g]) if hist_c is not None and not hist_c.empty else g
            hist = build_symbol_history(both, self.daily[sym])
            feats = compute_features(sig, g, hist, ctx, self.sectors.get(sym, "UNKNOWN"))
            if feats is None:
                continue
            rows.append({**sig, **feats})
            meta.append(sig)

        if not rows:
            return []

        frame = feature_frame(rows)
        scores = score_frame(self.model, self.features, frame)
        base_scores = (score_frame(self.baseline.model, self.baseline.features, frame)
                       if self.baseline is not None else [None] * len(rows))
        feature_rows = frame.to_dict("records")
        variant_scores = self._variant_scores(frame, base_scores, scores)

        decisions = []
        for i, (sig, sc, bsc, fv) in enumerate(zip(meta, scores, base_scores, feature_rows)):
            self.seen.add((sig["date"], sig["symbol"], sig["direction"]))

            entry_at = dt.datetime.combine(day, dt.datetime.strptime(sig["time"], "%H:%M:%S").time(),
                                           tzinfo=now.tzinfo)
            age = now - entry_at

            if age > MAX_SIGNAL_AGE:
                decision = "STALE"
            elif sc >= self.threshold:
                decision = "GO"
            else:
                decision = "SKIP"

            lv = exits.levels(float(sig["entry_price"]), float(sig["ORH"]),
                              float(sig["ORL"]), sig["direction"])

            decisions.append({
                **sig,
                "score": round(float(sc), 4),
                "threshold": round(self.threshold, 4),
                "decision": decision,
                "age_min": round(age.total_seconds() / 60, 1),
                "stop": lv["stop"],
                "trail_distance": lv["trail_distance"],
                "force_exit": lv["force_exit"],
                "decided_at": now.strftime("%H:%M:%S"),
                "target": lv["target"],
                "model_version": self.version,
                "baseline_version": self.baseline.version if self.baseline is not None else "",
                "baseline_score": round(float(bsc), 4) if bsc is not None else None,
                "baseline_threshold": round(self.baseline.threshold, 4) if self.baseline is not None else None,
                "baseline_go": bool(bsc >= self.baseline.threshold) if bsc is not None else None,
                "features": {k: (None if pd.isna(v) else float(v)) for k, v in fv.items()},
                "variants": {v["name"]: self._variant_decision(v, s[i], sig["time"])
                             for v in self.variants if (s := variant_scores.get(v["name"])) is not None},
            })

        return decisions

    def _variant_decision(self, v, score, entry_time):
        """One shadow variant's decision on one signal. threshold None = the champion's;
        `before` (a time) drops entries at or after it (docs/LATE_CUT_PROTOCOL.md)."""
        thr = self.threshold if v.get("threshold") is None else v["threshold"]
        go = bool(score >= thr)
        if v.get("before") is not None:
            go = go and dt.datetime.strptime(entry_time, "%H:%M:%S").time() < v["before"]
        version = v.get("version") or f"{self.version}@{thr:.4f}<{v['before'].strftime('%H:%M')}"
        return {"version": version, "threshold": round(thr, 4), "score": round(float(score), 4), "go": go}

    def _variant_scores(self, frame, base_scores, scores=None):
        """{variant name: scores} for the shadow variants. A variant that can't be scored
        is skipped; it can never affect the champion's decisions."""
        out = {}
        for v in self.variants:
            try:
                if isinstance(v.get("model"), str) and v["model"] == "champion":
                    if scores is not None:
                        out[v["name"]] = list(scores)
                elif v.get("model") is not None:
                    out[v["name"]] = score_frame(v["model"].model, v["model"].features, frame)
                elif base_scores is not None and base_scores[0] is not None:
                    out[v["name"]] = list(base_scores)
            except Exception as e:              # noqa: BLE001 — shadow only
                print(f"⚠️ shadow variant {v.get('name')} not scored: {type(e).__name__}: {e}")
        return out
