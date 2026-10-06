# webapp.py — ORBITAL dashboard
#
#   Live         today's model decisions (GO / SKIP / STALE), filterable by time
#   Historical   the live signal log or the walk-forward backtest, with filters
#   Performance  thesis results: strategy vs baselines, equity, months, the model
#   About        how it works, read straight from strategy_config.py
#
# Reads CSV/JSON files only — it never calls the broker.
#   python webapp.py                   http://127.0.0.1:5050
#   ORBITAL_PORT=8080 python webapp.py another port
#   ORBITAL_DEBUG=1 python webapp.py   Flask debugger on (local use only)
#   waitress-serve --listen=127.0.0.1:5050 webapp:app   production (the VM, behind Caddy)
#
# Login (auth.py): set ORBITAL_DASHBOARD_PASSWORD or ORBITAL_DASHBOARD_PASSWORD_FILE
# and every page needs the password. With neither set the dashboard is open, for
# local use; ORBITAL_REQUIRE_LOGIN=1 (the VM) refuses to start that way.

import datetime as dt
import hashlib
import json
import os
import re
import hmac
import threading
import time
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd
from flask import (Flask, Response, abort, g, jsonify, render_template, request, redirect,
                   stream_with_context, url_for)
from werkzeug.middleware.proxy_fix import ProxyFix

import auth
import charts
import exits
import registry
import shadow
import strategy_config as C

app = Flask(__name__)
# Caddy terminates HTTPS and says so in X-Forwarded-Proto; trust one hop so
# request.is_secure is true and the session cookie gets the Secure flag.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# ---------------------------------------------------------------------------
# LOGIN — password only, signed session cookie (same scheme as QuantRadar)
# ---------------------------------------------------------------------------

SESSION_HOURS = 12
PASSWORD = auth.configured_password()
REQUIRE_LOGIN = os.getenv("ORBITAL_REQUIRE_LOGIN") == "1"
if not PASSWORD and REQUIRE_LOGIN:
    raise SystemExit("ORBITAL_REQUIRE_LOGIN=1 but no dashboard password is configured")
_secret = os.getenv("ORBITAL_DASHBOARD_SECRET")
SIGNER = auth.SessionSigner(_secret.encode() if _secret else None, ttl_s=SESSION_HOURS * 3600)
CHECKER = auth.PasswordCheck(PASSWORD) if PASSWORD else None
OPEN_ENDPOINTS = {"login_page", "login_form", "api_login", "logout", "api_logout", "healthz",
                  "static", "robots"}          # static: CSS/JS/figures only, no data


def logged_in():
    return CHECKER is None or SIGNER.valid(request.cookies.get(auth.COOKIE))


def safe_next(target):
    """Only same-site paths — never an open redirect."""
    return target if target and target.startswith("/") and not target.startswith("//") \
        and "\\" not in target else url_for("live")


@app.before_request
def require_login():
    if request.endpoint in GUEST_ENDPOINTS:
        return guest_gate()
    if request.path.startswith("/guest") and request.routing_exception is not None:
        return None                               # e.g. POST to a guest page: a plain 405/404
    if request.endpoint in OPEN_ENDPOINTS or logged_in():
        return None
    if request.path.startswith("/api/"):          # the live stream asks, like QuantRadar's client
        return jsonify(error="login required"), 401
    return redirect(url_for("login_page", next=request.full_path.rstrip("?")))


@app.after_request
def security_headers(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers.setdefault("Cache-Control", "no-store")
    if request.endpoint in GUEST_ENDPOINTS or request.endpoint == "robots":
        resp.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    return resp


def _try_password(password):
    """(ok, error message, retry_after) — QuantRadar's wording."""
    wait = CHECKER.locked_for()
    if wait > 0:
        return False, f"Too many attempts. Try again in {round(wait)} s.", wait
    if not CHECKER.check(password):
        wait = CHECKER.locked_for()     # this failure may have been the fifth
        if wait > 0:
            return False, f"Too many attempts. Try again in {round(wait)} s.", wait
        return False, "Wrong password.", 0
    return True, "", 0


def _cookie_secure():
    # Deployed (REQUIRE_LOGIN) = always behind HTTPS: Secure regardless of what the
    # proxy headers say. Locally, only when the request really is HTTPS.
    return REQUIRE_LOGIN or request.is_secure


def _set_session(resp):
    resp.set_cookie(auth.COOKIE, SIGNER.issue(), max_age=SESSION_HOURS * 3600, httponly=True,
                    samesite="Strict", secure=_cookie_secure(), path="/")
    return resp


@app.get("/login")
def login_page():
    if logged_in():
        return redirect(safe_next(request.args.get("next")))
    return render_template("login.html", next_url=safe_next(request.args.get("next")), error="")


@app.post("/login")
def login_form():
    """No-JavaScript fallback for the login form."""
    if CHECKER is None:
        return redirect(url_for("live"))
    ok, error, _ = _try_password(request.form.get("password", ""))
    nxt = safe_next(request.form.get("next"))
    if ok:
        return _set_session(redirect(nxt, code=303))
    return render_template("login.html", next_url=nxt, error=error), 401


@app.post("/api/login")
def api_login():
    if CHECKER is None:
        return Response(status=204)
    body = request.get_json(silent=True) or {}
    ok, error, wait = _try_password(str(body.get("password", "")))
    if ok:
        return _set_session(Response(status=204))
    if wait:
        return jsonify(error="too many attempts", retry_after=round(wait)), 429, \
            {"Retry-After": str(int(wait) + 1)}
    return jsonify(error="wrong password"), 401


@app.post("/api/logout")
def api_logout():
    resp = Response(status=204)
    resp.delete_cookie(auth.COOKIE, path="/", secure=_cookie_secure(), httponly=True, samesite="Strict")
    return resp


@app.post("/logout")
def logout():
    resp = redirect(url_for("login_page"), code=303)
    resp.delete_cookie(auth.COOKIE, path="/", secure=_cookie_secure(), httponly=True, samesite="Strict")
    return resp


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/api/me")
def api_me():
    return {"ok": True}


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SENT_FILE = os.path.join(BASE_DIR, "sent_notifications.csv")
LIVE_LOG = os.path.join(BASE_DIR, "backtest_opening_range.csv")
DECISIONS_FILE = os.path.join(BASE_DIR, "live_decisions.csv")
RESEARCH = os.path.join(BASE_DIR, "data", "research")
SUMMARY_FILE = os.path.join(RESEARCH, "summary.json")
if not os.path.exists(SUMMARY_FILE):          # deployed copy: data/ isn't in git, docs/ is
    SUMMARY_FILE = os.path.join(BASE_DIR, "docs", "summary.json")
WALKFORWARD_FILE = os.path.join(RESEARCH, "walkforward.csv")
PAPER_FILE = os.path.join(RESEARCH, "paper_trades.csv")
MODEL_META = os.path.join(BASE_DIR, "models", "orbital_model.json")
PREREG = os.path.join(BASE_DIR, "docs", "PREREGISTRATION.md")
FIGURES = os.path.join(BASE_DIR, "static", "figures")

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Signals can fire from 09:40; the filter lets you look at any cut-off.
TIME_FILTERS = ["09:45", "10:00", "10:15", "10:30", "10:45", "11:00", "11:30",
                "12:00", "12:30", "13:00", "13:30", "14:00", "14:30", "15:00", "15:15"]

MAX_ROWS = 500

PERIOD_LABELS = {
    "clean_backward": "Clean test · 2022-01 → 2023-08",
    "development": "Development · 2023-09 → 2026-09",
    "clean_forward": "Sessions 2026-09-05 → 09-22",
    "v2_forward": "v2 clean test · from 2026-09-23",
}
MODEL_ROW = "Model v2 · profit (live)"
V1_ROW = "Model v1 · hit_1_5r (pre-registered)"
# Which model a period's headline should show: the one pre-registered for it.
HEADLINE_FOR = {"clean_backward": V1_ROW, "clean_forward": V1_ROW,
                "development": MODEL_ROW, "v2_forward": MODEL_ROW}


# ------------------------------------------------------------------ helpers
def now_ist():
    return dt.datetime.now(IST)


def read_csv(path, **kw):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kw)
    except Exception as e:
        print(f"❌ CSV read error for {path}: {e}")
        return pd.DataFrame()


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def num(x):
    try:
        v = float(str(x).replace("₹", "").replace(",", "").replace("%", "").strip())
        return None if v != v else v
    except Exception:
        return None


def price(x):
    v = num(x)
    return "" if v is None else f"{v:,.2f}"


def pct(x, digits=2):
    v = num(x)
    return "" if v is None else f"{v:+.{digits}f}%"


def minutes(t):
    try:
        h, m = str(t).split(":")[:2]
        return int(h) * 60 + int(m)
    except Exception:
        return None


def upto(df, cutoff, col="time"):
    """Rows at or before a HH:MM cut-off."""
    if df.empty or not cutoff:
        return df
    c = minutes(cutoff)
    return df[df[col].map(minutes).fillna(9999) <= c]


def records(df):
    if df is None or df.empty:
        return []
    return df.astype(object).where(pd.notna(df), "").to_dict("records")


def market_status(now):
    """(label, css) for the header pill — computed, not hardcoded."""
    if now.weekday() >= 5:
        return "WEEKEND", "closed"
    t = now.time()
    if t < dt.time(9, 15):
        return "PRE-OPEN", "closed"
    if t <= dt.time(15, 30):
        return "MARKET OPEN", "open"
    return "CLOSED", "closed"


def bot_heartbeat(now):
    """When the live bot last wrote a decision cycle today (None if not today)."""
    if not os.path.exists(DECISIONS_FILE):
        return None
    mtime = dt.datetime.fromtimestamp(os.path.getmtime(DECISIONS_FILE), IST)
    return mtime if mtime.date() == now.date() else None


def model_info():
    meta = read_json(MODEL_META) or {}
    thr = meta.get("threshold")
    return {"trained_through": meta.get("trained_through", "—"),
            "threshold": f"{thr:.3f}" if thr is not None else "—",
            "features": len(meta.get("features", [])) or "—"}


def config_frozen():
    """Is strategy_config.py still byte-for-byte the pre-registered file?"""
    try:
        want = re.findall(r"`([0-9a-f]{64})`", open(PREREG).read())[-1]   # latest registration
        with open(os.path.join(BASE_DIR, "strategy_config.py"), "rb") as f:
            have = hashlib.sha256(f.read()).hexdigest()
        return want == have, want
    except Exception:
        return None, None


# ------------------------------------------------------------------ live
SELECTIVE_NOTE = ("The model is selective by design: it takes only high-confidence breakouts. "
                  "Skipped signals are tracked to measure whether the filter is right.")


def tracker_today(day):
    """Today's rows of the bot's tracker.json by signal id. The bot rewrites it every
    cycle from the shadow positions it walks forward, GO and SKIP alike, so reading
    it costs no Dhan request."""
    snap = read_json(str(shadow.TRACKER_FILE)) or {}
    if snap.get("day") != day:
        return {}
    return {r["id"]: r for r in snap.get("rows", [])}


def signal_summary(rows):
    """The summary strip: counts by decision, and how today's SKIPs are doing (latest
    candle for open ones, the exit for closed ones). rows: dicts with decision, pnl_pct."""
    skip = [r for r in rows if r.get("decision") == "SKIP"]
    priced = [v for v in (r.get("pnl_pct") for r in skip)
              if isinstance(v, (int, float)) and v == v]           # not None, "", or NaN
    return {"signals": len(rows), "go": sum(r.get("decision") == "GO" for r in rows),
            "skip": len(skip), "stale": sum(r.get("decision") == "STALE" for r in rows),
            "skip_priced": len(priced), "skip_in_profit": sum(v > 0 for v in priced),
            "skip_avg_pnl": sum(priced) / len(priced) if priced else None}


# Status pill text (the full label is in the expanded row and the title).
STATUS_SHORT = {"open": "Open", "target": "Target", "stopped": "Stopped", "trailed": "Trailed out",
                "eod": "Closed EOD"}


LIVE_RULES = (exits.CURRENT_RULE,) + tuple(exits.SHADOW_RULES)


def chosen_rule():
    r = request.args.get("rule", exits.CURRENT_RULE)
    return r if r in LIVE_RULES else exits.CURRENT_RULE


def rule_options():
    return [(r, exits.RULE_LABELS.get(r, r)) for r in LIVE_RULES]


def live_rows(day, cutoff, show, rule=exits.CURRENT_RULE):
    """Today's decisions with their live state from the tracker (current price, P&L,
    trailing stop, best/worst, status) for GO and SKIP alike; the Telegram sent log's
    price is only a fallback when the tracker has no row."""
    d = read_csv(DECISIONS_FILE, dtype={"date": str})
    legacy = False
    if not d.empty:
        d = d[d["date"] == day]

    sent = read_csv(SENT_FILE)
    if not sent.empty:
        sent.columns = [c.lower() for c in sent.columns]
        sent = sent[sent["date"].astype(str).str[:10] == day]

    if d.empty:
        # No decision log for today (e.g. the older bot) — show the sent log.
        if sent.empty:
            return pd.DataFrame(), False
        d = sent.rename(columns={"orh": "ORH", "orl": "ORL"}).copy()
        d["decision"] = "GO"
        legacy = True
    elif not sent.empty and "current_price" in sent:
        cur = sent[["symbol", "direction", "current_price"]].drop_duplicates(["symbol", "direction"])
        d = d.merge(cur, on=["symbol", "direction"], how="left")

    d = upto(d, cutoff)
    if show == "go":
        d = d[d["decision"] == "GO"]
    if d.empty:
        return d, legacy

    track = tracker_today(day)
    names = company_names()
    rows = []
    for _, r in d.iterrows():
        direction = str(r["direction"]).upper()
        t = track.get(shadow.signal_id(r.get("date"), r["symbol"], r["direction"])) or {}
        if rule != exits.CURRENT_RULE and t:
            t = {**t, **(t.get("alt") or {}).get(rule, {})}      # the same signal under the what-if rule
        e = num(r.get("entry_price"))
        if t.get("price") is not None:
            c, diff = t["price"], t["pnl_pct"]              # signed in the trade's favour
        else:
            c = num(r.get("current_price"))
            diff = (((c - e) if direction == "BUY" else (e - c)) / e * 100) if e and c else None
        is_open = t.get("status") == "open"
        sc, th = num(r.get("score")), num(r.get("threshold"))
        stop_now = num(t.get("trail_stop"))
        initial = num(t.get("initial_stop")) if t.get("initial_stop") is not None else num(r.get("stop"))
        rows.append({
            "id": shadow.signal_id(r.get("date"), r["symbol"], r["direction"]),
            "company": names.get(str(r["symbol"]).upper(), ""),
            "entry_raw": e, "initial_raw": initial, "orh_raw": num(r.get("ORH")), "orl_raw": num(r.get("ORL")),
            "prev_close_raw": num(r.get("prev_close")),
            "time": str(r["time"]), "symbol": str(r["symbol"]).upper(), "direction": direction,
            "decision": str(r["decision"]),
            "entry": price(e), "current": price(c), "diff": pct(diff), "diff_raw": diff, "pnl_pct": diff,
            "trail": price(stop_now), "trail_raw": stop_now,
            "initial_stop": price(initial), "stop_moved": stop_now is not None and initial is not None
                                                          and abs(stop_now - initial) > 1e-9,
            "best": pct(t.get("best_pct")), "best_raw": num(t.get("best_pct")),
            "worst": pct(t.get("worst_pct")), "worst_raw": num(t.get("worst_pct")),
            "status": t.get("status", ""), "status_short": STATUS_SHORT.get(t.get("status", ""), ""),
            "status_label": t.get("status_label", ""), "exit_time": t.get("exit_time") or "",
            "score_raw": num(r.get("score")), "threshold": num(r.get("threshold")),
            "prev_close": price(r.get("prev_close")), "target": price(t.get("target")),
            "decided_at": str(r.get("decided_at") or "")[:8], "age_min": num(r.get("age_min")),
            "orh": price(r.get("ORH")), "orl": price(r.get("ORL")), "stop": price(r.get("stop")),
            "score": "" if sc is None else f"{sc:.2f}",
            "score_pct": 0 if sc is None else round(min(100, max(0, sc * 100)), 1),
            "thr_pct": 0 if th is None else round(min(100, max(0, th * 100)), 1),
        })
    out = pd.DataFrame(rows)
    out["go_first"] = (out["decision"] != "GO").astype(int)         # GO pinned to the top
    out = out.sort_values(["go_first", "time", "symbol"], ascending=[True, False, True]).drop(columns="go_first")
    return out, legacy


def ticker_chips(signals):
    """One entry per ticker that signalled, alphabetical; first signal time, directions, GO."""
    out = {}
    for s in sorted(signals, key=lambda s: (s["time"], s["symbol"])):
        t = out.setdefault(s["symbol"], {"symbol": s["symbol"], "time": s["time"][:5], "dirs": [], "go": False,
                                         "company": s.get("company", "")})
        if s["direction"] not in t["dirs"]:
            t["dirs"].append(s["direction"])
        t["go"] = t["go"] or s["decision"] == "GO"
    return sorted(out.values(), key=lambda t: t["symbol"])


_names = {"stamp": None, "map": {}}


def company_names():
    """Symbol -> company name from NSE's list files (current, then the previous list kept
    under data/reference/, for names that left the index but signalled before it updated)."""
    files = [os.path.join(BASE_DIR, "data", "ind_nifty200list.csv")]
    ref = os.path.join(BASE_DIR, "data", "reference")
    if os.path.isdir(ref):
        files += sorted((os.path.join(dp, f) for dp, _, fs in os.walk(ref) for f in fs if f == "ind_nifty200list.csv"),
                        reverse=True)
    stamp = tuple((f, os.path.getmtime(f)) for f in files if os.path.exists(f))
    if stamp != _names["stamp"]:
        m = {}
        for f, _ in reversed(stamp):                       # current list read last, so it wins
            df = read_csv(f)
            if {"Symbol", "Company Name"} <= set(df.columns):
                m.update(zip(df["Symbol"].astype(str).str.strip().str.upper(), df["Company Name"].astype(str)))
        _names.update(stamp=stamp, map=m)
    return _names["map"]


def range_scale(rows):
    """One % scale for every row's best/worst bar, so bars compare across rows."""
    vals = [abs(v) for r in rows for v in (r.get("best_raw"), r.get("worst_raw")) if isinstance(v, (int, float))]
    return max([1.0] + vals)


# ------------------------------------------------------------------ historical
def historical_rows(f):
    if f["source"] == "backtest":
        df = read_csv(WALKFORWARD_FILE, dtype={"date": str})
        if df.empty:
            return df, 0
        df["decision"] = df["go"].map(lambda g: "GO" if str(g) == "True" else "SKIP")
    else:
        df = read_csv(LIVE_LOG)
        if df.empty:
            return df, 0
        df.columns = [c.lower() for c in df.columns]
        df["date"] = df["date"].astype(str).str[:10]
        dec = read_csv(DECISIONS_FILE, dtype={"date": str})
        if not dec.empty:
            df = df.merge(dec[["date", "symbol", "direction", "score", "decision"]],
                          on=["date", "symbol", "direction"], how="left")
        if "decision" not in df:
            df["decision"] = "—"
        df["decision"] = df["decision"].fillna("—")

    if f["date"]:
        df = df[df["date"] == f["date"]]
    if f["direction"] in ("BUY", "SELL"):
        df = df[df["direction"].astype(str).str.upper() == f["direction"]]
    if f["symbol"]:
        df = df[df["symbol"].astype(str).str.contains(f["symbol"].upper().strip(), na=False, regex=False)]
    if f["decision"] == "GO":
        df = df[df["decision"] == "GO"]
    df = upto(df, f["time"])

    total = len(df)
    df = df.sort_values(["date", "time", "symbol"], ascending=[False, False, True]).head(MAX_ROWS)

    rows = []
    for _, r in df.iterrows():
        p = num(r.get("pnl_%"))
        sc = num(r.get("score"))
        rows.append({
            "date": r["date"], "time": r["time"], "symbol": r["symbol"],
            "direction": str(r["direction"]).upper(),
            "entry": price(r.get("entry_price")), "orh": price(r.get("orh")),
            "orl": price(r.get("orl")), "prev_close": price(r.get("prev_close")),
            "score": "" if sc is None else f"{sc:.2f}", "decision": r["decision"],
            "pnl": pct(p), "pnl_raw": p, "exit": r.get("exit_reason", "") or "",
        })
    return pd.DataFrame(rows), total


# ------------------------------------------------------------------ performance
def fmt_strategy(name, s):
    def f(key, spec):
        v = s.get(key)
        return "—" if v is None or (isinstance(v, float) and v != v) else spec.format(v)
    return {
        "name": name, "trades": f"{s.get('trades', 0):,}", "per_day": f("per_day", "{:.2f}"),
        "win": f("win_rate", "{:.1f}%"), "mean": pct(s.get("mean"), 3), "mean_raw": s.get("mean"),
        "ci": f"[{pct(s.get('ci_low'), 3)}, {pct(s.get('ci_high'), 3)}]" if s.get("ci_low") is not None else "—",
        "pf": f("profit_factor", "{:.2f}"), "sharpe": f("sharpe", "{:.2f}"),
        "dd": f("max_dd", "{:.1f}%"), "months": s.get("months_positive", "—"),
        "is_model": name == MODEL_ROW,
        "status": s.get("status", ""),
    }


def performance_view(period):
    summary = read_json(SUMMARY_FILE)
    if not summary or not summary.get("periods"):
        return None

    periods = summary["periods"]
    if period not in periods:
        period = "clean_backward" if "clean_backward" in periods else next(iter(periods))
    block = periods[period]
    headline_row = HEADLINE_FOR.get(period, MODEL_ROW)
    model = block["strategies"].get(headline_row, {})
    live = block["strategies"].get(MODEL_ROW, {})

    equity = monthly = ""
    wf_file = WALKFORWARD_FILE if headline_row == MODEL_ROW else os.path.join(RESEARCH, "walkforward_hit_1_5r.csv")
    wf = read_csv(wf_file, dtype={"date": str})
    if not wf.empty:
        wf = wf[(wf["date"] >= block["start"]) & (wf["date"] <= block["end"])]
        go = wf[wf["go"].astype(str) == "True"]
        days = sorted(wf["date"].unique())

        def curve(df, scale=1.0):
            s = df.groupby("date")["pnl_%"].sum().reindex(days, fill_value=0).cumsum() * scale
            return [(d[:7], float(v)) for d, v in s.items()]

        if days:
            equity = charts.line_chart([
                (headline_row + " — GO trades", charts.PALETTE["model"], curve(go)),
                ("All ORBITAL signals, scaled to the model's trade count",
                 charts.PALETTE["pool"], curve(wf, len(go) / max(len(wf), 1))),
            ])
        if not go.empty:
            m = go.groupby(go["date"].str[:7])["pnl_%"].sum()
            monthly = charts.bar_chart([(k, float(v)) for k, v in m.items()])

    paper = read_csv(PAPER_FILE)
    paper_go = paper[paper["decision"] == "GO"].copy() if not paper.empty else paper
    if not paper_go.empty:
        paper_go["pnl"] = paper_go["pnl_%"].map(pct)

    p_val = model.get("selection_p_value", block.get("selection_p_value"))
    dsr = model.get("deflated_sharpe")
    return {
        "period": period,
        "periods": [(k, PERIOD_LABELS.get(k, k)) for k in periods],
        "block": block,
        "strategies": [fmt_strategy(n, s) for n, s in block["strategies"].items()],
        "headline": fmt_strategy(headline_row, model) if model else None,
        "headline_name": headline_row,
        "headline_status": model.get("status", ""),
        "concentration": (f"Best month {model['best_month']} = {model['best_month_share']:.0f}% of P&L; "
                          f"without it {pct(model.get('mean_ex_best_month'), 3)} vs pool "
                          f"{pct(model.get('pool_ex_best_month'), 3)} (p = {model.get('p_ex_best_month', float('nan')):.3f})")
                         if model.get("p_ex_best_month") is not None else "",
        "p_value": f"{p_val:.3f}" if isinstance(p_val, (int, float)) and p_val == p_val else "—",
        "random_sel": pct(model.get("random_selection_mean", block.get("random_selection_mean")), 3),
        "dsr": f"{dsr:.2f}" if isinstance(dsr, (int, float)) and dsr == dsr else "—",
        "equity": equity, "monthly": monthly,
        "paper": records(paper_go),
        "paper_total": pct(paper_go["pnl_%"].sum()) if not paper_go.empty else "",
        "shap": [f for f in ("shap_global.png", "shap_beeswarm.png")
                 if os.path.exists(os.path.join(FIGURES, f))],
    }


# ------------------------------------------------------------------ rendering
def render(tab, **kw):
    now = now_ist()
    status, status_css = market_status(now)
    beat = bot_heartbeat(now)
    stale = status_css == "open" and now.time() >= dt.time(9, 45) and (
        beat is None or (now - beat) > dt.timedelta(minutes=12))
    return render_template(
        "dashboard.html", active_tab=tab, today=now.date().isoformat(),
        updated_at=now.strftime("%H:%M:%S"), status=status, status_css=status_css,
        heartbeat=beat.strftime("%H:%M") if beat else None, heartbeat_stale=stale,
        model=model_info(), time_filters=TIME_FILTERS, login_enabled=CHECKER is not None,
        guest_banner=GUEST_BANNER, **kw)


@app.route("/")
def index():
    return redirect(url_for("live"))


def _signals(rows):
    return [{k: (None if isinstance(v, float) and v != v else v) for k, v in r.items()}
            for r in (rows.to_dict("records") if not rows.empty else [])]       # NaN -> None


@app.route("/live")
def live():
    """All of today's signals in one table. ?fragment=1 returns only the parts the
    page swaps in when it refreshes itself (summary strip + table body)."""
    cutoff = request.args.get("time", "15:15")
    if cutoff not in TIME_FILTERS:
        cutoff = "15:15"
    day = now_ist().date().isoformat()
    rule = chosen_rule()

    rows, legacy = live_rows(day, cutoff, "all", rule)
    everything, _ = live_rows(day, None, "all", rule)
    signals = _signals(rows)
    kw = dict(selected_time=cutoff, legacy=legacy, summary=signal_summary(records(everything)),
              selective_note=SELECTIVE_NOTE, signals=signals, scale=range_scale(signals),
              rule=rule, rule_options=rule_options(), current_rule=exits.CURRENT_RULE,
              tickers=ticker_chips(signals))
    if request.args.get("fragment") == "1":
        return render_template("live_fragment.html", model=model_info(), **kw)
    return render("live", **kw)


# ------------------------------------------------------------------ tracker (live push)
# Same mechanism as QuantRadar's dashboard: Server-Sent Events. The bot rewrites
# data/live/tracker.json after every price cycle; each open stream checks it once
# a second and pushes the new snapshot (`event: tracker`, `id: <version>` so a
# reconnect doesn't resend an unchanged one), with keep-alive comments between.
# Streams end after STREAM_SECONDS (the browser reconnects in 3 s) and at most
# MAX_STREAMS run at once, so they can't take every server thread.
STREAM_SECONDS = 300
MAX_STREAMS = 4
_streams = threading.BoundedSemaphore(MAX_STREAMS)


def tracker_snapshot():
    snap = read_json(str(shadow.TRACKER_FILE)) or {}
    snap.setdefault("rows", [])
    snap["today"] = now_ist().date().isoformat()
    snap["summary"] = signal_summary(snap["rows"])
    return snap


@app.get("/api/tracker")
def api_tracker():
    return tracker_snapshot()


@app.get("/api/tracker/stream")
def api_tracker_stream():
    if not _streams.acquire(blocking=False):
        return jsonify(error="too many live connections"), 503, {"Retry-After": "10"}
    last = request.headers.get("Last-Event-ID")
    max_events = request.args.get("max_events", type=int)       # tests and debugging

    def events():
        nonlocal last
        sent, tick, started = 0, 0, time.monotonic()
        try:
            yield "retry: 3000\n\n"
            while time.monotonic() - started < STREAM_SECONDS:
                snap = tracker_snapshot()
                version = str(snap.get("version", ""))
                if version and version != last:
                    last = version
                    yield f"id: {version}\nevent: tracker\ndata: {json.dumps(snap, default=str)}\n\n"
                    sent += 1
                    if max_events and sent >= max_events:
                        return
                elif tick % 15 == 0:
                    yield ": keep-alive\n\n"
                tick += 1
                time.sleep(1)
        finally:
            _streams.release()

    return Response(stream_with_context(events()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


@app.route("/tracker")
def tracker():
    snap = tracker_snapshot()
    return render("tracker", snapshot=snap, selective_note=SELECTIVE_NOTE)


# ------------------------------------------------------------------ scorecard
def skip_report(df, signals):
    """The latest live session's SKIPs once their outcomes are labelled: how many made
    money, their average P&L, the best ones, and how the score related to the outcome
    across all of that session's labelled signals (GO and SKIP)."""
    if df.empty or "source" not in df:
        return None
    live = df[df["source"] == "live"]
    if live.empty:
        return None
    day = live["date"].max()
    today = live[live["date"] == day]
    skips = today[today["decision"] == "SKIP"]
    total = int(((signals["date"] == day) & (signals["source"] == "live")
                 & (signals["decision"] == "SKIP")).sum()) if not signals.empty else len(skips)
    out = {"date": day, "n": len(skips), "open": max(0, total - len(skips)),
           "wins": int((skips["pnl_%"] > 0).sum()),
           "avg": float(skips["pnl_%"].mean()) if len(skips) else None,
           "median": float(skips["pnl_%"].median()) if len(skips) else None,
           "best": records(skips.nlargest(5, "pnl_%")[["symbol", "time", "direction", "score", "pnl_%", "status"]]),
           "go_avg": float(today.loc[today["decision"] == "GO", "pnl_%"].mean())
                     if (today["decision"] == "GO").any() else None,
           "go_n": int((today["decision"] == "GO").sum()), "rho": None, "bands": []}
    if len(today) >= 4 and today["score"].nunique() > 1:
        out["rho"] = float(today["score"].rank().corr(today["pnl_%"].rank()))     # Spearman
        q = pd.qcut(today["score"], 4, duplicates="drop")
        for band, g in today.groupby(q, observed=True):
            out["bands"].append({"lo": float(band.left), "hi": float(band.right), "n": len(g),
                                 "hit": float((g["pnl_%"] > 0).mean() * 100), "avg": float(g["pnl_%"].mean())})
    return out


def exit_rule_comparison(rule=None):
    """Live signals labelled under both the current exit rule and a side-by-side rule:
    per-trade P&L under each, for all signals and for GO. The evidence for (or against)
    ever switching the live rule; it decides nothing by itself."""
    rule = rule or (exits.SHADOW_RULES[0] if exits.SHADOW_RULES else None)
    if not rule:
        return None
    s = shadow._read(shadow.SIGNALS_FILE)
    o = shadow._read(shadow.OUTCOMES_FILE)
    a = shadow._read(shadow.alt_outcomes_file(shadow.OUTCOMES_FILE, rule))
    if s.empty or o.empty or a.empty:
        return {"rule": rule, "label": exits.RULE_LABELS.get(rule, rule), "n": 0}
    s = s[s["source"] == "live"].drop_duplicates("signal_id")[["signal_id", "date", "decision"]]
    df = s.merge(o[["signal_id", "pnl_pct"]].drop_duplicates("signal_id"), on="signal_id") \
          .merge(a[["signal_id", "pnl_pct"]].drop_duplicates("signal_id"), on="signal_id", suffixes=("_cur", "_alt"))
    for c in ("pnl_pct_cur", "pnl_pct_alt"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["pnl_pct_cur", "pnl_pct_alt"])

    def side(x):
        d = x["pnl_pct_alt"] - x["pnl_pct_cur"]
        return {"n": len(x), "cur": float(x["pnl_pct_cur"].mean()) if len(x) else None,
                "alt": float(x["pnl_pct_alt"].mean()) if len(x) else None,
                "changed": int((d.abs() > 1e-9).sum()), "better": int((d > 1e-9).sum()),
                "worse": int((d < -1e-9).sum()), "too_few": len(x) < shadow.MIN_SAMPLE}
    return {"rule": rule, "label": exits.RULE_LABELS.get(rule, rule), "n": len(df),
            "sessions": int(df["date"].nunique()), "first": df["date"].min() if len(df) else None,
            "all": side(df), "go": side(df[df["decision"] == "GO"])}


VARIANT_RULE = {"min_trades": 50, "min_days": 15, "cost": 0.05}    # docs/THRESHOLD_PROTOCOL.md, step 7


def _day_boot(days, pnl, reps=4000, seed=5, other=None):
    """95% day-block bootstrap CI of mean P&L per trade (or of the difference of two
    per-trade means when `other` = (days, pnl) of the comparison set)."""
    rng = np.random.default_rng(seed)
    uniq = sorted(set(days) | (set(other[0]) if other is not None else set()))
    if len(uniq) < 2:
        return None, None
    ix = {d: i for i, d in enumerate(uniq)}

    def sums(ds, ps):
        s, n = np.zeros(len(uniq)), np.zeros(len(uniq))
        np.add.at(s, [ix[d] for d in ds], ps)
        np.add.at(n, [ix[d] for d in ds], 1)
        return s, n
    s1, n1 = sums(days, pnl)
    s2, n2 = sums(*other) if other is not None else (None, None)
    out = []
    for _ in range(reps):
        k = rng.integers(0, len(uniq), len(uniq))
        m = s1[k].sum() / max(n1[k].sum(), 1)
        if other is not None:
            m -= s2[k].sum() / max(n2[k].sum(), 1)
        out.append(m)
    return float(np.percentile(out, 2.5)), float(np.percentile(out, 97.5))


def variant_scorecard():
    """The champion vs the shadow variants on the same live sessions: only `source = live`
    signals labelled under the current exit rule, from the first variant-logged day."""
    vf = shadow.LIVE_DIR / "shadow_variants.csv"
    if not vf.exists():
        return None
    v = shadow._read(vf)
    s, o = shadow._read(shadow.SIGNALS_FILE), shadow._read(shadow.OUTCOMES_FILE)
    if v.empty or s.empty:
        return None
    v = v[v["source"] == "live"]
    start = v["date"].min() if not v.empty else None
    s = s[(s["source"] == "live") & (s["date"] >= start)].drop_duplicates("signal_id")
    out = o.drop_duplicates("signal_id")[["signal_id", "pnl_pct"]] if not o.empty else pd.DataFrame(columns=["signal_id", "pnl_pct"])
    sets = {"champion": s.loc[s["model_go"].astype(str).isin(["True", "true", "1"]), ["signal_id", "date"]]}
    labels = {"champion": f"Champion ({s['model_version'].iloc[0] if len(s) else 'v2'} @ "
                          f"{float(s['threshold'].iloc[0]) if len(s) else 0.644:.3f})"}
    for name, g in v.groupby("variant"):
        sets[name] = g.loc[g["go"].astype(str).isin(["True", "true", "1"]), ["signal_id", "date"]]
        labels[name] = f"{name} @ {float(g['threshold'].iloc[0]):.3f}"
    trades = {k: x.merge(out, on="signal_id").assign(pnl=lambda d: pd.to_numeric(d["pnl_pct"], errors="coerce")).dropna(subset=["pnl"])
              for k, x in sets.items()}
    champ = trades["champion"]
    rows = []
    for k, t in trades.items():
        n, days = len(t), t["date"].nunique()
        mean = float(t["pnl"].mean()) if n else None
        lo, hi = _day_boot(t["date"].tolist(), t["pnl"].to_numpy()) if n else (None, None)
        row = {"key": k, "label": labels[k], "n": n, "days": days,
               "hit": float((t["pnl"] > 0).mean() * 100) if n else None,
               "mean": {c: (mean - c if mean is not None else None) for c in COST_LEVELS},
               "ci05": (lo - 0.05, hi - 0.05) if lo is not None else (None, None),
               "worst_day": float(t.groupby("date")["pnl"].sum().min()) if n else None}
        if k != "champion" and n and len(champ):
            d = mean - float(champ["pnl"].mean())
            dlo, dhi = _day_boot(t["date"].tolist(), t["pnl"].to_numpy(),
                                 other=(champ["date"].tolist(), champ["pnl"].to_numpy()))
            row.update(diff=d, diff_ci=(dlo, dhi),
                       rule={"enough": n >= VARIANT_RULE["min_trades"] and days >= VARIANT_RULE["min_days"],
                             "beats": d > 0, "ci_excludes_zero": dlo is not None and (dlo > 0 or dhi < 0)})
        rows.append(row)
    return {"start": start, "rows": rows, "sessions": int(s["date"].nunique()), "rule": VARIANT_RULE}


COST_LEVELS = (0.0, 0.05, 0.10)


def scorecard_view(by):
    df = shadow.labelled_rows()
    signals = shadow._read(shadow.SIGNALS_FILE)
    open_n = 0
    if not signals.empty:
        labelled = set(df["signal_id"]) if not df.empty else set()
        open_n = int((~signals["signal_id"].isin(labelled)).sum())
    card = shadow.scorecard(df, by=by)
    try:
        champ = registry.champion()
        versions = {"champion": champ.version, "champion_thr": champ.threshold,
                    "champion_trained": champ.trained_through}
    except Exception as e:                       # a broken registry must not hide the scorecard
        versions = {"champion": f"error: {e}", "champion_thr": None, "champion_trained": None}
    versions["baseline"] = registry.BASELINE_VERSION
    scored_by = (df["model_version"].value_counts().to_dict() if not df.empty else {})
    return {"by": by, "card": card, "open": open_n, "labelled": len(df), "versions": versions,
            "scored_by": scored_by, "min_sample": shadow.MIN_SAMPLE, "costs": C.COST_SENSITIVITY,
            "comparisons": list(reversed(registry.comparisons()))[:12],
            "drift": read_json(str(shadow.LIVE_DIR / "drift.json")),
            "skips": skip_report(df, signals), "exit_compare": exit_rule_comparison(),
            "variants": variant_scorecard()}


@app.route("/scorecard")
def scorecard():
    by = "baseline_go" if request.args.get("filter") == "baseline" else "model_go"
    return render("scorecard", sc=scorecard_view(by))


@app.route("/historical")
def historical():
    f = {k: request.args.get(k, d) for k, d in
         [("source", "live"), ("date", ""), ("direction", "ALL"), ("symbol", ""),
          ("time", "15:15"), ("decision", "ALL")]}
    rows, total = historical_rows(f)
    return render("historical", filters=f, trades=records(rows), total=total,
                  max_rows=MAX_ROWS, has_backtest=os.path.exists(WALKFORWARD_FILE))


@app.route("/performance")
def performance():
    return render("performance", view=performance_view(request.args.get("period", "clean_backward")))


@app.route("/about")
def about():
    frozen, sha = config_frozen()
    cfg = [
        ("Universe", "Nifty 200, point-in-time membership (survivorship-bias corrected)"),
        ("Opening range", f"{C.OR_START:%H:%M}–{C.OR_END:%H:%M}, all {C.OR_REQUIRED_CANDLES} candles required"),
        ("Breakout", f"close beyond ORH / ORL by {C.BREAKOUT_BUFFER:.1%}"),
        ("Move filter", f"±{C.PREV_CLOSE_MOVE:.1%} from the previous close"),
        ("Pivot filter", f"beyond Fibonacci R1 / S1 ({C.PIVOT_FIB})"),
        ("Entry", f"close of a completed 5-min candle; last entry {C.LAST_ENTRY_TIME:%H:%M}"),
        ("Exit", f"stop {C.STOP_ORB_MULT}× and trail {C.TRAIL_ORB_MULT}× the opening-range width, "
                 f"{'no fixed target' if C.TARGET_ORB_MULT is None else f'target {C.TARGET_ORB_MULT}×'}, "
                 f"flat by {C.FORCE_EXIT_TIME:%H:%M}"),
        ("Model", f"XGBoost {C.MODEL_VERSION}, 28 inputs, predicts '{C.LABEL}' — "
                  f"whether the trade makes money under the exit rule below"),
        ("GO threshold", f"{C.THRESHOLD_QUANTILE:.0%} quantile of out-of-sample calibration scores"),
        ("Retraining", f"{C.RETRAIN}, walk-forward"),
    ]
    return render("about", cfg=cfg, frozen=frozen, sha=sha)


# ------------------------------------------------------------------ guest (read-only demo)
# /guest?k=<key>: the live scanner, tracker, scorecard and performance tabs, read-only, with
# every price replaced by a % move. The key lives only on the VM (GUEST_KEY_FILE, written by
# `python guest.py rotate`) and is re-read when the file changes, so rotating needs no
# restart; no key file = no guest view. Guests get no session, so every other page and the
# logout/login endpoints stay exactly as they were. Rows are built from a whitelist of %
# fields: a template slip can show "—", never a price.
GUEST_KEY_FILE = Path(os.getenv("ORBITAL_GUEST_KEY_FILE", "~/.orbital_guest_key")).expanduser()
GUEST_ENDPOINTS = {"guest_live", "guest_tracker", "guest_scorecard", "guest_performance", "guest_api_tracker"}
GUEST_BANNER = "Read-only demo. Signals only, no orders. Prices shown as %."
_guest_key_cache = {"stat": None, "key": None}


class RateWindow:
    """At most `n` hits per `per` seconds for each client (sliding window)."""

    def __init__(self, n, per, clock=time.monotonic):
        self.n, self.per, self.clock = n, per, clock
        self.hits, self.lock = {}, threading.Lock()

    def allow(self, who):
        now = self.clock()
        with self.lock:
            q = self.hits.setdefault(who, deque())
            while q and now - q[0] >= self.per:
                q.popleft()
            if len(q) >= self.n:
                return False, self.per - (now - q[0])
            q.append(now)
            if len(self.hits) > 5000:                        # forget idle clients
                for k in [k for k, v in self.hits.items() if not v]:
                    del self.hits[k]
            return True, 0.0


GUEST_RATE = RateWindow(120, 60)          # page + 30 s refreshes, with room for clicking around
GUEST_BAD_KEY = RateWindow(20, 600)       # wrong keys: guessing is pointless (32 random bytes) but capped


def guest_key():
    try:
        st = GUEST_KEY_FILE.stat()
    except OSError:
        return None
    stamp = (st.st_mtime_ns, st.st_size)
    if _guest_key_cache["stat"] != stamp:
        _guest_key_cache.update(stat=stamp, key=GUEST_KEY_FILE.read_text().strip() or None)
    return _guest_key_cache["key"]


def guest_gate():
    who = request.remote_addr or "?"          # the real client: ProxyFix trusts Caddy's X-Forwarded-For
    ok, wait = GUEST_RATE.allow(who)
    if not ok:
        return Response("Too many requests.\n", 429, {"Retry-After": str(int(wait) + 1)}, mimetype="text/plain")
    key, given = guest_key(), request.args.get("k", "")
    if key and given and hmac.compare_digest(given.encode(), key.encode()):
        g.guest, g.guest_key = True, key
        return None
    ok, wait = GUEST_BAD_KEY.allow(who)
    if not ok:
        return Response("Too many requests.\n", 429, {"Retry-After": str(int(wait) + 1)}, mimetype="text/plain")
    abort(404)                                # don't confirm the page exists


@app.template_global()
def tab_url(tab, **kw):
    """A tab's URL: the guest copy (with the key) for guests, the normal page otherwise."""
    if getattr(g, "guest", False):
        return url_for("guest_" + tab, k=g.guest_key, **kw)
    return url_for(tab, **kw)


def _pct(a, b, sign=1.0):
    return None if a is None or b in (None, 0) else sign * (a - b) / b * 100


def guest_signal(s):
    """One live row with every price replaced by a % move (whitelist)."""
    sign = 1.0 if s["direction"] == "BUY" else -1.0
    e, orh, orl = s.get("entry_raw"), s.get("orh_raw"), s.get("orl_raw")
    level = orh if sign > 0 else orl
    keep = ("id", "time", "symbol", "company", "direction", "decision", "score", "score_raw", "threshold", "score_pct",
            "thr_pct", "diff", "diff_raw", "best", "best_raw", "worst", "worst_raw", "status", "status_short",
            "status_label", "exit_time", "decided_at", "age_min", "stop_moved")
    out = {k: s.get(k) for k in keep}
    out.update(
        stop_pct=_pct(s.get("trail_raw"), e, sign),            # stop now / at exit vs entry, in the trade's favour
        initial_stop_pct=_pct(s.get("initial_raw"), e, sign),
        range_pct=_pct(orh, orl),                                # opening-range width
        breakout_pct=_pct(e, level, sign),                       # entry beyond the range edge
        prev_close_pct=_pct(e, s.get("prev_close_raw")),         # entry vs yesterday's close
    )
    return out


GUEST_TRACKER_FIELDS = ("id", "symbol", "time", "direction", "score", "threshold", "go", "decision",
                        "baseline_go", "status", "status_label", "pnl_pct", "best_pct", "worst_pct",
                        "exit_time", "model_version")


def guest_tracker_snapshot():
    snap = tracker_snapshot()
    rows = []
    for r in snap["rows"]:
        sign = 1.0 if r.get("direction") == "BUY" else -1.0
        alt = {rule: {**{k: a.get(k) for k in ("status", "status_label", "pnl_pct", "best_pct", "worst_pct",
                                                 "exit_time")},
                      "stop_pct": _pct(a.get("trail_stop"), r.get("entry"), sign)}
               for rule, a in (r.get("alt") or {}).items()}
        rows.append({**{k: r.get(k) for k in GUEST_TRACKER_FIELDS},
                     "stop_pct": _pct(r.get("trail_stop"), r.get("entry"), sign),
                     "initial_stop_pct": _pct(r.get("initial_stop", r.get("stop")), r.get("entry"), sign),
                     "alt": alt})
    return {"version": snap.get("version"), "day": snap.get("day"), "as_of": snap.get("as_of"),
            "today": snap["today"], "summary": snap["summary"], "rows": rows, "guest": True}


@app.get("/guest")
def guest_live():
    cutoff = request.args.get("time", "15:15")
    if cutoff not in TIME_FILTERS:
        cutoff = "15:15"
    day = now_ist().date().isoformat()
    rule = chosen_rule()
    rows, _ = live_rows(day, cutoff, "all", rule)
    everything, _ = live_rows(day, None, "all", rule)
    signals = [guest_signal(s) for s in _signals(rows)]
    kw = dict(selected_time=cutoff, legacy=False, summary=signal_summary(records(everything)),
              selective_note=SELECTIVE_NOTE, signals=signals, scale=range_scale(signals),
              rule=rule, rule_options=rule_options(), current_rule=exits.CURRENT_RULE,
              tickers=ticker_chips(signals))
    if request.args.get("fragment") == "1":
        return render_template("live_fragment.html", model=model_info(), **kw)
    return render("live", **kw)


@app.get("/guest/tracker")
def guest_tracker():
    return render("tracker", snapshot=guest_tracker_snapshot(), selective_note=SELECTIVE_NOTE)


@app.get("/guest/api/tracker")
def guest_api_tracker():
    return guest_tracker_snapshot()


@app.get("/guest/scorecard")
def guest_scorecard():
    by = "baseline_go" if request.args.get("filter") == "baseline" else "model_go"
    return render("scorecard", sc=scorecard_view(by))


@app.get("/guest/performance")
def guest_performance():
    view = performance_view(request.args.get("period", "clean_backward"))
    if view:
        view["paper"] = [{k: v for k, v in p.items() if k in ("date", "time", "symbol", "direction", "score",
                                                              "pnl_%", "pnl", "exit_reason")}
                         for p in view.get("paper") or []]
    return render("performance", view=view)


@app.get("/robots.txt")
def robots():
    return Response("User-agent: *\nDisallow: /\n", mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=os.getenv("ORBITAL_DEBUG") == "1", port=int(os.getenv("ORBITAL_PORT", "5050")))
