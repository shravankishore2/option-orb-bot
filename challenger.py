"""
challenger.py — monthly champion/challenger model update (no daily retraining).

Run on the first weekend of each month (deploy/orbital-challenger.timer):

  1. Data: every labelled signal to date — the research history
     (data/training/history.csv.gz, exported from the walk-forward dataset)
     plus the live shadow signals whose outcome is complete (shadow.py).
  2. Leakage checks: inputs are exactly the model's features, none of them a
     label; every shadow label was written after its exit; no duplicate signals;
     the challenger never sees the held-out month.
  3. Held-out window: start with the last complete calendar month and pool
     earlier months, newest first, until champion AND challenger each have at
     least MIN_GO_TRADES (30) GO trades on the same window — a single month
     has only ~15-20. Only sessions the champion never trained on count, and
     the challenger is retrained each time the window grows, on everything
     BEFORE the window (walk_forward.py's recipe), so the whole window is
     excluded from both models' training. The pooled months are logged.
  4. Compared on GO-trade P&L per trade at every cost level in
     strategy_config.COST_SENSITIVITY (the RESULTS.md sensitivity); promote
     only if the challenger is better at every level. The challenger's
     date-grouped 5-fold CV AUC is logged.
  5. Logged to models/registry/comparisons.jsonl either way. On promotion the
     same recipe is refit on all labelled data through the held-out month and
     becomes the champion; the evaluated challenger is kept too. The frozen v2
     baseline is never replaced.

    python challenger.py                 # the monthly run (skips a month already decided)
    python challenger.py --dry-run       # evaluate and print; change nothing
    python challenger.py --export-history   # (research machine) write data/training/history.csv.gz
"""

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import registry
import shadow
import strategy_config as C
from features import LABELS

BASE_DIR = Path(__file__).resolve().parent
HISTORY = BASE_DIR / "data" / "training" / "history.csv.gz"
FEATURES = shadow.FEATURE_COLUMNS
KEYS = ["date", "symbol", "direction"]
MIN_GO_TRADES = shadow.MIN_SAMPLE          # 30: fewer GO trades than this decide nothing
MAX_POOLED_MONTHS = 12                     # pool at most a year of held-out months
OUTCOME_COLUMNS = set(LABELS) | {"pnl_%", "profit", "exit_reason", "exit_time", "exit_price",
                                 "status", "labelled_at", "mfe_pct", "mae_pct"}
CV_FOLDS = 5


# ---------------------------------------------------------------- data
def export_history(out=HISTORY):
    """Compact copy of the walk-forward training set (features + outcome only)."""
    import walk_forward as W
    df, feats = W.load_dataset()
    assert feats == FEATURES, "walk-forward features differ from the live feature vector"
    cols = KEYS + ["time", "entry_price", "orh", "orl", "prev_close", "exit_reason", "pnl_%", "profit"] + FEATURES
    out.parent.mkdir(parents=True, exist_ok=True)
    df[cols].to_csv(out, index=False, compression="gzip")    # full precision: rounding changes the fit
    print(f"💾 {out}: {len(df):,} signals, {df['date'].min()} → {df['date'].max()}")


def load_history(path=HISTORY):
    if not Path(path).exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype={"date": str, "time": str})
    df["source"] = "history"
    return df


def labelled_data(history=None, shadow_rows=None):
    """History + completed shadow signals, checked for leakage."""
    h = load_history() if history is None else history
    s = shadow.labelled_rows() if shadow_rows is None else shadow_rows
    if not s.empty:
        shadow.check_labels_are_complete(s)
        s = s.copy()
        s["source"] = "shadow:" + s["source"].astype(str)
    cols = KEYS + ["time", "entry_price", "orh", "orl", "pnl_%", "profit", "source"] + FEATURES
    parts = [p.reindex(columns=cols) for p in (h, s) if not p.empty]
    if not parts:
        return pd.DataFrame(columns=cols)
    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(KEYS, keep="first")          # a history row wins over a shadow copy
    # float32: XGBoost works in float32 anyway (identical results), half the memory
    df[FEATURES] = df[FEATURES].apply(pd.to_numeric, errors="coerce").astype("float32")
    df["pnl_%"] = pd.to_numeric(df["pnl_%"], errors="coerce")
    df = df.dropna(subset=["pnl_%"])
    df["profit"] = (df["pnl_%"] > 0).astype(int)
    return df.sort_values(["date", "time", "symbol"]).reset_index(drop=True)


def check_leakage(train, features, held_out_start):
    """Raise if training could see a label, the held-out month, or a duplicate."""
    leaked = set(features) & OUTCOME_COLUMNS
    if leaked:
        raise AssertionError(f"label columns used as model inputs: {sorted(leaked)}")
    if list(features) != FEATURES:
        raise AssertionError("challenger inputs differ from the live feature vector")
    if (train["date"] >= held_out_start).any():
        raise AssertionError("training data reaches into the held-out month")
    if train.duplicated(KEYS).any():
        raise AssertionError("duplicate signals in training data")


# ---------------------------------------------------------------- evaluation
def grouped_cv_auc(train, features, folds=CV_FOLDS):
    """Date-grouped K-fold AUC (no session is split across folds) — a diagnostic."""
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import GroupKFold
    from xgboost import XGBClassifier

    X, y, groups = train[features].fillna(0), train[C.LABEL], train["date"]
    aucs = []
    for fit_idx, val_idx in GroupKFold(n_splits=folds).split(X, y, groups):
        params = dict(C.MODEL_PARAMS)
        yf = y.iloc[fit_idx]
        params["scale_pos_weight"] = (yf == 0).sum() / max((yf == 1).sum(), 1)
        m = XGBClassifier(**params).fit(X.iloc[fit_idx], yf)
        yv = y.iloc[val_idx]
        if yv.nunique() == 2:
            aucs.append(float(roc_auc_score(yv, m.predict_proba(X.iloc[val_idx])[:, 1])))
    return {"folds": folds, "auc_mean": float(np.mean(aucs)) if aucs else None,
            "auc_std": float(np.std(aucs)) if aucs else None}


def go_metrics(window, scores, threshold, costs=C.COST_SENSITIVITY):
    go = window[np.asarray(scores) >= threshold]
    n = len(go)
    mean = float(go["pnl_%"].mean()) if n else None
    return {"go_trades": n, "signals": len(window), "threshold": round(float(threshold), 4),
            "hit_rate": float((go["pnl_%"] > 0).mean() * 100) if n else None,
            "pnl_per_trade": mean,
            "net_per_trade": {str(c): (mean - c if n else None) for c in costs}}


def score(model, window):
    return model.model.predict_proba(window[model.features].fillna(0))[:, 1]


def decide(champ_m, chall_m, costs=C.COST_SENSITIVITY, min_trades=MIN_GO_TRADES):
    """(promote?, reason) — challenger must beat the champion at every cost level."""
    if champ_m["go_trades"] < min_trades or chall_m["go_trades"] < min_trades:
        return False, (f"too few GO trades to judge (champion {champ_m['go_trades']}, "
                       f"challenger {chall_m['go_trades']}; need {min_trades} each)")
    worse = [c for c in costs
             if chall_m["net_per_trade"][str(c)] <= champ_m["net_per_trade"][str(c)]]
    if worse:
        return False, f"challenger not better after {', '.join(f'{c:.2f}%' for c in worse)} round-trip costs"
    return True, "challenger beats the champion per GO trade at every cost level"


def held_out_month(today):
    first = today.replace(day=1)
    last = first - dt.timedelta(days=1)
    return last.replace(day=1), last


class _Fresh:
    """A just-trained model, duck-typed like registry.Model."""

    def __init__(self, model, threshold):
        self.model, self.features, self.threshold = model, FEATURES, threshold


def month_starts_back(last_start, n):
    """[last_start, the month before, ...] — n month starts, newest first."""
    out, m = [], last_start
    for _ in range(n):
        out.append(m)
        m = (m - dt.timedelta(days=1)).replace(day=1)
    return out


def pooled_comparison(df, champ, last_start, last_end, min_trades=MIN_GO_TRADES, max_months=MAX_POOLED_MONTHS):
    """Pool held-out months, newest first, until champion AND challenger each have
    `min_trades` GO trades on the same window.

    The window is every session from the earliest pooled month to `last_end`
    that is out-of-sample for the champion (after its trained_through). The
    challenger is retrained each time the window grows, on data strictly before
    the window — so neither model has seen any of it. Pooling stops at the
    champion's training boundary. Returns (model, threshold, info, window,
    train, attempts); model is None if no window had enough GO trades.
    """
    import walk_forward as W
    oos_after = str(champ.trained_through or "")
    e = last_end.isoformat()
    attempts, best = [], None
    for first in month_starts_back(last_start, max_months):
        last_day = (first + dt.timedelta(days=32)).replace(day=1) - dt.timedelta(days=1)
        if oos_after and last_day.isoformat() <= oos_after:
            break                                              # the champion trained on this whole month
        s = first.isoformat()
        window = df[(df["date"] >= s) & (df["date"] <= e) & (df["date"] > oos_after)]
        months = sorted(window["date"].str[:7].unique())
        n_champ = int((score(champ, window) >= champ.threshold).sum()) if not window.empty else 0
        attempt = {"from": f"{first:%Y-%m}", "months": months, "sessions": int(window["date"].nunique()),
                   "champion_go": n_champ, "challenger_go": None}
        attempts.append(attempt)
        if window.empty or n_champ < min_trades:               # no need to train yet
            continue
        start = window["date"].min()
        train = df[df["date"] < start]
        if len(train) < C.MIN_TRAIN_SIGNALS:
            attempt["stopped"] = f"only {len(train):,} training signals before {start} (need {C.MIN_TRAIN_SIGNALS:,})"
            break
        check_leakage(train, FEATURES, start)
        model, thr, info = W.fit_with_threshold(train, FEATURES)
        n_chall = int((score(_Fresh(model, thr), window) >= thr).sum())
        attempt["challenger_go"] = n_chall
        best = (model, thr, info, window, train)
        if n_chall >= min_trades:
            return model, thr, info, window, train, attempts
    if best is None:
        return None, None, None, None, None, attempts
    model, thr, info, window, train = best                    # largest window tried: decide() reports "too few"
    return model, thr, info, window, train, attempts


def run(today=None, dry_run=False, force=False, data=None, cv=True):
    today = today or dt.date.today()
    start, end = held_out_month(today)
    month = f"{start:%Y-%m}"
    version = f"r{today:%Y-%m}"

    if not force and not dry_run and any(c.get("held_out_month") == month for c in registry.comparisons()):
        print(f"⏭️  {month} was already decided — see {registry.COMPARISONS}")
        return None

    champ, base = registry.champion(), registry.baseline()
    df = labelled_data() if data is None else data
    print(f"📦 {len(df):,} labelled signals ({(df['source'] == 'history').sum():,} history, "
          f"{df['source'].str.startswith('shadow').sum():,} shadow), {df['date'].min()} → {df['date'].max()}")

    model, thr, info, window, train, attempts = pooled_comparison(df, champ, start, end)
    record = {"run_at": dt.datetime.now().isoformat(timespec="seconds"), "held_out_month": month,
              "champion": champ.version, "baseline": base.version, "pooling": attempts,
              "pooled_months": sorted(window["date"].str[:7].unique()) if window is not None else [],
              "window": [window["date"].min(), window["date"].max()] if window is not None else None,
              "window_sessions": int(window["date"].nunique()) if window is not None else 0,
              "train_rows": len(train) if train is not None else 0,
              "train_through": train["date"].max() if train is not None and len(train) else None,
              "dry_run": dry_run}

    if model is None:
        best = max(attempts, key=lambda a: a["champion_go"], default=None)
        stopped = next((a["stopped"] for a in attempts if a.get("stopped")), None)
        record.update(promoted=False, would_promote=False, reason=(
            "no held-out sessions out-of-sample for the champion" if not best or not best["sessions"] else
            f"not enough training data: {stopped}" if stopped else
            f"too few GO trades to judge even pooling {len(best['months'])} month(s) "
            f"{', '.join(best['months'])} (champion {best['champion_go']}; need {MIN_GO_TRADES})"))
        return _finish(record, dry_run)

    import walk_forward as W
    record["cv"] = grouped_cv_auc(train, FEATURES) if cv else None
    chall = _Fresh(model, thr)
    m_champ = go_metrics(window, score(champ, window), champ.threshold)
    m_chall = go_metrics(window, score(chall, window), thr)
    m_base = go_metrics(window, score(base, window), base.threshold)
    promote, reason = decide(m_champ, m_chall)
    reason += f" (pooled {', '.join(record['pooled_months'])})"
    record.update(champion_metrics=m_champ, challenger_metrics=m_chall, baseline_metrics=m_base,
                  promoted=promote and not dry_run, would_promote=promote, reason=reason)

    if not dry_run:
        evaluated = registry.save_model(f"c{today:%Y-%m}", model, FEATURES, thr, {
            "role": "challenger (held-out evaluation)", "trained_through": record["train_through"],
            "trained_from": train["date"].min(), "rows": len(train), **info,
            "pooled_months": record["pooled_months"],
            "label": C.LABEL, "config_sha256": registry.sha256(BASE_DIR / "strategy_config.py")})
        record["challenger"] = evaluated.version
        if promote:
            full = df[df["date"] <= end.isoformat()]
            check_leakage(full, FEATURES, (end + dt.timedelta(days=1)).isoformat())
            m2, thr2, info2 = W.fit_with_threshold(full, FEATURES)
            live = registry.save_model(version, m2, FEATURES, thr2, {
                "role": "champion (refit through the held-out window)",
                "trained_through": full["date"].max(), "trained_from": full["date"].min(),
                "rows": len(full), **info2, "label": C.LABEL, "evaluated_as": evaluated.version,
                "config_sha256": registry.sha256(BASE_DIR / "strategy_config.py")})
            registry.promote(live, reason)
            record["new_champion"] = live.version
    return _finish(record, dry_run)


def _finish(record, dry_run):
    if not dry_run:
        registry.log_comparison(record)
    def fmt(m):
        if not m:
            return "—"
        p = m["pnl_per_trade"]
        return (f"{m['go_trades']} GO, " + (f"{p:+.3f}%/trade gross, {m['net_per_trade']['0.05']:+.3f}% after 0.05%"
                                             if p is not None else "no trades"))
    print(f"🏁 {record['held_out_month']}: champion {record['champion']} [{fmt(record.get('champion_metrics'))}] "
          f"vs challenger [{fmt(record.get('challenger_metrics'))}] → "
          f"{'PROMOTED ' + record.get('new_champion', '') if record.get('promoted') else 'WOULD PROMOTE' if record.get('would_promote') else 'kept champion'}: "
          f"{record['reason']}" + ("  (dry run — nothing changed)" if dry_run else ""))
    return record


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="re-run a month already decided")
    ap.add_argument("--export-history", action="store_true")
    ap.add_argument("--today", default=None, help="pretend the run date is YYYY-MM-DD")
    a = ap.parse_args()
    if a.export_history:
        export_history()
        sys.exit(0)
    run(dt.date.fromisoformat(a.today) if a.today else None, dry_run=a.dry_run, force=a.force)
