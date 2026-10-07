"""
variant_status.py — the champion vs the shadow variants on live signals, and the
weekly status line (docs/THRESHOLD_PROTOCOL.md, steps 6-7; docs/V2_1_PROTOCOL.md).

scorecard() is what the dashboard's Scorecard shows. Run as a script (Fridays after
the close, orbital-variant-status.timer) it appends one dated status line per run to
data/live/variant_status.json and rewrites the <!-- variant-status:start/end --> block
of docs/THRESHOLD_RESULTS.md from that history. It decides nothing: the champion
changes only through the protocol's rule and the owner's approval.

    python variant_status.py [--date YYYY-MM-DD]
"""

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

import shadow

RULE = {"min_trades": 50, "min_days": 15, "cost": 0.05}       # docs/THRESHOLD_PROTOCOL.md, step 7
COST_LEVELS = (0.0, 0.05, 0.10)
# Step 7, condition 4 (threshold variant only): the step-5 sweep at 0.05% cost. Fixed by
# docs/THRESHOLD_RESULTS.md (2026-10-07): 0.54 beats 0.644 on combined total, with and
# without the best month.
SWEEP_AGREES = {"v2@0.54": True}
DOC = Path(__file__).resolve().parent / "docs" / "THRESHOLD_RESULTS.md"
START, END = "<!-- variant-status:start -->", "<!-- variant-status:end -->"


def status_file():
    return shadow.LIVE_DIR / "variant_status.json"


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


def scorecard(until=None):
    """The champion vs the shadow variants on the same live sessions: only `source = live`
    signals labelled under the current exit rule, from the first variant-logged day
    (through `until`, a YYYY-MM-DD, when given)."""
    vf = shadow.LIVE_DIR / "shadow_variants.csv"
    if not vf.exists():
        return None
    v = shadow._read(vf)
    s, o = shadow._read(shadow.SIGNALS_FILE), shadow._read(shadow.OUTCOMES_FILE)
    if v.empty or s.empty:
        return None
    v = v[v["source"] == "live"]
    if until:
        v = v[v["date"] <= until]
    start = v["date"].min() if not v.empty else None
    s = s[(s["source"] == "live") & (s["date"] >= start)].drop_duplicates("signal_id")
    if until:
        s = s[s["date"] <= until]
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
                       rule={"enough": n >= RULE["min_trades"] and days >= RULE["min_days"],
                             "beats": d > 0, "ci_excludes_zero": dlo is not None and (dlo > 0 or dhi < 0),
                             "ci_above_zero": dlo is not None and dlo > 0})
        if k != "champion":
            row["conditions"] = conditions(row)
        rows.append(row)
    return {"start": start, "rows": rows, "sessions": int(s["date"].nunique()), "rule": RULE}


def conditions(row):
    """The protocol's conditions for one variant: [(number, text, state)], state one of
    "met", "not met", "not yet"."""
    n, days, r = row["n"], row["days"], row.get("rule") or {}
    out = [(1, f"≥{RULE['min_trades']} trades over ≥{RULE['min_days']} days ({n}/{RULE['min_trades']}, "
               f"{days}/{RULE['min_days']})", "met" if r.get("enough") else "not yet"),
           (2, "mean after 0.05% above the champion's", "met" if r.get("beats") else "not met"),
           # with condition 2, "excludes zero" means the whole interval is above it: a variant
           # that is reliably worse must not count as meeting it
           (3, "95% CI of the difference excludes zero", "met" if r.get("ci_above_zero") else "not met")]
    if row["key"] in SWEEP_AGREES:
        out.append((4, "the step-5 sweep agrees", "met" if SWEEP_AGREES[row["key"]] else "not met"))
    out.append((5, "owner approval", "not yet"))
    return out


def _f(x, digits=3):
    return "—" if x is None or x != x else f"{x:+.{digits}f}%"


def status_line(card, as_of):
    """One line per variant, plus the champion's numbers, for the given date."""
    if not card or not card["rows"]:
        return f"**{as_of}**: no live variant data yet."
    rows = {r["key"]: r for r in card["rows"]}
    c = rows["champion"]
    parts = [f"**{as_of}** · live since {card['start']}, {card['sessions']} session(s). "
             f"{c['label']}: {c['n']} trades, {c['days']} days, after 0.05% {_f(c['mean'][0.05])}."]
    for k, r in rows.items():
        if k == "champion":
            continue
        ci = r.get("diff_ci") or (None, None)
        diff = (f"difference {_f(r.get('diff'))} (95% CI {_f(ci[0])} to {_f(ci[1])})"
                if r.get("diff") is not None else "difference — (no trades on one side)")
        met = [str(i) for i, _, s in r["conditions"] if s == "met"]
        open_ = [f"{i} {s}" for i, _, s in r["conditions"] if s != "met"]
        parts.append(f"{r['label']}: {r['n']} trades, {r['days']} days, after 0.05% {_f(r['mean'][0.05])}; {diff}. "
                     f"Conditions met: {', '.join(met) or 'none'}; " + "; ".join(open_) + ".")
    return " ".join(parts)


def history():
    try:
        return json.loads(status_file().read_text())
    except (FileNotFoundError, ValueError):
        return []


def record(as_of, card=None):
    """Store this date's status (replacing an earlier one for the same date)."""
    card = card if card is not None else scorecard(until=as_of)
    entry = {"date": as_of, "line": status_line(card, as_of)}
    h = [e for e in history() if e.get("date") != as_of] + [entry]
    h.sort(key=lambda e: e["date"])
    p = status_file()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(h, indent=1))
    os.replace(tmp, p)
    return h


def write_doc(h, doc=DOC):
    """Rewrite the status block of docs/THRESHOLD_RESULTS.md (added once, at the end)."""
    block = "\n".join([START, "", *[f"- {e['line']}" for e in h], "", END])
    text = doc.read_text() if doc.exists() else ""
    if START in text and END in text:
        text = re.sub(re.escape(START) + ".*?" + re.escape(END), lambda _: block, text, flags=re.S)
    else:
        text = text.rstrip() + ("\n\n## Weekly live status (Fridays after the close)\n\n"
                                "Written by `variant_status.py`; conditions are those of "
                                "`docs/THRESHOLD_PROTOCOL.md` step 7.\n\n") + block + "\n"
    doc.write_text(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="status as of this date (default: today, IST)")
    a = ap.parse_args()
    as_of = a.date or dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).date().isoformat()
    h = record(as_of)
    write_doc(h)
    print(h[-1]["line"])


if __name__ == "__main__":
    main()
