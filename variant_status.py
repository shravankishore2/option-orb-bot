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

import registry
import shadow

RULE = {"min_trades": 50, "min_days": 15, "cost": 0.05}       # docs/THRESHOLD_PROTOCOL.md, step 7
COST_LEVELS = (0.0, 0.05, 0.10)
# Step 7, condition 4 (threshold variant only): the step-5 sweep at 0.05% cost. Fixed by
# docs/THRESHOLD_RESULTS.md (2026-10-07): 0.54 beats 0.644 on combined total, with and
# without the best month.
SWEEP_AGREES = {"v2@0.54": True}
# docs/V3A_PROTOCOL.md, condition 4: history agrees (docs/V3_LABEL_RESULTS.md, walk-forward top 2%:
# mean after 0.05% +0.134% vs v2 +0.118%, total after 0.10% +170.2 vs +145.0). Fixed in the protocol.
HISTORY_AGREES = {registry.V3A: True}
# variants whose protocol counts from a later session than the first variant-logged day; the
# champion is compared over the same sessions
VARIANT_START = {registry.V3A: registry.V3A_START}
# docs/LATE_CUT_PROTOCOL.md: the late slice is judged on its own, not against the champion
LATE_RULE = {"min_trades": 50, "min_days": 15, "cost": 0.05}
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
    s = s[s["source"] == "live"].drop_duplicates("signal_id")
    if until:
        v, s = v[v["date"] <= until], s[s["date"] <= until]
    out = o.drop_duplicates("signal_id")[["signal_id", "pnl_pct"]] if not o.empty else pd.DataFrame(columns=["signal_id", "pnl_pct"])
    late = late_card(v[v["variant"] == registry.LATE_CUT], s, out)
    v = v[v["variant"] != registry.LATE_CUT]          # judged by its own protocol, below
    start = v["date"].min() if not v.empty else None
    s = s[s["date"] >= start] if start else s.iloc[0:0]
    sets = {"champion": s.loc[s["model_go"].astype(str).isin(["True", "true", "1"]), ["signal_id", "date"]]}
    labels = {"champion": f"Champion ({s['model_version'].iloc[0] if len(s) else 'v2'} @ "
                          f"{float(s['threshold'].iloc[0]) if len(s) else 0.644:.3f})"}
    logged_days = {}                                     # sessions on which each variant logged rows
    for name, g in v.groupby("variant"):
        logged_days[name] = set(g["date"])
        sets[name] = g.loc[g["go"].astype(str).isin(["True", "true", "1"]), ["signal_id", "date"]]
        labels[name] = f"{name} @ {float(g['threshold'].iloc[0]):.3f}" + \
            (f" (from {VARIANT_START[name]})" if name in VARIANT_START else "")
    trades = {k: x.merge(out, on="signal_id").assign(pnl=lambda d: pd.to_numeric(d["pnl_pct"], errors="coerce")).dropna(subset=["pnl"])
              for k, x in sets.items()}
    champ = trades["champion"]
    rows = []
    for k, t in trades.items():
        vstart = VARIANT_START.get(k)
        champ_k = champ
        if k != "champion":                              # "over the same live sessions": only days this
            champ_k = champ[champ["date"].isin(logged_days.get(k, set()))]   # variant logged (8 Oct: none)
        if vstart:                                       # its own window, and the champion's over it
            t, champ_k = t[t["date"] >= vstart], champ_k[champ_k["date"] >= vstart]
        n, days = len(t), t["date"].nunique()
        mean = float(t["pnl"].mean()) if n else None
        lo, hi = _day_boot(t["date"].tolist(), t["pnl"].to_numpy()) if n else (None, None)
        row = {"key": k, "label": labels[k], "n": n, "days": days,
               "hit": float((t["pnl"] > 0).mean() * 100) if n else None,
               "mean": {c: (mean - c if mean is not None else None) for c in COST_LEVELS},
               "ci05": (lo - 0.05, hi - 0.05) if lo is not None else (None, None),
               "worst_day": float(t.groupby("date")["pnl"].sum().min()) if n else None}
        if k != "champion":
            row["start"] = vstart or start
            row["total10"] = {"variant": float((t["pnl"] - 0.10).sum()), "champion": float((champ_k["pnl"] - 0.10).sum())}
        if k != "champion" and n and len(champ_k):
            d = mean - float(champ_k["pnl"].mean())
            dlo, dhi = _day_boot(t["date"].tolist(), t["pnl"].to_numpy(),
                                 other=(champ_k["date"].tolist(), champ_k["pnl"].to_numpy()))
            row.update(diff=d, diff_ci=(dlo, dhi),
                       rule={"enough": n >= RULE["min_trades"] and days >= RULE["min_days"],
                             "beats": d > 0, "ci_excludes_zero": dlo is not None and (dlo > 0 or dhi < 0),
                             "ci_above_zero": dlo is not None and dlo > 0})
        if k != "champion":
            row["conditions"] = conditions(row)
        if k in VARIANT_START:
            row["verdict"], row["locked"] = variant_verdict(row), locked_variant_verdict(k)
        rows.append(row)
    return {"start": start, "rows": rows, "sessions": int(s["date"].nunique()), "rule": RULE, "late": late}


def _is_true(col):
    return col.astype(str).isin(["True", "true", "1"])


def late_card(lv, s, out):
    """docs/LATE_CUT_PROTOCOL.md. The late slice = the champion's GO trades that the
    v2-late-cut variant dropped (entry at or after 15:00), on live sessions from
    registry.LATE_CUT_START. Primary test: the late slice's mean after 0.05%, with a 95%
    day-block bootstrap CI; the cut is supported only if the whole CI is below zero."""
    start = registry.LATE_CUT_START
    lv = lv[lv["date"] >= start]
    sig = s[(s["date"] >= start) & s["signal_id"].isin(lv["signal_id"])]
    champ_go = sig[_is_true(sig["model_go"])][["signal_id", "date", "time"]]
    kept_ids = set(lv.loc[_is_true(lv["go"]), "signal_id"])

    def trades(x):
        return x.merge(out, on="signal_id").assign(pnl=lambda d: pd.to_numeric(d["pnl_pct"], errors="coerce")) \
                .dropna(subset=["pnl"])
    late, kept, champ = (trades(champ_go[~champ_go["signal_id"].isin(kept_ids)]),
                         trades(champ_go[champ_go["signal_id"].isin(kept_ids)]), trades(champ_go))
    n, days = len(late), int(late["date"].nunique())
    lo, hi = _day_boot(late["date"].tolist(), late["pnl"].to_numpy()) if n else (None, None)
    mean = float(late["pnl"].mean()) if n else None
    card = {"start": start, "sessions": int(sig["date"].nunique()), "n": n, "days": days,
            "hit": float((late["pnl"] > 0).mean() * 100) if n else None,
            "mean": {c: (mean - c if mean is not None else None) for c in COST_LEVELS},
            "ci": {c: ((lo - c, hi - c) if lo is not None else (None, None)) for c in (0.05, 0.10)},
            "worst_day": float(late.groupby("date")["pnl"].sum().min()) if n else None,
            "total": {c: {"variant": float((kept["pnl"] - c).sum()), "champion": float((champ["pnl"] - c).sum())}
                      for c in (0.05, 0.10)},
            "consistent": bool(late["time"].map(registry.in_late_slice).all()) if n else True}
    card["floor"] = n >= LATE_RULE["min_trades"] and days >= LATE_RULE["min_days"]
    card["verdict"] = late_verdict(card)
    card["conditions"] = late_conditions(card)
    return card


def late_verdict(card):
    if not card["floor"]:
        return "not enough data"
    lo, hi = card["ci"][0.05]
    if hi is not None and hi < 0:
        return "supported: late entries lose money after 0.05% costs"
    if lo is not None and lo > 0:
        return "not supported: late entries make money after 0.05% costs"
    return "not supported: inconclusive (the CI contains zero)"


def late_conditions(card):
    n, days = card["n"], card["days"]
    hi = card["ci"][0.05][1]
    return [(1, f"≥{LATE_RULE['min_trades']} late GO trades over ≥{LATE_RULE['min_days']} days "
                f"({n}/{LATE_RULE['min_trades']}, {days}/{LATE_RULE['min_days']})", "met" if card["floor"] else "not yet"),
            (2, "whole 95% CI of the late slice's mean after 0.05% below zero",
             ("met" if hi is not None and hi < 0 else "not met") if card["floor"] else "not yet"),
            (3, "owner approval", "not yet")]


def variant_verdict(r):
    """Conditions 1-3 of a mean-vs-champion protocol (V3A_PROTOCOL.md); 4 and 5 are listed apart."""
    rule = r.get("rule") or {}
    if not rule.get("enough"):
        return "not enough data"
    if rule.get("beats") and rule.get("ci_above_zero"):
        return "conditions 1-3 met (owner decides)"
    return "not supported: " + ("ahead, but the CI of the difference includes zero" if rule.get("beats")
                                else "not ahead of the champion")


def locked_variant_verdict(name, h=None):
    """The verdict on the first Friday status at which the variant's floor was met, or None."""
    for e in (h if h is not None else history()):
        v = (e.get("variants") or {}).get(name)
        if v and v.get("floor"):
            return {"date": e["date"], "verdict": v["verdict"]}
    return None


def locked_late_verdict(h=None):
    """The verdict on the first Friday status at which the floor was met (protocol: later
    weeks are reported but don't replace it), or None."""
    for e in (h if h is not None else history()):
        if (e.get("late") or {}).get("floor"):
            return {"date": e["date"], "verdict": e["late"]["verdict"]}
    return None


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
    if row["key"] in HISTORY_AGREES:
        out.append((4, "history agrees (docs/V3_LABEL_RESULTS.md)", "met" if HISTORY_AGREES[row["key"]] else "not met"))
    out.append((5, "owner approval", "not yet"))
    return out


def _f(x, digits=3):
    return "—" if x is None or x != x else f"{x:+.{digits}f}%"


def late_line(late):
    if not late:
        return ""
    ci = {c: late["ci"][c] for c in (0.05, 0.10)}
    tot = late["total"]
    state = "; ".join(f"{i} {s}" for i, _, s in late["conditions"])
    return (f"{registry.LATE_CUT} (late slice: champion GO entered at or after 15:00, live from {late['start']}): "
            f"{late['n']} late trades, {late['days']} days, after 0.05% {_f(late['mean'][0.05])} "
            f"(95% CI {_f(ci[0.05][0])} to {_f(ci[0.05][1])}), after 0.10% {_f(late['mean'][0.10])} "
            f"(95% CI {_f(ci[0.10][0])} to {_f(ci[0.10][1])}); total after 0.05%: variant {tot[0.05]['variant']:+.2f} vs "
            f"champion {tot[0.05]['champion']:+.2f}, after 0.10%: {tot[0.10]['variant']:+.2f} vs "
            f"{tot[0.10]['champion']:+.2f}. Conditions (docs/LATE_CUT_PROTOCOL.md): {state}. Verdict: {late['verdict']}.")


def status_line(card, as_of):
    """One line per variant, plus the champion's numbers, for the given date."""
    if not card or not card["rows"]:
        return f"**{as_of}**: no live variant data yet." + (" " + late_line(card.get("late")) if card else "")
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
        extra = ""
        if k in VARIANT_START:                           # its protocol's secondary and verdict
            tt = r["total10"]
            extra = (f" Secondary: total after 0.10% {tt['variant']:+.2f} vs champion {tt['champion']:+.2f}."
                     f" Verdict: {variant_verdict(r)}.")
        parts.append(f"{r['label']}: {r['n']} trades, {r['days']} days, after 0.05% {_f(r['mean'][0.05])}; {diff}. "
                     f"Conditions met: {', '.join(met) or 'none'}; " + "; ".join(open_) + "." + extra)
    if card.get("late"):
        parts.append(late_line(card["late"]))
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
    if card and card.get("late"):
        entry["late"] = {"floor": card["late"]["floor"], "verdict": card["late"]["verdict"]}
    for r in (card or {}).get("rows", []):
        if r["key"] in VARIANT_START:
            entry.setdefault("variants", {})[r["key"]] = {"floor": bool((r.get("rule") or {}).get("enough")),
                                                         "verdict": variant_verdict(r)}
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
