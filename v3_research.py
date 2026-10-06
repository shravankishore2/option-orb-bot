"""
v3_research.py — the v3 candidate study (docs/V3_PROTOCOL.md). Research only:
nothing here is used by the live bot, the champion or the v2 baseline.

    ORBITAL_OFFLINE=1 python v3_research.py replay      # §4: signals, retests, P&L
    ORBITAL_OFFLINE=1 python v3_research.py features    # §5: features for both entry types
    ORBITAL_OFFLINE=1 python v3_research.py evaluate    # §6-8: selection, walk-forward, report

Outputs go to data/research/v3/ (git-ignored) and docs/V3_RESULTS.md.
"""

import datetime as dt
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

import strategy_config as C

OUT = Path("data/research/v3")
START, END = dt.date(2021, 1, 1), dt.date(2026, 9, 22)
RETEST_X, RETEST_N = 0.25, 6                               # §4b, fixed in the protocol
SENS_X, SENS_N = (0.10, 0.25, 0.50), (3, 6, 12)            # §4b-7 sensitivity grid
CANDLE = pd.Timedelta(minutes=C.CANDLE_MINUTES)
SIG_COLS = ["date", "time", "symbol", "direction", "entry_price", "ORH", "ORL", "prev_close"]


# ---------------------------------------------------------------- §4b retest
def find_retest(g, sig, x=RETEST_X, n=RETEST_N):
    """The retest of one breakout, per the protocol. g: the day's completed candles.

    Returns a dict: outcome (retest / no_retest / closed_inside / too_late) and, for a
    retest, the entry (close of R, timed at R's end) plus the raw inputs of the retest
    features. Only bars up to and including R are read (R must have closed)."""
    long = sig["direction"] == "BUY"
    orh, orl = float(sig["ORH"]), float(sig["ORL"])
    orb = orh - orl
    edge = orh if long else orl
    b_stamp = pd.Timestamp(dt.datetime.combine(dt.date.fromisoformat(sig["date"]),
                                               dt.time.fromisoformat(sig["time"]))).tz_localize(g.index.tz) - CANDLE
    pos = g.index.get_indexer([b_stamp])[0]
    if pos < 0 or orb <= 0:
        return {"outcome": "no_breakout_bar"}
    hi, lo, cl, vol = (g[c].to_numpy(float) for c in ("High", "Low", "Close", "Volume"))
    for k in range(1, n + 1):
        r = pos + k
        if r >= len(g):
            return {"outcome": "no_retest"}
        inside = cl[r] <= edge if long else cl[r] >= edge
        if inside:
            return {"outcome": "closed_inside", "bars": k}
        touched = lo[r] <= edge + x * orb if long else hi[r] >= edge - x * orb
        if touched:
            entry_end = g.index[r] + CANDLE
            if entry_end.time() > C.LAST_ENTRY_TIME:
                return {"outcome": "too_late", "bars": k}
            ext = hi[pos:r].max() if long else lo[pos:r].min()          # extreme in [B, R)
            rng = hi[r] - lo[r]
            sign = 1.0 if long else -1.0
            return {
                "outcome": "retest", "bars": k, "time": entry_end.strftime("%H:%M:%S"),
                "entry_price": round(float(cl[r]), 2),
                "retest_depth_norm": sign * (ext - (lo[r] if long else hi[r])) / orb,
                "level_penetration": max(0.0, sign * (edge - (lo[r] if long else hi[r]))) / orb,
                "retest_vol_ratio": vol[r] / vol[pos] if vol[pos] > 0 else np.nan,
                "retest_rejection": ((cl[r] - lo[r]) / rng if long else (hi[r] - cl[r]) / rng) if rng > 0 else 0.5,
                "bars_to_retest": k,
                "pre_retest_excursion": sign * (ext - edge) / orb,
                "retest_extreme": float(lo[r] if long else hi[r]),        # for stop_dist_r (needs ATR)
            }
    return {"outcome": "no_retest"}


# ---------------------------------------------------------------- §4 replay
def _replay_symbol(args):
    import exits
    import history_cache
    import dhan_client as dhan
    from build_historical_signals import prev_session, replay_day, score
    sym, days = args
    try:
        c = history_cache.get_candles(sym, START, END, interval=dhan.INTERVAL_5M)
        daily = history_cache.get_daily_candles(sym, START - dt.timedelta(days=15), END)
    except dhan.DhanError:
        return sym, [], [], [], []
    if c.empty or daily.empty:
        return sym, [], [], [], []
    c = dhan.regular_session(c)
    check, nomove, retests, sens = [], [], [], []
    for day, g in c.groupby(c.index.date):
        if days is not None and day not in days:
            continue
        prev = prev_session(daily, day)
        if prev is None:
            continue
        ph, pl, pc = prev
        if pc <= 0 or ph <= pl:
            continue
        for s in replay_day(sym, day, g, ph, pl, pc):                       # today's rule: reproduction check
            r = score(s, g)
            check.append({**s, "pnl_%": r["pnl_%"] if r else np.nan})
        for s in replay_day(sym, day, g, ph, pl, pc, rules="orbital_nomove"):
            r = score(s, g)
            if r is None:
                continue
            nomove.append({**s, "exit_reason": r["exit_reason"], "pnl_%": r["pnl_%"]})
            for x in SENS_X:
                for n in SENS_N:
                    rt = find_retest(g, s, x, n)
                    row = {"date": s["date"], "symbol": sym, "direction": s["direction"], "x": x, "n": n,
                           "outcome": rt["outcome"], "breakout_pnl": r["pnl_%"], "retest_pnl": np.nan}
                    if rt["outcome"] == "retest":
                        out = exits.simulate_day(g, dt.time.fromisoformat(rt["time"]), rt["entry_price"],
                                                 s["ORH"], s["ORL"], s["direction"])
                        if out is not None:
                            row["retest_pnl"], row["retest_exit"] = round(out[0], 4), out[1]
                            if x == RETEST_X and n == RETEST_N:
                                retests.append({**{k: s[k] for k in SIG_COLS}, "time": rt["time"],
                                                "entry_price": rt["entry_price"], "breakout_time": s["time"],
                                                "breakout_price": s["entry_price"], "exit_reason": out[1],
                                                "pnl_%": round(out[0], 4),
                                                **{k: v for k, v in rt.items() if k not in ("outcome", "time", "entry_price", "bars")}})
                    sens.append(row)
    return sym, check, nomove, retests, sens


def replay():
    from build_historical_signals import universe
    OUT.mkdir(parents=True, exist_ok=True)
    targets = universe("pit", START, END)
    print(f"replaying {len(targets)} symbols, {START} → {END}", flush=True)
    check, nomove, retests, sens = [], [], [], []
    with ProcessPoolExecutor(max_workers=max(1, (os.cpu_count() or 2) - 1)) as ex:
        for i, (sym, a, b, c_, d) in enumerate(ex.map(_replay_symbol, targets, chunksize=4), 1):
            check += a; nomove += b; retests += c_; sens += d
            if i % 50 == 0:
                print(f"  {i}/{len(targets)} symbols, {len(nomove):,} breakouts", flush=True)
    by = ["date", "time", "symbol", "direction"]
    pd.DataFrame(check).sort_values(by).to_csv(OUT / "signals_orbital_check.csv", index=False)
    nm = pd.DataFrame(nomove).sort_values(by)
    nm[SIG_COLS].to_csv(OUT / "signals_nomove.csv", index=False)
    nm[SIG_COLS + ["exit_reason", "pnl_%"]].to_csv(OUT / "signals_nomove_results.csv", index=False)
    rt = pd.DataFrame(retests).sort_values(by)
    rt[SIG_COLS].to_csv(OUT / "signals_retest.csv", index=False)
    rt.to_csv(OUT / "retest_rows.csv", index=False)
    rt[SIG_COLS + ["exit_reason", "pnl_%"]].to_csv(OUT / "signals_retest_results.csv", index=False)
    pd.DataFrame(sens).to_csv(OUT / "retest_outcomes.csv", index=False)
    print(f"done: {len(check):,} current-rule, {len(nm):,} new-rule breakouts, {len(rt):,} retest entries")


# ---------------------------------------------------------------- §5 features
def _mod_volume_symbol(args):
    """rel_vol_entry_mod for one symbol's signals: the volume of the last bar that closed
    before entry, over the median volume of that same bar of the day across the prior
    20 sessions (strictly before the signal's day)."""
    import history_cache
    import dhan_client as dhan
    sym, rows = args
    try:
        c = dhan.regular_session(history_cache.get_candles(sym, START, END, interval=dhan.INTERVAL_5M))
    except dhan.DhanError:
        return []
    if c.empty:
        return []
    v = pd.DataFrame({"day": c.index.date, "tod": c.index.time, "vol": c["Volume"].to_numpy(float)})
    wide = v.pivot_table(index="day", columns="tod", values="vol", aggfunc="last").sort_index()
    base = wide.rolling(20, min_periods=5).median().shift(1)          # prior sessions only
    out = []
    for r in rows:
        day = dt.date.fromisoformat(r["date"])
        bar = (dt.datetime.combine(day, dt.time.fromisoformat(r["time"])) - CANDLE).time()
        try:
            num, den = wide.at[day, bar], base.at[day, bar]
        except KeyError:
            num = den = np.nan
        out.append({"date": r["date"], "symbol": sym, "direction": r["direction"],
                    "rel_vol_entry_mod": num / den if den and den > 0 and pd.notna(num) else np.nan})
    return out


def features():
    import mine_features
    for kind in ("nomove", "retest"):
        sig = OUT / f"signals_{kind}.csv"
        print(f"mining features for {sig} ...", flush=True)
        mine_features.build(str(sig), str(OUT / f"features_{kind}.csv"), universe="pit")
        df = pd.read_csv(sig)
        jobs = [(s, g.to_dict("records")) for s, g in df.groupby("symbol")]
        rows = []
        with ProcessPoolExecutor(max_workers=max(1, (os.cpu_count() or 2) - 1)) as ex:
            for part in ex.map(_mod_volume_symbol, jobs, chunksize=4):
                rows += part
        pd.DataFrame(rows).to_csv(OUT / f"extra_{kind}.csv", index=False)
        print(f"  {kind}: {len(df):,} signals, minute-of-day volume baseline for {len(rows):,}", flush=True)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"replay": replay, "features": features, "evaluate": lambda: evaluate_all()}.get(cmd, lambda: sys.exit(__doc__))()


# ---------------------------------------------------------------- §6-8 evaluation
GROUPS = {                                                     # §5, fixed in the protocol
    "stock_id": ["entry_log", "orb_range_abs"],
    "prev_close_vs_orb": ["prev_close_vs_orb"],
    "sheet 3/22/23": ["breakout_strength", "vwap_dist", "move_from_open"],
    "sheet 8/9/1": ["orb_range_abs", "entry_log", "orb_range_pct"],
    "bucket 3 (contextual)": ["sector_ret", "breadth", "nifty_or_pos", "atr_pct", "vol_trend", "range_expansion",
                              "move_from_open"],
    "market": ["nifty_ret", "aligned_with_nifty", "breadth", "nifty_or_pos"],
    "volume": ["rel_vol_or", "rel_vol_entry", "vol_trend"],
    "levels": ["dist_pdh", "dist_pdl", "level_touches", "consec_bars", "pos_in_day_range"],
}
FIXES = ["gap_dir", "prev_close_vs_orb_dir", "nifty_ret_dir", "sector_ret_dir", "dist_pd_level_dir", "rel_vol_entry_mod"]
RETEST_FEATS = ["retest_depth_norm", "level_penetration", "retest_vol_ratio", "retest_rejection", "bars_to_retest",
                "pre_retest_excursion", "stop_dist_r"]
TEST_FIRST = ["stock_id", "prev_close_vs_orb"]
ABLATION_ORDER = ["sheet 3/22/23", "sheet 8/9/1", "bucket 3 (contextual)", "market", "volume", "levels"]
TEST_YEARS = (2022, 2023, 2024, 2025, 2026)
TEST_FROM, TEST_TO = "2022-01-01", "2026-09-22"
PERIODS = {"clean_backward": ("2022-01-01", "2023-08-31"), "development": ("2023-09-05", "2026-09-04"),
           "clean_forward": ("2026-09-05", "2026-09-22"), "combined": (TEST_FROM, TEST_TO)}
COSTS = (0.0, 0.05, 0.10)
NOISE_DRAWS = 30
CV_FOLDS = 5
TOP_SHARE = 1 - C.THRESHOLD_QUANTILE                          # the top 2%


def load(kind):
    """A v3 dataset with every candidate input: base 28 + fixes (+ retest features)."""
    from features import FEATURES as CONTEXT, geometry_features
    keys = ["date", "symbol", "direction"]
    sig = pd.read_csv(OUT / f"signals_{kind}.csv")
    res = pd.read_csv(OUT / f"signals_{kind}_results.csv")
    ext = pd.read_csv(OUT / f"features_{kind}.csv")
    extra = pd.read_csv(OUT / f"extra_{kind}.csv")
    for f in (sig, res, ext):
        f.columns = [c.lower() for c in f.columns]
    df = sig.merge(res[keys + ["exit_reason", "pnl_%"]], on=keys).merge(
        ext.drop(columns=["time"], errors="ignore").drop_duplicates(keys), on=keys).merge(
        extra.drop_duplicates(keys), on=keys, how="left")
    if kind == "retest":
        rt = pd.read_csv(OUT / "retest_rows.csv")
        rt.columns = [c.lower() for c in rt.columns]
        df = df.merge(rt[keys + [c for c in RETEST_FEATS if c != "stop_dist_r"] + ["retest_extreme"]]
                      .drop_duplicates(keys), on=keys, how="left")
    df, base = geometry_features(df)
    df = df.loc[:, ~df.columns.duplicated()]
    base = list(dict.fromkeys(base + list(CONTEXT)))
    sign = np.where(df["direction"].str.upper() == "BUY", 1.0, -1.0)
    df["gap_dir"] = sign * df["gap_pct"]
    df["prev_close_vs_orb_dir"] = np.where(sign > 0, df["prev_close_vs_orb"], 1 - df["prev_close_vs_orb"])
    df["nifty_ret_dir"] = sign * df["nifty_ret"]
    df["sector_ret_dir"] = sign * df["sector_ret"]
    df["dist_pd_level_dir"] = np.where(sign > 0, df["dist_pdh"], -df["dist_pdl"])
    cand = base + FIXES
    if kind == "retest":
        atr = df["atr_pct"] * df["entry_price"] / 100
        df["stop_dist_r"] = sign * (df["entry_price"] - df["retest_extreme"]) / atr.replace(0, np.nan)
        cand += RETEST_FEATS
    df[cand] = df[cand].apply(pd.to_numeric, errors="coerce")
    df["profit"] = (df["pnl_%"] > 0).astype(int)
    df["month"] = df["date"].str[:7]
    return df.reset_index(drop=True), base, cand


def _fit(train, feats):
    from xgboost import XGBClassifier
    params = dict(C.MODEL_PARAMS)
    y = train["profit"]
    params["scale_pos_weight"] = (y == 0).sum() / max((y == 1).sum(), 1)
    m = XGBClassifier(**params)
    m.fit(train[feats].fillna(0), y)
    return m


def cv_metric(data, feats, folds=CV_FOLDS, permute=None, seed=0):
    """Date-grouped K-fold: mean P&L per trade of the top 2% scores in each held-out fold.
    permute: columns shuffled together inside the held-out fold (grouped importance)."""
    from sklearn.model_selection import GroupKFold
    vals = []
    rng = np.random.default_rng(seed)
    for fit_idx, val_idx in GroupKFold(n_splits=folds).split(data, groups=data["date"]):
        fit, val = data.iloc[fit_idx], data.iloc[val_idx].copy()
        m = _fit(fit, feats)
        if permute:
            perm = rng.permutation(len(val))
            val[permute] = val[permute].to_numpy()[perm]
        s = m.predict_proba(val[feats].fillna(0))[:, 1]
        k = max(1, int(round(len(val) * TOP_SHARE)))
        vals.append(val["pnl_%"].to_numpy()[np.argsort(-s)[:k]].mean())
    vals = np.asarray(vals)
    return float(vals.mean()), float(vals.std(ddof=1) / np.sqrt(len(vals)))


def select(data, base, cand, groups, additions, log, label):
    """§6 for one selection window. Returns the chosen features."""
    feats = list(cand)
    cur, se = cv_metric(data, feats); n_cfg = 1
    start_metric = cur
    log.append(f"  start ({len(feats)} inputs): CV {cur:+.4f}% ± {se:.4f}")
    def drop(fs, g):
        return [f for f in fs if f not in groups[g]]
    for g in TEST_FIRST + [x for x in ABLATION_ORDER if x in groups]:
        trial = drop(feats, g)
        if trial == feats:
            continue
        m, s = cv_metric(data, trial); n_cfg += 1
        keep_drop = m > cur - se                                # parsimony: dropping costs < 1 SE
        log.append(f"  drop {g:<22} CV {m:+.4f}% ({m - cur:+.4f}) -> {'dropped' if keep_drop else 'kept'}")
        if keep_drop:
            feats, cur, se = trial, m, s
    for g in additions:                                         # additions stay only if they help by >= 1 SE
        trial = drop(feats, g)
        if trial == feats:
            continue
        m, s = cv_metric(data, trial); n_cfg += 1
        helps = m <= cur - se
        log.append(f"  addition {g:<18} without it CV {m:+.4f}% ({m - cur:+.4f}) -> {'kept' if helps else 'dropped'}")
        if not helps:
            feats, cur, se = trial, m, s
    dropped_base = [f for f in base if f not in feats]
    noise = []
    if dropped_base:
        rng = np.random.default_rng(1)
        kept_add = [f for f in feats if f not in base]
        for _ in range(NOISE_DRAWS):
            rnd = list(rng.choice(base, size=len(dropped_base), replace=False))
            trial = [f for f in base if f not in rnd] + kept_add
            noise.append(cv_metric(data, trial)[0] - start_metric); n_cfg += 1
    imp = {}
    for g, cols in groups.items():
        present = [c for c in cols if c in feats]
        if present:
            imp[g] = cur - cv_metric(data, feats, permute=present)[0]
    log.append(f"  chosen {len(feats)} inputs: CV {cur:+.4f}% (start {start_metric:+.4f}); dropped base: "
               f"{', '.join(dropped_base) or 'none'}; added kept: {', '.join(f for f in feats if f not in base) or 'none'}")
    if noise:
        pct = float((np.asarray(noise) < cur - start_metric).mean() * 100)
        log.append(f"  noise benchmark: chosen change {cur - start_metric:+.4f}% vs {NOISE_DRAWS} random drops of "
                   f"{len(dropped_base)} base inputs (median {np.median(noise):+.4f}%, max {max(noise):+.4f}%) -> "
                   f"better than {pct:.0f}% of them")
    log.append("  grouped permutation importance: " + ", ".join(f"{g} {v:+.4f}" for g, v in
                                                            sorted(imp.items(), key=lambda kv: -kv[1])))
    return feats, n_cfg


def walk(df, feats_by_year, label):
    """Monthly walk-forward over the test months with each year's feature set."""
    import walk_forward as W
    out = []
    for m in sorted(df.loc[(df["date"] >= TEST_FROM) & (df["date"] <= TEST_TO), "month"].unique()):
        train, test = df[df["month"] < m], df[df["month"] == m].copy()
        if len(train) < C.MIN_TRAIN_SIGNALS or test.empty:
            continue
        feats = feats_by_year[int(m[:4])]
        model, thr, _ = W.fit_with_threshold(train, feats, "profit")
        test["score"] = model.predict_proba(test[feats].fillna(0))[:, 1]
        test["go"] = test["score"] >= thr
        out.append(test)
    r = pd.concat(out, ignore_index=True)
    r["variant"] = label
    return r


def summarize(trades, pool, label, period):
    from stats import block_bootstrap_ci, permutation_vs_random
    a, b = PERIODS[period]
    t = trades[(trades["date"] >= a) & (trades["date"] <= b)]
    p = pool[(pool["date"] >= a) & (pool["date"] <= b)]
    if t.empty:
        return {"variant": label, "period": period, "trades": 0}
    x = t["pnl_%"]
    days = p["date"].nunique()
    by_m = t.groupby(t["date"].str[:7])["pnl_%"]
    lo, hi = block_bootstrap_ci(t)
    rnd, pval = permutation_vs_random(t, p) if len(t) < len(p) else (np.nan, np.nan)
    best = by_m.sum().idxmax()
    ex = t[t["date"].str[:7] != best]["pnl_%"]
    return {"variant": label, "period": period, "trades": len(t), "per_day": len(t) / max(days, 1),
            "mean": x.mean(), "net05": x.mean() - 0.05, "net10": x.mean() - 0.10, "hit": (x > 0).mean() * 100,
            "worst_trade": x.min(), "worst_month_sum": by_m.sum().min(), "worst_month_mean": by_m.mean().min(),
            "ci": (lo, hi), "random": rnd, "p_random": pval, "ex_best_month": ex.mean() if len(ex) else np.nan,
            "best_month": best}


def reality_check(series, base, reps=5000, seed=3):
    """White's reality check on mean P&L per trade, day-block bootstrap, recentred.
    series: {variant: trades df}; base: V0 trades. p for the best variant vs V0."""
    days = sorted(set(base["date"]).union(*[set(t["date"]) for t in series.values()]))
    idx = {d: i for i, d in enumerate(days)}

    def agg(t):
        s = np.zeros(len(days)); n = np.zeros(len(days))
        np.add.at(s, t["date"].map(idx).to_numpy(), t["pnl_%"].to_numpy())
        np.add.at(n, t["date"].map(idx).to_numpy(), 1)
        return s, n
    bs, bn = agg(base)
    vs = {k: agg(t) for k, t in series.items()}
    obs = {k: s.sum() / n.sum() - bs.sum() / bn.sum() for k, (s, n) in vs.items()}
    rng = np.random.default_rng(seed)
    mx = np.empty(reps)
    for r in range(reps):
        pick = rng.integers(0, len(days), len(days))
        b = bs[pick].sum() / max(bn[pick].sum(), 1)
        mx[r] = max((s[pick].sum() / max(n[pick].sum(), 1) - b) - obs[k] for k, (s, n) in vs.items())
    best = max(obs, key=obs.get)
    return best, obs, float((mx >= obs[best]).mean())


def evaluate_all():
    log, rows, n_total = [], [], 0
    v0 = pd.read_csv("data/research/walkforward.csv", dtype={"date": str})
    v0 = v0[(v0["date"] >= TEST_FROM) & (v0["date"] <= TEST_TO)]
    nm, base, cand_nm = load("nomove")
    rt, _, cand_rt = load("retest")
    log += [f"datasets: new-rule breakouts {len(nm):,}, retest entries {len(rt):,} (2021-01 → 2026-09-22)"]

    variants = {"V0 v2 as-is": v0[v0["go"].astype(str) == "True"]}
    pools = {"V0 v2 as-is": v0}
    v1 = walk(nm, {y: base for y in TEST_YEARS}, "V1")
    variants["V1 v2 recipe, new rule"], pools["V1 v2 recipe, new rule"] = v1[v1["go"]], v1
    for kind, data, cand, adds, label in (("nomove", nm, cand_nm, ["fixes"], "V2 v3 breakout"),
                                          ("retest", rt, cand_rt, ["fixes", "retest"], "V3 v3 retest")):
        groups = dict(GROUPS, fixes=FIXES, **({"retest": RETEST_FEATS} if kind == "retest" else {}))
        chosen = {}
        for y in TEST_YEARS:
            log.append(f"[{label}] selection for {y} on data before {y}-01-01:")
            chosen[y], n = select(data[data["date"] < f"{y}-01-01"], base, cand, groups, adds, log, label)
            n_total += n
        r = walk(data, chosen, label)
        variants[label], pools[label] = r[r["go"]], r
        log.append(f"[{label}] features by year: " + "; ".join(f"{y}: {len(f)}" for y, f in chosen.items()))
    every = nm[(nm["date"] >= TEST_FROM) & (nm["date"] <= TEST_TO)]
    variants["V4 every signal, new rule"], pools["V4 every signal, new rule"] = every, every
    variants["V4r every signal, current rule"], pools["V4r every signal, current rule"] = v0, v0
    for label, t in variants.items():
        for period in PERIODS:
            rows.append(summarize(t, pools[label], label, period))
    best, obs, p = reality_check({k: v for k, v in variants.items() if k[:2] in ("V1", "V2", "V3")},
                                 variants["V0 v2 as-is"])
    write_report(rows, log, n_total, best, obs, p, variants, nm)


def _fmt(v, f="{:+.3f}%"):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else f.format(v)


def write_report(rows, log, n_total, best, obs, p, variants, nm):
    keys = ["date", "symbol", "direction"]
    md = ["# v3 candidate — results", "",
          "Follows [`V3_PROTOCOL.md`](V3_PROTOCOL.md) (committed before any v3 code ran). **Exploratory: every "
          "session 2021-01 → 2026-09-22 was examined while v1 and v2 were built.** Nothing here changes the live "
          "bot, the champion or the frozen v2 baseline.", ""]
    # §4a counts + reproduction check
    chk = pd.read_csv(OUT / "signals_orbital_check.csv")
    pub = pd.read_csv("data/research/signals_orbital.csv")
    same = chk[keys + ["time"]].merge(pub[keys + ["time"]], on=keys + ["time"]).shape[0]
    md += ["## Entry rule without the ±1.8% condition (§4a)", "",
           f"Integrity check: the replay re-derived the current rule's signals: {len(chk):,} vs {len(pub):,} "
           f"published, {same:,} identical (same stock, day, direction and time).", "",
           "| Period | Current rule | New rule | Also fired by the current rule | Extra signals |", "|---|---|---|---|---|"]
    for period, (a, b) in list(PERIODS.items()) + [("all history", ("2021-01-01", TEST_TO))]:
        c = chk[(chk["date"] >= a) & (chk["date"] <= b)]
        n = nm[(nm["date"] >= a) & (nm["date"] <= b)]
        both = n[keys].merge(c[keys], on=keys).shape[0]
        md.append(f"| {period} ({a} → {b}) | {len(c):,} | {len(n):,} | {both:,} | +{len(n) - both:,} ({(len(n) / max(len(c), 1)):.1f}×) |")
    # comparison table
    md += ["", "## Comparison (§7) — identical walk-forward months and costs", "",
           "P&L per trade, gross; the net columns subtract a flat round trip. Worst month = lowest monthly "
           "sum / mean of the variant's trades. *p (random)*: share of random picks from the same daily pool "
           "that did at least as well.", ""]
    for period in PERIODS:
        md += [f"### {period} ({PERIODS[period][0]} → {PERIODS[period][1]})", "",
               "| Variant | Trades | /day | Mean | after 0.05% | after 0.10% | Hit | Worst trade | Worst month (sum / mean) | 95% CI | Random picks | p (random) | Without best month |",
               "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for r in [r for r in rows if r["period"] == period]:
            if not r["trades"]:
                md.append(f"| {r['variant']} | 0 | | | | | | | | | | | |")
                continue
            md.append(f"| {r['variant']} | {r['trades']:,} | {r['per_day']:.2f} | {_fmt(r['mean'])} | {_fmt(r['net05'])} | "
                      f"{_fmt(r['net10'])} | {r['hit']:.1f}% | {_fmt(r['worst_trade'], '{:+.2f}%')} | "
                      f"{_fmt(r['worst_month_sum'], '{:+.1f}%')} / {_fmt(r['worst_month_mean'])} | "
                      f"[{_fmt(r['ci'][0])}, {_fmt(r['ci'][1])}] | {_fmt(r['random'])} | {_fmt(r['p_random'], '{:.3f}')} | "
                      f"{_fmt(r['ex_best_month'])} ({r['best_month']}) |")
        md.append("")
    # pre-registered, separate
    v1 = pd.read_csv("data/research/walkforward_hit_1_5r.csv", dtype={"date": str})
    v1 = v1[(v1["go"].astype(str) == "True") & (v1["date"] >= "2022-01-01") & (v1["date"] <= "2023-08-31")]
    md += ["## Pre-registered results (kept separate, unchanged)", "",
           f"- **v1, pre-registered clean test 2022-01 → 2023-08:** {len(v1):,} trades, {v1['pnl_%'].mean():+.3f}% per trade "
           "(the published −0.027%).",
           "- **v2 forward test from 2026-09-23:** live and replayed sessions on the dashboard scorecard; not part of "
           "this study's data, and far too few GO trades to read yet.", ""]
    # decision rule
    comb = {r["variant"]: r for r in rows if r["period"] == "combined"}
    v0c = comb["V0 v2 as-is"]
    md += ["## Selection noise and the decision rule (§7–8)", "",
           f"- Configurations tried: **{n_total + 1 + 9 + 1}** (feature-selection CV evaluations across all years "
           f"{n_total}, V1 1, the retest sensitivity grid 9, the current-list sensitivity 1).",
           f"- White's reality check, V1–V3 vs V0, mean P&L per trade, combined months: best = **{best}** "
           f"({obs[best]:+.4f}% per trade vs V0), **p = {p:.3f}**. Differences: "
           + ", ".join(f"{k} {v:+.4f}%" for k, v in obs.items()) + ".", ""]
    md += ["| Variant | 1. beats V0 after 0.05% | 2. reality-check p < 0.05 | 3. beats V0 without best months | 4. ≥ 100 GO trades | Beats v2? |",
           "|---|---|---|---|---|---|"]
    winners = []
    for k in [k for k in comb if k[:2] in ("V1", "V2", "V3")]:
        r = comb[k]
        c1 = r["trades"] and r["net05"] > v0c["net05"]
        c2 = p < 0.05 and k == best
        c3 = r["trades"] and r["ex_best_month"] > v0c["ex_best_month"]
        c4 = r["trades"] >= 100
        ok = bool(c1 and c2 and c3 and c4)
        winners += [k] if ok else []
        md.append(f"| {k} | {'yes' if c1 else 'no'} | {'yes' if c2 else 'no'} | {'yes' if c3 else 'no'} | "
                  f"{'yes' if c4 else 'no'} | **{'yes' if ok else 'no'}** |")
    md += ["", f"**Outcome:** {'a v3 variant beats v2 out-of-sample: ' + ', '.join(winners) + ' (shadow-track per §8)' if winners else 'no v3 variant beats v2 out-of-sample by the protocol rule; nothing is shadow-tracked.'}", ""]
    # retest tables
    out = pd.read_csv(OUT / "retest_outcomes.csv", dtype={"date": str})
    out = out[(out["date"] >= TEST_FROM) & (out["date"] <= TEST_TO)]
    prim = out[(out["x"] == RETEST_X) & (out["n"] == RETEST_N)]
    md += ["## Retest entry (§4b), test months, every breakout (no model)", "",
           f"Definition: within {RETEST_N} bars, a bar whose low (high for shorts) comes within {RETEST_X} × ORB of "
           "the edge and closes outside the range; a close back inside first kills the setup; entry at that bar's "
           "close once it has closed.", "",
           "| Outcome | Breakouts | Share | Breakout-entry P&L (mean) | Retest-entry P&L (mean) |", "|---|---|---|---|---|"]
    for o, g in prim.groupby("outcome"):
        md.append(f"| {o} | {len(g):,} | {len(g) / len(prim) * 100:.1f}% | {g['breakout_pnl'].mean():+.3f}% | "
                  f"{_fmt(g['retest_pnl'].mean()) if g['retest_pnl'].notna().any() else '—'} |")
    nr = prim[prim["outcome"] != "retest"]
    rr = prim[prim["outcome"] == "retest"]
    md += ["", "**Cost of waiting** (per breakout, all test months):", "",
           f"- Breakouts with no retest entry: {len(nr):,}; had they been taken at the breakout: "
           f"{(nr['breakout_pnl'] > 0).sum():,} winners missed (avg {_fmt(nr.loc[nr['breakout_pnl'] > 0, 'breakout_pnl'].mean())}), "
           f"{(nr['breakout_pnl'] < 0).sum():,} losers avoided (avg {_fmt(nr.loc[nr['breakout_pnl'] < 0, 'breakout_pnl'].mean())}).",
           f"- Retested breakouts ({len(rr):,}): breakout entry {_fmt(rr['breakout_pnl'].mean())} vs retest entry "
           f"{_fmt(rr['retest_pnl'].mean())} per trade.",
           f"- Net per breakout: take every breakout {_fmt(prim['breakout_pnl'].mean())}; wait for the retest "
           f"{_fmt(prim['retest_pnl'].fillna(0).mean())} (0 for breakouts not entered).", "",
           "Sensitivity (reported, not used to choose): take-every-retest mean P&L per trade (entries)", "",
           "| Band X (× ORB) | N = 3 bars | N = 6 bars | N = 12 bars |", "|---|---|---|---|"]
    for x in SENS_X:
        cells = []
        for n in SENS_N:
            g = out[(out["x"] == x) & (out["n"] == n) & out["retest_pnl"].notna()]
            cells.append(f"{g['retest_pnl'].mean():+.3f}% ({len(g):,})" if len(g) else "—")
        md.append(f"| {x} | " + " | ".join(cells) + " |")
    # current-list sensitivity
    try:
        from fetch_symbols import get_symbols
        cur = set(get_symbols(quiet=True))
        md += ["", "## Sensitivity: today's Nifty 200 only (survivorship-biased, not used to choose)", "",
               "| Variant | Trades | Mean per trade |", "|---|---|---|"]
        for k, t in variants.items():
            tt = t[t["symbol"].isin(cur)]
            md.append(f"| {k} | {len(tt):,} | {_fmt(tt['pnl_%'].mean())} |")
    except Exception as e:                                      # never block the report on the live list
        md.append(f"(current-list sensitivity skipped: {e})")
    md += ["", "## Feature selection log (§6)", "", "```"] + log + ["```", ""]
    Path("docs/V3_RESULTS.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
