"""
stats.py — statistics for trade lists, done carefully.

Trades on the same day are NOT independent (one market regime drives them all),
so confidence intervals resample whole DAYS (block bootstrap), not trades.

P&L units: % of entry price per trade, equal notional per trade. The daily
series is the sum of that day's trade returns (0 on days with no trade), i.e.
returns on one position-sized unit of capital per trade.
"""

import math

import numpy as np
import pandas as pd
from scipy import stats as sps

TRADING_DAYS = 252


def _norm_ppf(p):
    return sps.norm.ppf(p)


def block_bootstrap_ci(trades, reps=5000, alpha=0.05, seed=7):
    """95% CI for mean P&L per trade, resampling trading days."""
    by_day = [g.to_numpy(float) for _, g in trades.groupby("date")["pnl_%"]]
    if len(by_day) < 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    n = len(by_day)
    sums = np.array([d.sum() for d in by_day])
    counts = np.array([len(d) for d in by_day])
    idx = rng.integers(0, n, size=(reps, n))
    means = sums[idx].sum(axis=1) / counts[idx].sum(axis=1)
    return (float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2)))


def daily_series(trades, sessions):
    s = trades.groupby("date")["pnl_%"].sum()
    return s.reindex(sorted(sessions), fill_value=0.0)


def max_drawdown(series):
    eq = series.cumsum().to_numpy()
    if len(eq) == 0:
        return 0.0
    peak = np.maximum.accumulate(np.concatenate([[0.0], eq]))[1:]
    return float((eq - peak).min())


def deflated_sharpe(daily, n_trials):
    """Deflated Sharpe Ratio (Bailey & Lopez de Prado, 2014).

    Probability that the true Sharpe exceeds the best Sharpe you'd expect from
    `n_trials` unskilled strategies. The variance of the Sharpe estimate across
    trials isn't observed, so the null variance 1/T is used for it.
    """
    x = np.asarray(daily, float)
    T = len(x)
    if T < 30 or x.std(ddof=1) == 0:
        return np.nan
    sr = x.mean() / x.std(ddof=1)                     # per-day Sharpe
    skew = sps.skew(x)
    kurt = sps.kurtosis(x, fisher=False)
    gamma = 0.5772156649
    v = 1.0 / T
    sr0 = math.sqrt(v) * ((1 - gamma) * _norm_ppf(1 - 1 / n_trials)
                          + gamma * _norm_ppf(1 - 1 / (n_trials * math.e)))
    denom = math.sqrt(max(1 - skew * sr + (kurt - 1) / 4 * sr ** 2, 1e-12))
    return float(sps.norm.cdf((sr - sr0) * math.sqrt(T - 1) / denom))


def summarize(trades, sessions, n_trials=None, costs=(0.0,)):
    """One row of statistics for a trade list over a set of sessions."""
    p = trades["pnl_%"].to_numpy(float)
    n = len(p)
    days = max(len(sessions), 1)
    out = {"trades": n, "per_day": n / days}

    if n == 0:
        return out

    wins, losses = p[p > 0], p[p <= 0]
    lo, hi = block_bootstrap_ci(trades)
    daily = daily_series(trades, sessions)
    sd = daily.std(ddof=1)

    out.update({
        "win_rate": (p > 0).mean() * 100,
        "mean": p.mean(),
        "median": float(np.median(p)),
        "ci_low": lo, "ci_high": hi,
        "t_stat": p.mean() / (p.std(ddof=1) / math.sqrt(n)) if n > 2 and p.std() > 0 else np.nan,
        "profit_factor": wins.sum() / abs(losses.sum()) if losses.sum() != 0 else np.nan,
        "total": p.sum(),
        "sharpe": daily.mean() / sd * math.sqrt(TRADING_DAYS) if sd > 0 else np.nan,
        "max_dd": max_drawdown(daily),
        "months_positive": f"{(trades.assign(m=trades['date'].str[:7]).groupby('m')['pnl_%'].sum() > 0).sum()}"
                           f"/{trades['date'].str[:7].nunique()}",
    })
    monthly = trades.assign(m=trades["date"].str[:7]).groupby("m")["pnl_%"].sum()
    if len(monthly) > 1 and p.sum() > 0:       # "share of the total" only means something if it's positive
        best = monthly.idxmax()
        rest = trades[trades["date"].str[:7] != best]["pnl_%"]
        out["best_month"] = best
        out["best_month_share"] = float(monthly.max() / p.sum() * 100)
        out["mean_ex_best_month"] = float(rest.mean()) if len(rest) else np.nan
    for c in costs:
        if c:
            out[f"mean_net_{c:.2f}"] = p.mean() - c
    if n_trials:
        out["deflated_sharpe"] = deflated_sharpe(daily, n_trials)
    return out


def permutation_vs_random(selected, pool, reps=2000, seed=11):
    """Does the model pick better than chance?

    For each day, draw as many trades at random from that day's full signal
    pool as the model selected, and compare mean P&L. Returns
    (mean of random draws, p-value = share of draws >= the model's mean).
    """
    rng = np.random.default_rng(seed)
    k = selected.groupby("date").size()
    pools = {d: pool.loc[pool["date"] == d, "pnl_%"].to_numpy(float) for d in k.index}
    target = selected["pnl_%"].mean()

    means = np.empty(reps)
    for r in range(reps):
        draws = [rng.choice(pools[d], size=min(n, len(pools[d])), replace=False)
                 for d, n in k.items() if len(pools[d])]
        means[r] = np.concatenate(draws).mean() if draws else np.nan
    return float(np.nanmean(means)), float((means >= target).mean())
