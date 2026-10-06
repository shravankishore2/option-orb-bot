# v3 candidate — test protocol

Written and committed **before** any v3 code was run (2026-10-06, branch
`research/v3-candidate`). Everything below is fixed in advance; any change made
after a result has been seen is recorded under **Deviations** at the end, with
the reason. Inputs: [`retest_entry_feature_matrix.xlsx`](retest_entry_feature_matrix.xlsx)
(feature ranking, retest additions, cautions).

## 0. What this can and cannot show

- Every session from 2021-01-01 to 2026-09-22 has already been examined while
  v1 and v2 were built. **All results here are therefore exploratory
  (post-hoc)**, however carefully the selection is nested. They can justify
  *tracking* a v3 candidate; they cannot confirm it.
- The **pre-registered results stay separate** and are reported unchanged:
  v1's clean test (2022-01 → 2023-08, −0.027% per trade) and v2's forward test
  (every session from 2026-09-23, the live scorecard).
- Confirmation for any v3 variant can only come from sessions after this
  protocol's date: live shadow tracking, then a new pre-registration, which
  needs the owner's approval. Nothing here changes the live bot, the champion
  model or the frozen v2 baseline.

## 1. Data and universe

- Candles: the local cache (`data/history/`, 2021-01-01 → 2026-09-22), offline
  (`ORBITAL_OFFLINE=1`). No new history is downloaded for the research.
- Universe: the **point-in-time** Nifty 200 proxy (`survivorship.py`,
  `data/nifty200_membership.csv`), exactly as RESULTS.md. A signal counts only
  if its stock was a member on that day.
- The NSE list refreshed on 2026-10-06 is today's constituents. Using it for
  2021–26 would be survivorship bias, so it is **not** used to build or select
  anything. It appears once, as a labelled sensitivity row (signals restricted
  to today's 200 names).

## 2. Held fixed for every variant

- Exit rule `exit_v1` (stop 1× opening-range width, trail 1×, no target, flat
  by 15:15), applied from each variant's own entry.
- Label `profit` (made money under the exit rule). Model: XGBoost with
  `strategy_config.MODEL_PARAMS`. Walk-forward: a fresh model before every
  calendar month, trained on all earlier months; GO threshold = 98th
  percentile of out-of-sample scores on the newest 15% of training sessions;
  no month is traded with fewer than 8,000 training signals.
- Costs: 0, 0.05% and 0.10% round trip, subtracted per trade.
- Test months: 2022-01 → 2026-09 (to 2026-09-22), identical for every variant,
  reported as clean_backward (2022-01 → 2023-08), development
  (2023-09 → 2026-09-04) and clean_forward (2026-09-05 → 09-22), plus all three
  combined.
- `strategy_config.py` is not edited (its hash is the v2 pre-registration).
  Research rule variants are parameters with defaults that reproduce today's
  rules exactly; a test asserts that.

## 3. Variants compared

| ID | Entry | Model / features |
|---|---|---|
| V0 | current rule | v2 as-is (the existing walk-forward) |
| V1 | new breakout rule (§4a) | v2 recipe, same 28 features |
| V2 | new breakout rule | v3: features chosen by §6 |
| V3 | retest entry (§4b) | v3: features chosen by §6 on retest data, incl. retest features |
| V4 | new breakout rule | none: take every signal |
| V4r | current rule | none: take every signal (reference) |

## 4. Entry rules

**a) New breakout rule.** As today (close of a completed 5-minute candle beyond
ORH × 1.001 / ORL × 0.999 and beyond Fibonacci R1 / S1; entry at that close,
timed at the candle's end; first BUY and first SELL per stock per session;
last entry 15:10) **without** the "moved ±1.8% from the previous close"
condition. Reported: signal counts per period under both rules, and how many
new-rule signals the old rule also produces (same stock, day, direction).

**b) Retest entry.** Defined numerically before any data is examined. For a
long (shorts mirrored, ORL for ORH):

1. The breakout bar *B* is the new-rule signal bar.
2. A retest bar *R* is the **first** bar with *B* < *R* ≤ *B* + 6 (30 minutes)
   whose **low ≤ ORH + 0.25 × ORB** (price comes back within a quarter of the
   range width of the edge) and whose **close > ORH** (it did not close back
   inside).
3. If any bar in (*B*, *R*] **closes ≤ ORH** first, the breakout failed and
   there is no entry.
4. The retest is confirmed only once *R* has **closed**. Entry = close of *R*,
   entry time = end of *R*; it must be ≤ 15:10. No feature uses any bar after *R*.
5. Every breakout is logged with its outcome: `retest`, `no_retest`
   (6 bars passed), `closed_inside`, `too_late`.
6. **Cost of waiting**: for every breakout, its breakout-entry P&L (from *B*)
   against its retest outcome (P&L from *R*, or nothing if no entry): missed
   winners, avoided losers, net per breakout.
7. Sensitivity, reported and counted as configurations but not used to choose:
   return band X ∈ {0.10, 0.25, 0.50} × ORB and window N ∈ {3, 6, 12} bars,
   take-every-retest P&L only (no model).

## 5. Features

Base: v2's 28 inputs. Additions:

- **Retest features** (retest variant only), computed at *R*'s close, long
  shown (mirrored for shorts):
  `retest_depth_norm` = (highest high in [*B*, *R*) − low of *R*) / ORB;
  `level_penetration` = max(0, ORH − low of *R*) / ORB;
  `retest_vol_ratio` = volume of *R* / volume of *B*;
  `retest_rejection` = (close − low) / (high − low) of *R* (1 = closed at its high);
  `bars_to_retest` = *R* − *B* in bars;
  `pre_retest_excursion` = (highest high in [*B*, *R*) − ORH) / ORB;
  `stop_dist_r` = (entry − low of *R*) / ATR(14).
- **Caution fixes** (cheap, from the Risks sheet), for V2 and V3:
  direction-normalised copies of the signed inputs that aren't already
  (`gap_dir`, `prev_close_vs_orb_dir`, `nifty_ret_dir`, `sector_ret_dir`), the
  previous-day level on the trade's side (`dist_pd_level_dir`: PDH for longs,
  PDL for shorts, signed in the trade's favour), and a minute-of-day volume
  baseline (`rel_vol_entry_mod`: the entry bar's volume ÷ the median volume of
  the same bar of the day over the prior 20 sessions, strictly before the day).
  Note: the code already measures `nifty_ret`, `sector_ret` and
  `move_from_open` from **today's open**, not the previous close, so the
  sheet's gap-contamination fix applies only to `gap_pct` and
  `prev_close_vs_orb`, which are about the gap by definition.
- Point-in-time rule as everywhere: only candles that closed before the entry.

**Feature groups** for selection (fixed now; groups may overlap):

| Group | Members |
|---|---|
| stock_id (test first) | entry_log, orb_range_abs |
| prev_close_vs_orb (test first) | prev_close_vs_orb |
| sheet 3/22/23 | breakout_strength, vwap_dist, move_from_open |
| sheet 8/9/1 | orb_range_abs, entry_log, orb_range_pct |
| bucket 3 (contextual) | sector_ret, breadth, nifty_or_pos, atr_pct, vol_trend, range_expansion, move_from_open |
| market | nifty_ret, aligned_with_nifty, breadth, nifty_or_pos |
| volume | rel_vol_or, rel_vol_entry, vol_trend |
| levels | dist_pdh, dist_pdl, level_touches, consec_bars, pos_in_day_range |
| fixes (additions) | the caution-fix features above |
| retest (additions, V3) | the 7 retest features |

## 6. Feature selection (inside training data only)

- Selection is redone **once per test year** (2022 … 2026) on data **before
  1 January of that year**; that feature set is used for every monthly retrain
  in that year. The test year is never seen by selection.
- Metric: 5-fold date-grouped CV (whole sessions per fold, sklearn
  `GroupKFold` on date, as `challenger.py`). In each fold the model is fit on
  4 folds and scores the 5th; the fold metric is the mean P&L per trade of
  the top 2% of scores in the held-out fold (the GO trades). The CV metric is
  the mean over folds, with its standard error. AUC is logged, not used.
- Steps, in this order:
  1. Start: base 28 + fixes (+ retest group for V3).
  2. Test first: drop `stock_id`, then `prev_close_vs_orb`.
  3. One round of drop-group ablation over the remaining groups (a group is
     dropped as a whole). Parsimony rule: a drop is **kept** if the CV metric
     falls by less than one standard error; additions (fixes, retest) are
     **kept** only if dropping them lowers the metric by at least one
     standard error. Contextual bucket 3 is a group like the others: it stays
     only if it helps.
  4. Grouped permutation importance (shuffle a whole group within the
     held-out fold) on the final set, for the report only.
- **Noise benchmark**: for each selection year, 30 random drops of a set of
  base features of the same size as the chosen drop, evaluated the same way;
  the chosen set's CV improvement is reported against that distribution.
- Every CV evaluation counts as one configuration; the total is reported.

## 7. Evaluation and report

Per variant and period: trades, trades per day, mean P&L per trade (gross;
after 0.05% and 0.10%), hit rate, worst trade, worst month (sum and mean),
day-block bootstrap 95% CI, and a permutation test against random picks from
the same daily pool. Plus: signal counts for §4a, the retest outcome table and
the cost of waiting for §4b, the selected features per year, the number of
configurations tried, and the sensitivity rows (§1, §4b-7).

**Selection noise on the final comparison**: White's reality check. The
day-level P&L differences of V1–V3 against V0 on the combined test months are
day-block bootstrapped (recentred), the maximum over variants taken each time,
and the observed best compared with that distribution (p-value).

## 8. Decision rule (fixed now)

A v3 variant **beats v2 out-of-sample** only if, on the combined test months,
all of these hold:

1. its mean P&L per trade after 0.05% cost is above V0's;
2. the reality-check p-value for the best variant is below 0.05;
3. without its best month it still beats V0 without V0's best month;
4. it has at least 100 GO trades.

If so, it is **shadow-tracked live** beside v2: scored on every live signal,
logged, shown on the scorecard, with no effect on what is sent. Switching
needs weeks of live evidence and the owner's approval. If not, the result is
recorded as negative and nothing is tracked.

## Deviations

(none yet)
