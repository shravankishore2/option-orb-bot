# v2.1 — protocol

Written and committed before the v2.1 model is trained or scored (2026-10-07). Changes made after
any result has been seen go under **Deviations**.

## What v2.1 is

v2's exact recipe with three inputs removed: `entry_log`, `orb_range_abs` and `prev_close_vs_orb`
(25 inputs instead of 28).

- **Why these three:** the v3 study (`docs/V3_RESULTS.md`, branch `research/v3-candidate`) dropped
  them in all ten of its feature selections. That study used design-period data, so this is a
  **post-hoc** idea. v2.1 is shadow-tracked; it decides nothing.
- **Recipe, unchanged from v2:** `walk_forward.load_dataset` (same signals, results and
  features); `walk_forward.fit_with_threshold` (XGBoost with `strategy_config.MODEL_PARAMS`,
  label `profit`, `scale_pos_weight` from the fit rows); fit on the oldest 85% of training
  sessions; threshold = the 98th percentile of scores on the newest 15%.
- **Training data:** the same rows as the live v2 model: every labelled signal from 2021-01-01
  through **2026-09-22** (v2's `trained_through`, 110,731 rows).
- **Check before use:** the same code with v2's 28 inputs must reproduce the live v2 model, with
  the same threshold (0.6441) and the same scores on the training rows. That proves the recipe is
  identical, and only then is v2.1 fitted.
- **Files:** `models/variants/v2.1.pkl` and `.json`, with its own threshold, feature list and the
  SHA-256 of this protocol. `strategy_config.py`, the champion and the frozen v2 baseline are not
  touched.

## History (exploratory, reported, not decisive)

v2.1 is walk-forward evaluated on the same months, costs and periods as `docs/V3_RESULTS.md`
(2022-01 → 2026-09-22; 0 / 0.05% / 0.10%) against v2 as-is. All of it is design-period data, so
it informs the decision but cannot settle it.

## Live shadow tracking

From its first deployment, every live signal is also scored by v2.1. Its GO is score ≥ v2.1's own
threshold, and its decision is logged beside the champion's (`data/live/shadow_variants.csv`).
Outcomes are the same per-signal labels the scorecard already uses (exit rule `exit_v1`), so a
variant's trades are the signals it would have taken. Only `source = live` signals count; replays
don't.

The scorecard shows, for the champion (v2 @ 0.644) and each variant: GO trade count, number of
distinct trading days, hit rate, mean P&L per trade at 0 / 0.05% / 0.10% costs, the worst day
(the lowest daily sum of GO P&L), and 95% day-block bootstrap confidence intervals.

## Decision rule (fixed now)

v2.1 replaces the champion only if all of these hold:

1. it has **at least 50 live GO trades over at least 15 distinct trading days**;
2. its mean P&L per trade **after 0.05% costs is above the champion's**, over the same live
   sessions;
3. the **95% day-block bootstrap confidence interval of that difference** (v2.1 minus champion,
   mean per trade after 0.05%) **excludes zero**;
4. the owner approves.

If the step-4 investigation (`docs/THRESHOLD_PROTOCOL.md`) finds a bug in live feature
computation, the bug is fixed, the models recalibrated, and every live count above restarts from
zero.

## Deviations

(none yet)

## Clarification (2026-10-07)

Added after the protocol was committed; the text above is unchanged. Condition 3 ("the 95%
day-block bootstrap confidence interval of that difference excludes zero") is met **only when the
whole 95% CI of (variant minus champion, mean per trade after 0.05%) is above zero**. An interval
wholly below zero also "excludes zero", but it means the variant is reliably worse, so it doesn't
meet the condition. When this note was written, variants had been tracked live for one session
(2026-10-07), far below the condition-1 floor. The scorecard and the Friday status line (`variant_status.conditions`) implement exactly
this, and `tests/test_variant_status.py` checks it.
