# GO threshold — protocol

Written and committed before any of the analyses below are run (2026-10-07). Changes made after
any result has been seen go under **Deviations**. The champion (v2 @ 0.644) and the frozen v2
baseline don't change without the owner's approval.

## Questions

1. **Is the live GO rate wrong?** v2 was designed so that about 2% of signals score above its
   threshold. Live it looks lower (4 of 380 signals, 1.05%, in the decision log for 2026-09-30 →
   10-06). Is that chance, a different mix of signals (time of day, market), or a bug in how the
   live bot computes features?
2. **Does a lower threshold raise TOTAL profit after costs, or only the number of trades?**

## Step 4: live vs history, and training/serving skew

**Data:**
- Live: every shadow-logged signal since 2026-09-23 (features as the bot computed them). Live
  sessions (`source = live`, from 2026-10-05) and sessions replayed through the live engine
  (`replay`, 2026-09-23 → 10-01) are reported separately.
- The decision log (`live_decisions.csv`) adds the live scores for 2026-09-30 and 10-01.
- History: v2's calibration window (2025-11-13 → 2026-09-22). Those signals are out-of-sample for
  v2, and this window set its threshold.

**Analyses:**
1. **GO rate by time of day.** Signals are bucketed by entry time into 30-minute bins. The GO
   rate expected under the live time-of-day mix is the history's GO rate per bin, weighted by the
   live signal counts. It is compared with the observed rate, with a binomial p-value and a
   Wilson 95% interval.
2. **Score distribution.** Live scores vs history, overall and within each time bin
   (two-sample KS, PSI, quantiles).
3. **Feature distributions.** For each of the 28 inputs, live vs history in the same time bins
   (KS and PSI), with the same random-window noise floor as `drift.py`.
4. **Skew.** Every live signal's features and score are recomputed offline from historical
   candles, with the research code (`build_historical_signals.replay_day` +
   `mine_features`-style features), and compared with what the bot logged. This covers live
   signals since 2026-10-05, and the decision-log signals of 2026-09-30 and 10-01, whose scores
   are compared because their features weren't logged. Mismatches are listed (feature, size,
   signal). The 2026-10-01 replay mismatch already recorded (68% same entry time) is explained
   here.

**Decision:** "live features differ" means any input that differs, on the same signal, by more
than rounding (1e-6 relative), outside the documented, by-design difference in universe (live:
today's list; history: the point-in-time proxy). If that happens, the cause is found and fixed on
a branch, and the models are recalibrated, **before any threshold change**; live counts then
restart from zero.

## Step 5: threshold sweep on history

- **Setup:** walk-forward exactly as `docs/V3_RESULTS.md`. Same signals and features as v2, a
  fresh model before every month from 2022-01 to 2026-09 (through 09-22), trained on all earlier
  months.
- **Top k% thresholds** (k = 1, 2, 3, 5, 10, 20, 50): each month's threshold is the (100 − k)th
  percentile of that month's model's scores on its calibration sessions (the newest 15% of its
  training sessions). It is chosen using only data from before the test month, exactly how v2's
  2% is set.
- **Fixed thresholds** (0.50, 0.54, 0.58, 0.60, 0.644): GO when that month's model scores at or
  above the number.
- **Integrity check:** k = 2 must reproduce v2's published walk-forward GO trades exactly.
- **Reported per setting**, for clean_backward, development, clean_forward and combined:
  - trade count and trades per day;
  - mean per trade at 0 / 0.05% / 0.10% costs;
  - **total P&L** (sum over trades) at each cost;
  - hit rate;
  - worst month (sum);
  - mean and total without the best month.
- **Plot:** total P&L after 0 / 0.05% / 0.10% costs against the share of signals taken
  (`docs/figures/threshold_sweep.png`).
- **The answer to question 2:** a lower threshold "increases total profit" at a given cost only
  if its combined total P&L at that cost is higher than at 0.644 (and at the 2% setting), and
  still higher without each setting's best month. Otherwise it only increases the trade count.

## Step 6: live shadow variants

From their first deployment, scored beside the champion (v2 @ 0.644) on every live signal, deciding
nothing:
- **v2.1**, per `docs/V2_1_PROTOCOL.md`;
- **v2 @ 0.54**: the same v2 model, GO when its score is at least 0.54.

The scorecard shows, for each and for the champion: trade count, distinct trading days, hit rate,
mean P&L per trade at 0 / 0.05% / 0.10% costs, the worst day, and 95% day-block bootstrap
confidence intervals. Only `source = live` signals from the deployment date count.

## Step 7: decision rules (fixed now)

- **v2.1 or v2 @ 0.54 replaces the current champion setup only if all of these hold:**
  1. at least **50 live GO trades over at least 15 distinct trading days**;
  2. its mean P&L per trade **after 0.05% costs is above the champion's** over the same live
     sessions;
  3. the **95% day-block bootstrap confidence interval of that difference** (variant minus
     champion, per trade after 0.05%) **excludes zero**;
  4. **for the threshold variant only:** the step-5 sweep agrees, meaning that at 0.05% cost the
     fixed 0.54 threshold gives a higher combined total P&L than 0.644, both with and without the
     best month;
  5. the owner approves.
- **If step 4 finds a feature bug:** fix it, recalibrate, and restart the live counts before
  anything else.

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
