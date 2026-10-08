# v3a: protocol

Written and committed on its own, before the v3a model is trained and before any tracking code
(2026-10-08, 14:10 IST, during that day's session). Changes made after any live result has been
seen go under **Deviations**. The model files' hashes are appended under **Frozen model** once it
is trained, before any tracking code. That's a record of what was trained, not a change of rule.

## What v3a is

v2's model recipe with a **cost-aware label** and **25 inputs**.

- **Why:** in the walk-forward label study (`docs/V3_LABEL_RESULTS.md`, design-period data,
  exploratory), the classifier on "P&L > 0.10%" with 25 inputs was the closest of 24
  candidate/rate combinations to beating v2. Its mean per trade was higher at every selection rate
  (+0.015% at 2%) and so was its total after 0.10% (+170 vs +145 at 2%), and it depended less on
  June 2024. But no CI excluded zero, so **this is a post-hoc idea, and v3a decides nothing**.
- **Label:** 1 if the trade's P&L under exit rule exit_v1 is **above 0.10%**, else 0.
- **Inputs:** v2's 28 minus `entry_log`, `orb_range_abs` and `prev_close_vs_orb` (the same 25 as v2.1).
- **Recipe, exactly as in `research/v3_label/v3_label_research.py`** (`_split`, `_model("a", …)`):
  - XGBoost classifier with `strategy_config.MODEL_PARAMS`;
  - `scale_pos_weight` = negatives ÷ positives of the label in the fit rows;
  - missing inputs filled with 0;
  - the oldest 85% of training sessions fit the model, and the newest 15% are held out.
- **Training data:** every labelled signal from 2021-01-01 through **2026-09-22**, v2's
  `trained_through` (110,731 rows, the same as v2 and v2.1). Calibration sessions run from
  2025-11-13.
- **Threshold, frozen at training:** v3a's GO = score at or above the **98th percentile of its own
  scores on the held-out calibration sessions**. It's an absolute number, recorded in
  `models/variants/v3a.json`, never re-estimated during the test.
- **Files:** `models/variants/v3a.pkl` and `.json`, tracked in git, with the SHA-256 of this
  protocol in the JSON. `strategy_config.py`, the champion (v2 @ 0.644), the frozen v2 baseline and
  the existing variants (v2.1, v2 @ 0.54, v2-late-cut) are not touched.

## Live shadow tracking

- Every live signal is also scored by v3a. Its decision is logged beside the others in
  `data/live/shadow_variants.csv`, and it decides nothing.
- Scoring is isolated: if v3a fails to load, score or decide, the failure is logged and v3a is
  skipped for that signal or session. It can never change the champion's decisions, the session or
  another variant.
- Outcomes are the per-signal labels the scorecard already uses (exit_v1,
  `data/live/shadow_outcomes.csv`). v3a's trades are the signals it would have taken.
- **Only `source = live` signals from the first session after this commit count**, which is
  **2026-10-09**. The champion is compared over the same sessions.

## Decision rule (fixed now)

v3a replaces the champion only if all five hold:

1. it has **at least 50 live v3a GO trades over at least 15 distinct trading days**;
2. its **mean P&L per trade after 0.05% costs is above the champion's**, over the same live
   sessions;
3. the **95% day-block bootstrap CI of that difference** (v3a minus champion, mean per trade after
   0.05%) is **entirely above zero**;
4. **history agrees:** in `docs/V3_LABEL_RESULTS.md` (walk-forward, top 2%), the candidate's mean
   after 0.05% (+0.134%) is above v2's (+0.118%), and its total after 0.10% (+170.2) is above v2's
   (+145.0). This is fixed now as **met**. Its CI there (+0.015% [−0.068, +0.097]) includes zero,
   so the support from history is weak;
5. the owner approves.

**Secondary (reported, not decisive):** total P&L after 0.10% costs, v3a vs the champion, over the
same sessions.

**When the verdict is read:** the status is reported every Friday after the close. The verdict is
the one on the **first Friday status at which condition 1 is met**. Later weeks are reported but
don't replace it.

If a bug is found in live feature computation, it is fixed, the models are recalibrated, and the
live counts restart from zero.

## Frozen model

(appended after training, before any tracking code)

## Deviations

(none yet)
