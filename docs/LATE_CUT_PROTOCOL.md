# v2 without entries at or after 15:00 — protocol

Written and committed on its own, before any tracking code (2026-10-07, evening, after the close).
The commit's timestamp is the pre-registration. Changes made after any live result has been seen
go under **Deviations**.

## The question

**Do live v2 GO entries at or after 15:00 IST make money after costs?**

The variant, `v2-late-cut`, is the champion (v2 @ 0.644) with every GO whose entry time is
15:00:00 IST or later dropped. Its trades are exactly the champion's GO trades entered before
15:00. The trades it removes are the **late slice**: champion GO trades with entry time ≥ 15:00
(entries at 15:00, 15:05 and 15:10; the last entry is 15:10). The entry time is the signal's
timestamp, the end of the candle that produced it.

## Why this isn't the standard variant test

The other shadow variants (`docs/V2_1_PROTOCOL.md`, `docs/THRESHOLD_PROTOCOL.md`) are judged by
mean P&L per trade against the champion. That test doesn't work here. `v2-late-cut` is a strict
subset of the champion's trades, and the late slice has a below-average mean in history
(`docs/LATE_ENTRY.md`). Removing below-average trades raises the mean per trade automatically,
whether or not the removed trades lose money. So the test is on the late slice itself: do those
trades make money after costs?

## Primary test

- **Statistic:** the mean P&L per trade of the live late slice, after a 0.05% round-trip cost.
- **Interval:** its 95% confidence interval from a day-block bootstrap (trading days resampled
  with replacement, 4,000 repetitions), the same method as the scorecard.
- **The cut is supported only if the whole 95% CI is below zero.** Otherwise it is not
  supported. If the whole CI is above zero, the late entries make money after costs. If the CI
  contains zero, the data can't tell.

## Secondary (reported, not decisive)

- The same mean and 95% CI at 0.10% cost.
- Total P&L (sum of per-trade %) of `v2-late-cut` and of the champion, at 0.05% and at 0.10%,
  over the same sessions.

## Sample floor

- **At least 50 late-slice GO trades over at least 15 distinct trading days**, counted on the
  late slice itself (days with at least one late GO trade), not on the champion's trades.
- **Only live trades after this commit count:** `source = live` signals from the first session
  after the commit, **2026-10-08**, onwards. Replayed sessions don't count.
- **History is context, not evidence.** The walk-forward numbers in `docs/LATE_ENTRY.md`
  (791 late trades, +0.021% after 0.05%) came from the data used to design v2, and led to this
  question.
- **When the verdict is read:** the status is reported every Friday after the close. The
  verdict is the one on the first Friday status at which the floor is met. Later weeks are
  reported, but they don't replace it. This prevents waiting for a favourable week.

## Fixed now

- The cutoff is **15:00:00 IST, inclusive**. It won't be re-tuned to 14:30, 15:05 or any other
  time later. A different cutoff would need a new protocol and fresh live data.
- Outcomes are the per-signal labels the scorecard already uses: exit rule `exit_v1`,
  `data/live/shadow_outcomes.csv`.

## What it decides

Nothing. The champion, its threshold (0.644), the frozen v2 baseline and the two existing shadow
variants (v2.1 and v2 @ 0.54) are unchanged by this protocol. A supported cut would be evidence
for the owner. Adopting it would change the entry window in `strategy_config.py`, which is
pre-registered and hashed, so it would need a new registration and the owner's approval.

## Deviations

(none yet)
