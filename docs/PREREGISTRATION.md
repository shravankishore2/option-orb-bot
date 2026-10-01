# Pre-registration

Two registrations. Each freezes `strategy_config.py` and records its SHA-256
**before** the data it will be judged on was examined. Verify with
`shasum -a 256 strategy_config.py` (current = v2) or the archived v1 copy.

---

## v1 — FAILED its clean test

**Frozen:** 2026-09-23, 01:40 IST
**Exact file:** [`docs/prereg/strategy_config_v1.py`](prereg/strategy_config_v1.py)
**SHA-256:** `3f57b1adf28283308b9da07021123ecf395fd7d113df83a465df95fa770ac80f`

Every design choice — label (`hit_1_5r`), the 28 features, the exit rule
(stop 1.0×ORB / trail 1.0×ORB / no target), the threshold method (98th
percentile of out-of-sample calibration scores), monthly retraining — was made
while looking at the development period, 2023-09-05 → 2026-09-04.

| Clean period | Dates | Why clean |
|---|---|---|
| Backward | 2022-01-01 → 2023-08-31 | Never downloaded before the freeze |
| Forward | 2026-09-05 → 2026-09-22 | Sessions after development ended |

Two data-cleaning fixes were applied after the freeze but before any result was
seen, neither touching a strategy parameter: candles outside 09:15–15:30 are
dropped (pre-open auction, Muhurat sessions, stray index stamps), and ETFs are
excluded from the point-in-time universe.

**Outcome (reported as it came out):** on the backward clean period the model's
GO trades averaged **−0.027% per trade** (95% CI −0.082% to +0.032%), and random
picks from the same daily signal pool did as well or better 83% of the time
(p = 0.83). The model *did* predict its own label out of sample (AUC 0.64) —
but trades scoring higher on `hit_1_5r` made **less** money under the exit
rule. The label was a proxy that pointed the wrong way. See docs/RESULTS.md.

---

## v2 — registered, awaiting its clean test

**Frozen:** 2026-09-23, 02:41 IST
**File:** `strategy_config.py`
**SHA-256:** `0c3e045fcf94b9aa7a7e4f98e70a75b8d9879c1f12cb5e8f1f7e13165c2f4395`

**What changed from v1:** only the label — `profit`, "the trade made money
under the exit rule", i.e. the quantity the strategy is actually judged on.
Features, exit, universe, threshold method and retraining are identical.

**Why it isn't claimed as tested yet.** v2 was chosen *after* v1 failed, using
a diagnosis made on the 2022-23 data. That period is therefore no longer clean
for v2. Its numbers there are reported as **post-hoc** only:

| Period | v2 GO trades | Mean/trade | 95% CI | vs random picks |
|---|---|---|---|---|
| Development 2023-09 → 2026-08 (design period) | 1,202 | +0.186% | +0.044% to +0.398% | p = 0.002 |
| 2022-01 → 2023-08 (post-hoc, one look) | 647 | +0.115% | −0.011% to +0.270% | p = 0.10 |

**v2's clean test = every session from 2026-09-23 onwards**, scored by the live
bot (`live_decisions.csv`) or, equivalently, `paper_trade.py` run after the fact.

**Success criteria, fixed now:** after at least **150 GO trades**,
1. mean P&L per GO trade > 0 with a day-block bootstrap 95% CI excluding zero, and
2. permutation p < 0.05 against random picks from the same daily signal pool.

At ~1.6 trades a day that is roughly **4–5 months** of sessions. Until then v2 is
"promising, unproven".

**Rule:** if any parameter in `strategy_config.py` changes before the criteria
are evaluated, the forward result is relabelled in-sample and a v3 registration
is needed.

**Note added 2026-10-02 (no parameter changed).** From October 2026 the model whose
GO decisions are *sent* can be replaced by a monthly challenger (`challenger.py`).
The v2 forward test is unaffected: the frozen v2 model keeps scoring **every** live
signal, and its own decisions (`baseline_score`, `baseline_go` in
`data/live/shadow_signals.csv`, with each signal's outcome in
`shadow_outcomes.csv`) are what the criteria above are evaluated on.
