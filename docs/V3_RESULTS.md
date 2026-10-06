# v3 candidate — results

Follows [`V3_PROTOCOL.md`](V3_PROTOCOL.md) (committed before any v3 code ran). **Exploratory: every session 2021-01 → 2026-09-22 was examined while v1 and v2 were built.** Nothing here changes the live bot, the champion or the frozen v2 baseline.

## Entry rule without the ±1.8% condition (§4a)

Integrity check: the replay re-derived the current rule's signals: 110,753 vs 110,753 published, 110,753 identical (same stock, day, direction and time).

| Period | Current rule | New rule | Also fired by the current rule | Extra signals |
|---|---|---|---|---|
| clean_backward (2022-01-01 → 2023-08-31) | 31,844 | 58,690 | 31,834 | +26,856 (1.8×) |
| development (2023-09-05 → 2026-09-04) | 57,278 | 107,647 | 57,267 | +50,380 (1.9×) |
| clean_forward (2026-09-05 → 2026-09-22) | 703 | 1,530 | 703 | +827 (2.2×) |
| combined (2022-01-01 → 2026-09-22) | 89,977 | 168,149 | 89,956 | +78,193 (1.9×) |
| all history (2021-01-01 → 2026-09-22) | 110,753 | 201,749 | 110,731 | +91,018 (1.8×) |

## Comparison (§7) — identical walk-forward months and costs

P&L per trade, gross; the net columns subtract a flat round trip. Worst month = lowest monthly sum / mean of the variant's trades. *p (random)*: share of random picks from the same daily pool that did at least as well.

### clean_backward (2022-01-01 → 2023-08-31)

| Variant | Trades | /day | Mean | after 0.05% | after 0.10% | Hit | Worst trade | Worst month (sum / mean) | 95% CI | Random picks | p (random) | Without best month |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V0 v2 as-is | 647 | 1.57 | +0.115% | +0.065% | +0.015% | 51.5% | -3.86% | -9.0% / -0.333% | [-0.011%, +0.270%] | +0.065% | 0.104 | +0.052% (2022-02) |
| V1 v2 recipe, new rule | 1,182 | 2.87 | +0.151% | +0.101% | +0.051% | 52.3% | -3.86% | -9.4% / -0.111% | [+0.057%, +0.261%] | +0.034% | 0.000 | +0.104% (2022-02) |
| V2 v3 breakout | 1,168 | 2.83 | +0.088% | +0.038% | -0.012% | 50.6% | -3.86% | -28.3% / -0.346% | [+0.003%, +0.172%] | +0.025% | 0.011 | +0.059% (2022-02) |
| V3 v3 retest | 626 | 1.52 | +0.083% | +0.033% | -0.017% | 53.0% | -3.83% | -5.8% / -0.345% | [-0.013%, +0.179%] | +0.031% | 0.073 | +0.060% (2022-10) |
| V4 every signal, new rule | 58,690 | 142.45 | +0.039% | -0.011% | -0.061% | 41.0% | -6.59% | -70.6% / -0.024% | [+0.020%, +0.060%] | — | — | +0.035% (2022-12) |
| V4r every signal, current rule | 31,834 | 77.27 | +0.054% | +0.004% | -0.046% | 41.7% | -6.59% | -93.3% / -0.047% | [+0.027%, +0.083%] | — | — | +0.047% (2022-12) |

### development (2023-09-05 → 2026-09-04)

| Variant | Trades | /day | Mean | after 0.05% | after 0.10% | Hit | Worst trade | Worst month (sum / mean) | 95% CI | Random picks | p (random) | Without best month |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V0 v2 as-is | 1,202 | 1.63 | +0.186% | +0.136% | +0.086% | 54.1% | -4.37% | -13.0% / -0.304% | [+0.045%, +0.406%] | +0.088% | 0.004 | +0.102% (2024-06) |
| V1 v2 recipe, new rule | 2,274 | 3.09 | +0.120% | +0.070% | +0.020% | 53.3% | -6.19% | -15.4% / -0.312% | [+0.041%, +0.217%] | +0.058% | 0.001 | +0.091% (2024-06) |
| V2 v3 breakout | 2,210 | 3.00 | +0.260% | +0.210% | +0.160% | 52.7% | -6.84% | -43.9% / -0.457% | [-0.014%, +0.681%] | +0.181% | 0.000 | +0.066% (2024-06) |
| V3 v3 retest | 1,136 | 1.54 | +0.208% | +0.158% | +0.108% | 54.2% | -7.36% | -13.1% / -0.772% | [-0.017%, +0.559%] | +0.128% | 0.005 | +0.078% (2024-06) |
| V4 every signal, new rule | 107,647 | 146.06 | +0.054% | +0.004% | -0.046% | 42.2% | -9.17% | -105.7% / -0.039% | [+0.033%, +0.076%] | — | — | +0.049% (2024-06) |
| V4r every signal, current rule | 57,267 | 77.70 | +0.072% | +0.022% | -0.028% | 43.4% | -9.17% | -55.0% / -0.037% | [+0.041%, +0.106%] | — | — | +0.062% (2024-06) |

### clean_forward (2026-09-05 → 2026-09-22)

| Variant | Trades | /day | Mean | after 0.05% | after 0.10% | Hit | Worst trade | Worst month (sum / mean) | 95% CI | Random picks | p (random) | Without best month |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V0 v2 as-is | 11 | 1.00 | +0.331% | +0.281% | +0.231% | 45.5% | -0.34% | +3.6% / +0.331% | [-0.107%, +1.239%] | +0.101% | 0.186 | — (2026-09) |
| V1 v2 recipe, new rule | 29 | 2.64 | +0.235% | +0.185% | +0.135% | 55.2% | -1.03% | +6.8% / +0.235% | [-0.012%, +0.681%] | +0.052% | 0.087 | — (2026-09) |
| V2 v3 breakout | 17 | 1.55 | -0.109% | -0.159% | -0.209% | 47.1% | -1.03% | -1.9% / -0.109% | [-0.295%, +0.070%] | -0.000% | 0.748 | — (2026-09) |
| V3 v3 retest | 14 | 1.27 | -0.125% | -0.175% | -0.225% | 42.9% | -1.72% | -1.8% / -0.125% | [-0.617%, +0.515%] | +0.030% | 0.758 | — (2026-09) |
| V4 every signal, new rule | 1,530 | 139.09 | +0.032% | -0.018% | -0.068% | 42.0% | -2.43% | +48.2% / +0.032% | [-0.137%, +0.231%] | — | — | — (2026-09) |
| V4r every signal, current rule | 703 | 63.91 | +0.086% | +0.036% | -0.014% | 45.5% | -2.43% | +60.5% / +0.086% | [-0.144%, +0.330%] | — | — | — (2026-09) |

### combined (2022-01-01 → 2026-09-22)

| Variant | Trades | /day | Mean | after 0.05% | after 0.10% | Hit | Worst trade | Worst month (sum / mean) | 95% CI | Random picks | p (random) | Without best month |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V0 v2 as-is | 1,863 | 1.60 | +0.162% | +0.112% | +0.062% | 53.1% | -4.37% | -13.0% / -0.333% | [+0.054%, +0.311%] | +0.081% | 0.001 | +0.108% (2024-06) |
| V1 v2 recipe, new rule | 3,495 | 3.01 | +0.131% | +0.081% | +0.031% | 53.0% | -6.19% | -15.4% / -0.214% | [+0.070%, +0.206%] | +0.050% | 0.000 | +0.113% (2024-06) |
| V2 v3 breakout | 3,401 | 2.93 | +0.198% | +0.148% | +0.098% | 51.9% | -6.84% | -43.9% / -0.457% | [+0.015%, +0.481%] | +0.127% | 0.000 | +0.072% (2024-06) |
| V3 v3 retest | 1,776 | 1.53 | +0.161% | +0.111% | +0.061% | 53.7% | -7.36% | -13.1% / -0.772% | [+0.010%, +0.396%] | +0.093% | 0.003 | +0.078% (2024-06) |
| V4 every signal, new rule | 168,149 | 144.71 | +0.049% | -0.001% | -0.051% | 41.8% | -9.17% | -105.7% / -0.039% | [+0.034%, +0.065%] | — | — | +0.046% (2024-06) |
| V4r every signal, current rule | 89,956 | 77.41 | +0.066% | +0.016% | -0.034% | 42.8% | -9.17% | -93.3% / -0.047% | [+0.045%, +0.090%] | — | — | +0.060% (2024-06) |

## Pre-registered results (kept separate, unchanged)

- **v1, pre-registered clean test 2022-01 → 2023-08:** 676 trades, -0.027% per trade (the published −0.027%).
- **v2 forward test from 2026-09-23:** live and replayed sessions on the dashboard scorecard; not part of this study's data, and far too few GO trades to read yet.

## Selection noise and the decision rule (§7–8)

- Configurations tried: **416** (feature-selection CV evaluations across all years 405, V1 1, the retest sensitivity grid 9, the current-list sensitivity 1).
- White's reality check, V1–V3 vs V0, mean P&L per trade, combined months: best = **V2 v3 breakout** (+0.0366% per trade vs V0), **p = 0.503**. Differences: V1 v2 recipe, new rule -0.0301%, V2 v3 breakout +0.0366%, V3 v3 retest -0.0005%.

| Variant | 1. beats V0 after 0.05% | 2. reality-check p < 0.05 | 3. beats V0 without best months | 4. ≥ 100 GO trades | Beats v2? |
|---|---|---|---|---|---|
| V1 v2 recipe, new rule | no | no | yes | yes | **no** |
| V2 v3 breakout | yes | no | no | yes | **no** |
| V3 v3 retest | no | no | no | yes | **no** |

**Outcome:** no v3 variant beats v2 out-of-sample by the protocol rule; nothing is shadow-tracked.

## Decisions (owner, 2026-10-07)

- **Keep the ±1.8%-from-previous-close condition** and **breakout entries**. No retest entries.
- The one consistent finding (the stock-ID proxies and `prev_close_vs_orb` were dropped in all ten
  selections) goes forward as **v2.1**, under its own protocol (`docs/V2_1_PROTOCOL.md` on `main`),
  shadow-tracked only.

## Lesson for future protocols

**Cap the total loss the parsimony rule can accumulate, not just each step.** Here a drop was accepted
whenever it cost less than one standard error, so small losses added up: in 2025 the chosen breakout
set ended 0.045% per trade below the full set, more than one SE in total. A future protocol should
stop dropping once the **cumulative** loss against the starting set reaches one standard error (or
choose the smallest set within one SE of the best, the "one-SE rule"), and report the cumulative loss
for every selection.

## Reading the result

- **No v3 variant beats v2 by the pre-set rule**, so nothing is shadow-tracked. The best-looking one, the v3
  breakout model (V2: +0.198% per trade over 2022-01 → 2026-09 against v2's +0.162%), fails three of the four
  conditions: the reality check gives p = 0.50 (an edge that size is what picking the best of three variants
  produces by chance); without its best month it averages +0.072% against v2's +0.108%; its worst month is
  far worse (−43.9% summed, against −13.0%); and it lost money in the September 2026 sessions (−0.109%).
- **Dropping the ±1.8% condition** almost doubles the signals (+78,193 in the test months, 1.9×) and lowers the
  average signal (+0.049% against +0.066% per trade without a model). With v2's recipe on the larger pool (V1)
  the model takes about twice as many trades at a lower average (+0.131% against +0.162%).
- **Retest entries don't pay for the wait.** Half of all breakouts retest. The ones that never come back are
  the best breakouts (+0.213% if taken at the break), so waiting filters out the strongest moves. Per breakout,
  taking the break makes +0.049% and waiting makes +0.026%; the retest model (V3) matches v2's average with
  a worse worst trade. Across the sensitivity grid, retest entries average +0.04% to +0.05%, before costs.
- **Feature selection is mostly noise.** The chosen set changes from year to year (6 to 21 inputs) and beats
  random drops of the same size in only one of ten selections by a clear margin (2023 breakout: 97th
  percentile); the others sit between the 7th and 73rd. One result is consistent: the stock-ID proxies
  (`entry_log`, `orb_range_abs`) and `prev_close_vs_orb`, tested first, were dropped in all ten selections,
  so they add nothing out of sample.
- **A flaw in the protocol's own rule:** the parsimony rule accepts each drop that costs less than one standard
  error, and those costs add up. In 2025 the chosen breakout set ended 0.045% below where it started (more than
  one SE overall). A future protocol should cap the total loss, not each step. It was not changed here, since it
  was fixed before the run.
- **The 416 configurations tried** are why the reality check, not the best raw number, decides.

## Retest entry (§4b), test months, every breakout (no model)

Definition: within 6 bars, a bar whose low (high for shorts) comes within 0.25 × ORB of the edge and closes outside the range; a close back inside first kills the setup; entry at that bar's close once it has closed.

| Outcome | Breakouts | Share | Breakout-entry P&L (mean) | Retest-entry P&L (mean) |
|---|---|---|---|---|
| closed_inside | 19,129 | 11.4% | -0.326% | — |
| no_retest | 61,827 | 36.8% | +0.213% | — |
| retest | 86,428 | 51.4% | +0.015% | +0.051% |
| too_late | 765 | 0.5% | -0.073% | — |

**Cost of waiting** (per breakout, all test months):

- Breakouts with no retest entry: 81,721; had they been taken at the breakout: 37,358 winners missed (avg +0.735%), 43,542 losers avoided (avg -0.473%).
- Retested breakouts (86,428): breakout entry +0.015% vs retest entry +0.051% per trade.
- Net per breakout: take every breakout +0.049%; wait for the retest +0.026% (0 for breakouts not entered).

Sensitivity (reported, not used to choose): take-every-retest mean P&L per trade (entries)

| Band X (× ORB) | N = 3 bars | N = 6 bars | N = 12 bars |
|---|---|---|---|
| 0.1 | +0.047% (51,566) | +0.045% (60,246) | +0.042% (68,277) |
| 0.25 | +0.052% (79,213) | +0.051% (86,428) | +0.047% (93,399) |
| 0.5 | +0.053% (103,037) | +0.052% (107,978) | +0.051% (112,958) |

## Sensitivity: today's Nifty 200 only (survivorship-biased, not used to choose)

| Variant | Trades | Mean per trade |
|---|---|---|
| V0 v2 as-is | 1,335 | +0.141% |
| V1 v2 recipe, new rule | 2,597 | +0.116% |
| V2 v3 breakout | 2,524 | +0.175% |
| V3 v3 retest | 1,250 | +0.123% |
| V4 every signal, new rule | 125,658 | +0.041% |
| V4r every signal, current rule | 63,884 | +0.061% |

## Feature selection log (§6)

```
datasets: new-rule breakouts 201,749, retest entries 103,967 (2021-01 → 2026-09-22)
[V2 v3 breakout] selection for 2022 on data before 2022-01-01:
  start (34 inputs): CV +0.1315% ± 0.0340
  drop stock_id               CV +0.1127% (-0.0188) -> dropped
  drop prev_close_vs_orb      CV +0.1175% (+0.0047) -> dropped
  drop sheet 3/22/23          CV +0.1688% (+0.0513) -> dropped
  drop sheet 8/9/1            CV +0.1099% (-0.0589) -> kept
  drop bucket 3 (contextual)  CV +0.0806% (-0.0882) -> kept
  drop market                 CV +0.0891% (-0.0796) -> kept
  drop volume                 CV +0.1819% (+0.0131) -> dropped
  drop levels                 CV +0.1542% (-0.0277) -> dropped
  addition fixes              without it CV +0.1518% (-0.0024) -> dropped
  chosen 14 inputs: CV +0.1518% (start +0.1315); dropped base: breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, rel_vol_or, rel_vol_entry, vol_trend, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change +0.0204% vs 30 random drops of 14 base inputs (median +0.0344%, max +0.1386%) -> better than 43% of them
  grouped permutation importance: bucket 3 (contextual) +0.0573, market +0.0098, sheet 8/9/1 -0.0133
[V2 v3 breakout] selection for 2023 on data before 2023-01-01:
  start (34 inputs): CV +0.1818% ± 0.0503
  drop stock_id               CV +0.1983% (+0.0164) -> dropped
  drop prev_close_vs_orb      CV +0.1749% (-0.0233) -> dropped
  drop sheet 3/22/23          CV +0.1939% (+0.0190) -> dropped
  drop sheet 8/9/1            CV +0.1536% (-0.0403) -> dropped
  drop bucket 3 (contextual)  CV +0.2136% (+0.0600) -> dropped
  drop market                 CV +0.1663% (-0.0473) -> kept
  drop volume                 CV +0.1347% (-0.0789) -> kept
  drop levels                 CV +0.1855% (-0.0281) -> kept
  addition fixes              without it CV +0.1860% (-0.0276) -> kept
  chosen 21 inputs: CV +0.2136% (start +0.1818); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_or_pos, breadth, sector_ret, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open; added kept: gap_dir, prev_close_vs_orb_dir, nifty_ret_dir, sector_ret_dir, dist_pd_level_dir, rel_vol_entry_mod
  noise benchmark: chosen change +0.0318% vs 30 random drops of 13 base inputs (median -0.0025%, max +0.0445%) -> better than 97% of them
  grouped permutation importance: fixes +0.0835, market +0.0696, volume +0.0412, levels +0.0097
[V2 v3 breakout] selection for 2024 on data before 2024-01-01:
  start (34 inputs): CV +0.1050% ± 0.0419
  drop stock_id               CV +0.1361% (+0.0311) -> dropped
  drop prev_close_vs_orb      CV +0.1164% (-0.0197) -> dropped
  drop sheet 3/22/23          CV +0.1206% (+0.0042) -> dropped
  drop sheet 8/9/1            CV +0.1362% (+0.0156) -> dropped
  drop bucket 3 (contextual)  CV +0.1300% (-0.0063) -> dropped
  drop market                 CV +0.1215% (-0.0085) -> dropped
  drop volume                 CV +0.1053% (-0.0162) -> dropped
  drop levels                 CV +0.1038% (-0.0015) -> dropped
  addition fixes              without it CV +0.1398% (+0.0360) -> dropped
  chosen 6 inputs: CV +0.1398% (start +0.1050); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, sector_ret, rel_vol_or, rel_vol_entry, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change +0.0347% vs 30 random drops of 22 base inputs (median +0.0130%, max +0.0853%) -> better than 63% of them
  grouped permutation importance: 
[V2 v3 breakout] selection for 2025 on data before 2025-01-01:
  start (34 inputs): CV +0.1121% ± 0.0274
  drop stock_id               CV +0.1059% (-0.0061) -> dropped
  drop prev_close_vs_orb      CV +0.0904% (-0.0155) -> dropped
  drop sheet 3/22/23          CV +0.1399% (+0.0495) -> dropped
  drop sheet 8/9/1            CV +0.1349% (-0.0050) -> dropped
  drop bucket 3 (contextual)  CV +0.1083% (-0.0266) -> dropped
  drop market                 CV +0.0847% (-0.0237) -> kept
  drop volume                 CV +0.0994% (-0.0089) -> dropped
  drop levels                 CV +0.0828% (-0.0167) -> dropped
  addition fixes              without it CV +0.0674% (-0.0154) -> dropped
  chosen 8 inputs: CV +0.0674% (start +0.1121); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_or_pos, breadth, sector_ret, rel_vol_or, rel_vol_entry, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change -0.0447% vs 30 random drops of 20 base inputs (median +0.0324%, max +0.0994%) -> better than 7% of them
  grouped permutation importance: market -0.1458
[V2 v3 breakout] selection for 2026 on data before 2026-01-01:
  start (34 inputs): CV +0.1840% ± 0.0286
  drop stock_id               CV +0.1617% (-0.0223) -> dropped
  drop prev_close_vs_orb      CV +0.1864% (+0.0247) -> dropped
  drop sheet 3/22/23          CV +0.2408% (+0.0544) -> dropped
  drop sheet 8/9/1            CV +0.2003% (-0.0405) -> dropped
  drop bucket 3 (contextual)  CV +0.1103% (-0.0900) -> kept
  drop market                 CV +0.1257% (-0.0746) -> kept
  drop volume                 CV +0.2032% (+0.0029) -> dropped
  drop levels                 CV +0.1873% (-0.0159) -> dropped
  addition fixes              without it CV +0.1802% (-0.0071) -> dropped
  chosen 13 inputs: CV +0.1802% (start +0.1840); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, rel_vol_or, rel_vol_entry, vol_trend, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change -0.0038% vs 30 random drops of 15 base inputs (median +0.0256%, max +0.1017%) -> better than 33% of them
  grouped permutation importance: bucket 3 (contextual) +0.1018, market -0.0135
[V2 v3 breakout] features by year: 2022: 14; 2023: 21; 2024: 6; 2025: 8; 2026: 13
[V3 v3 retest] selection for 2022 on data before 2022-01-01:
  start (41 inputs): CV +0.1922% ± 0.0587
  drop stock_id               CV +0.1842% (-0.0080) -> dropped
  drop prev_close_vs_orb      CV +0.1744% (-0.0098) -> dropped
  drop sheet 3/22/23          CV +0.1650% (-0.0094) -> dropped
  drop sheet 8/9/1            CV +0.1804% (+0.0155) -> dropped
  drop bucket 3 (contextual)  CV +0.1448% (-0.0356) -> kept
  drop market                 CV +0.1944% (+0.0140) -> dropped
  drop volume                 CV +0.1742% (-0.0202) -> dropped
  drop levels                 CV +0.2517% (+0.0775) -> dropped
  addition fixes              without it CV +0.2572% (+0.0055) -> dropped
  addition retest             without it CV +0.2155% (-0.0417) -> dropped
  chosen 9 inputs: CV +0.2155% (start +0.1922); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, rel_vol_or, rel_vol_entry, vol_trend, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change +0.0233% vs 30 random drops of 19 base inputs (median -0.0041%, max +0.1764%) -> better than 73% of them
  grouped permutation importance: bucket 3 (contextual) +0.0825
[V3 v3 retest] selection for 2023 on data before 2023-01-01:
  start (41 inputs): CV +0.1669% ± 0.0295
  drop stock_id               CV +0.1895% (+0.0227) -> dropped
  drop prev_close_vs_orb      CV +0.1681% (-0.0215) -> dropped
  drop sheet 3/22/23          CV +0.1613% (-0.0068) -> dropped
  drop sheet 8/9/1            CV +0.1743% (+0.0130) -> dropped
  drop bucket 3 (contextual)  CV +0.1950% (+0.0207) -> dropped
  drop market                 CV +0.2221% (+0.0271) -> dropped
  drop volume                 CV +0.2503% (+0.0282) -> dropped
  drop levels                 CV +0.2211% (-0.0292) -> dropped
  addition fixes              without it CV +0.1889% (-0.0322) -> kept
  addition retest             without it CV +0.1875% (-0.0336) -> kept
  chosen 19 inputs: CV +0.2211% (start +0.1669); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, sector_ret, rel_vol_or, rel_vol_entry, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: gap_dir, prev_close_vs_orb_dir, nifty_ret_dir, sector_ret_dir, dist_pd_level_dir, rel_vol_entry_mod, retest_depth_norm, level_penetration, retest_vol_ratio, retest_rejection, bars_to_retest, pre_retest_excursion, stop_dist_r
  noise benchmark: chosen change +0.0542% vs 30 random drops of 22 base inputs (median +0.0427%, max +0.0874%) -> better than 60% of them
  grouped permutation importance: retest +0.0632, fixes +0.0231
[V3 v3 retest] selection for 2024 on data before 2024-01-01:
  start (41 inputs): CV +0.1196% ± 0.0501
  drop stock_id               CV +0.1234% (+0.0038) -> dropped
  drop prev_close_vs_orb      CV +0.1410% (+0.0176) -> dropped
  drop sheet 3/22/23          CV +0.1050% (-0.0360) -> kept
  drop sheet 8/9/1            CV +0.1391% (-0.0020) -> dropped
  drop bucket 3 (contextual)  CV +0.1645% (+0.0254) -> dropped
  drop market                 CV +0.1291% (-0.0354) -> dropped
  drop volume                 CV +0.1520% (+0.0229) -> dropped
  drop levels                 CV +0.1582% (+0.0062) -> dropped
  addition fixes              without it CV +0.1368% (-0.0214) -> dropped
  addition retest             without it CV +0.0885% (-0.0484) -> dropped
  chosen 8 inputs: CV +0.0885% (start +0.1196); dropped base: orb_range_pct, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, sector_ret, rel_vol_or, rel_vol_entry, vol_trend, atr_pct, range_expansion, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change -0.0311% vs 30 random drops of 20 base inputs (median +0.0201%, max +0.1786%) -> better than 17% of them
  grouped permutation importance: sheet 3/22/23 -0.0480
[V3 v3 retest] selection for 2025 on data before 2025-01-01:
  start (41 inputs): CV +0.1529% ± 0.0497
  drop stock_id               CV +0.1305% (-0.0224) -> dropped
  drop prev_close_vs_orb      CV +0.1343% (+0.0038) -> dropped
  drop sheet 3/22/23          CV +0.1155% (-0.0188) -> dropped
  drop sheet 8/9/1            CV +0.1119% (-0.0037) -> dropped
  drop bucket 3 (contextual)  CV +0.0819% (-0.0299) -> dropped
  drop market                 CV +0.1291% (+0.0471) -> dropped
  drop volume                 CV +0.0698% (-0.0592) -> kept
  drop levels                 CV +0.1168% (-0.0122) -> dropped
  addition fixes              without it CV +0.1648% (+0.0480) -> dropped
  addition retest             without it CV +0.1738% (+0.0090) -> dropped
  chosen 8 inputs: CV +0.1738% (start +0.1529); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, sector_ret, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change +0.0209% vs 30 random drops of 20 base inputs (median -0.0080%, max +0.1947%) -> better than 60% of them
  grouped permutation importance: volume +0.0243
[V3 v3 retest] selection for 2026 on data before 2026-01-01:
  start (41 inputs): CV +0.1784% ± 0.0172
  drop stock_id               CV +0.1769% (-0.0015) -> dropped
  drop prev_close_vs_orb      CV +0.1659% (-0.0111) -> dropped
  drop sheet 3/22/23          CV +0.1720% (+0.0061) -> dropped
  drop sheet 8/9/1            CV +0.1687% (-0.0033) -> dropped
  drop bucket 3 (contextual)  CV +0.1479% (-0.0208) -> dropped
  drop market                 CV +0.1371% (-0.0107) -> dropped
  drop volume                 CV +0.1269% (-0.0102) -> dropped
  drop levels                 CV +0.1292% (+0.0023) -> dropped
  addition fixes              without it CV +0.1691% (+0.0399) -> dropped
  addition retest             without it CV +0.1316% (-0.0375) -> dropped
  chosen 6 inputs: CV +0.1316% (start +0.1784); dropped base: orb_range_pct, breakout_strength, prev_close_vs_orb, orb_range_abs, entry_log, nifty_ret, nifty_or_pos, aligned_with_nifty, breadth, sector_ret, rel_vol_or, rel_vol_entry, vol_trend, atr_pct, range_expansion, vwap_dist, move_from_open, pos_in_day_range, dist_pdh, dist_pdl, level_touches, consec_bars; added kept: none
  noise benchmark: chosen change -0.0468% vs 30 random drops of 22 base inputs (median -0.0061%, max +0.1535%) -> better than 23% of them
  grouped permutation importance: 
[V3 v3 retest] features by year: 2022: 9; 2023: 19; 2024: 8; 2025: 8; 2026: 6
```

