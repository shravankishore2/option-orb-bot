# ORBITAL — Complete Project Guide

> Everything you need to explain this project cold: what it is, how it's built,
> what each file does, every problem we hit, how it was solved, what the numbers
> actually say, and how to defend it in a viva or interview.

Major work sessions: **4 September 2026** (data, first research) and
**23 September 2026** (live bot, survivorship, clean test, rigour, dashboard).

---

## 1. The one-minute pitch

ORBITAL is an intraday **Opening Range Breakout (ORB)** system for the Nifty 200.
After the 09:20–09:35 range forms, it checks every completed 5-minute candle for
a breakout that also clears a ±1.8% move from yesterday's close and a Fibonacci
pivot. Each signal is scored by an XGBoost model on 28 inputs; only the top ~2%
are sent to Telegram with a stop and trail.

The engineering is solid — the live bot and the backtest run literally the same
code, verified signal-for-signal. The research is the interesting part: I
**pre-registered** the model, tested it on two years of data I had never looked
at, and **it failed** — it predicted its training label well (AUC 0.64) but that
label pointed the wrong way for profit. The rule set itself has a small, real
edge out of sample. A second model with a corrected label looks better but is
concentrated in market-crash days, and is now in its own pre-registered forward
test. The project's value is an evaluation pipeline honest enough to catch all
of that.

---

## 2. Architecture

```
                         DhanHQ Data API
          (real-time quotes, 5-min + daily candles, session OHLC)
                                 │
                           dhan_client.py
        rate limit 4/s · retries · symbol→id · inclusive date ranges ·
        regular-session filter (09:15–15:30)
                                 │
          ┌──────────────────────┴─────────────────────────┐
          │                                                │
     LIVE PATH                                        RESEARCH PATH
          │                                                │
     main.py  (every 5 min, --once for CI)          history_cache.py  (disk cache,
          │                                          missing ranges only, offline mode)
     live_engine.py                                        │
       completed candles only                        survivorship.py  (point-in-time
          │                                          Nifty 200, validated 83%)
          ├──────────► build_historical_signals.replay_day ◄──────┤
          │                 (THE entry rules, shared)             │
          ├──────────► features.py  (THE 28 inputs, shared) ◄─────┤ mine_features.py
          ├──────────► exits.py     (THE exit rule, shared) ◄─────┤
          │                                                       │
     model (models/orbital_model.pkl) ◄── walk_forward.py (monthly retrain)
          │                                                       │
     notifier.py → Telegram                     report.py · stats.py · baselines.py
          │                                     paper_trade.py · explain_model.py
     live_decisions.csv ──► webapp.py (dashboard) ◄── data/research/summary.json
```

**The key design decision:** there is exactly one implementation of the entry
rules, the features and the exit rule, and both the live bot and the backtest
call them. That is why the paper test can show the live engine, run every 5
minutes through 11 real sessions, making **703 of 703** identical decisions to
the batch backtest.

---

## 3. File-by-file reference

| File | What it does |
|---|---|
| `strategy_config.py` | Every parameter. Frozen and hashed for pre-registration (v1, then v2) |
| `dhan_client.py` | Dhan API wrapper: LTP, 5-min + daily candles, session OHLC; rate limit, retries; **inclusive date ranges**; drops out-of-session candles |
| `history_cache.py` | Gzipped per-symbol cache in `data/history/`; fetches only missing ranges; exact tail; `ORBITAL_OFFLINE=1` never calls the API |
| `survivorship.py` | Rebuilds Nifty 200 membership at every Mar/Sep rebalance from 6-month median traded value (shares only, no ETFs); validates against two real NSE lists |
| `fetch_symbols.py` | Today's live universe from NSE's file; aliases (`LTIM`→`LTM`), placeholders, staleness warning |
| `signal_generator.py` | The entry rules (`evaluate()`) |
| `build_historical_signals.py` | `replay_day()` — first BUY / first SELL per day on completed candles; `prev_session()` by date; `--rules plain_orb` baseline |
| `features.py` | 9 geometry + 19 context inputs; `MarketContext` (breadth, sector, NIFTY); forward labels |
| `exits.py` | Stop 1×range, trail 1×range, no target, flat 15:15; stop checked first |
| `mine_features.py` | Features + labels for every historical signal (memory-light) |
| `walk_forward.py` | Retrain before every month, absolute threshold from calibration; `--train-live`; `--label` |
| `baselines.py` · `stats.py` · `report.py` | Random entry baseline; day-block bootstrap CI, Sharpe, max DD, deflated Sharpe, permutation test, best-month concentration → `docs/RESULTS.md` |
| `paper_trade.py` | Replays past sessions cycle-by-cycle through the live engine and diffs against the backtest |
| `explain_model.py` | Exact tree SHAP via XGBoost → figures + `docs/MODEL_SHAP.md` |
| `model_report.py` | Every model input, how it is computed, gain + permutation importance → `docs/MODEL.md` |
| `run_pipeline.py` | All of the above in order, resumable |
| `live_engine.py` | Live decisions; `DhanSource` / `CacheSource`; session-OHLC snapshot fallback |
| `main.py` | The bot: 5-min loop, `--once`, `--dry-run`, `--snapshot`; holiday / token / restart handling |
| `notifier.py` | Telegram (IST dates, plain text, env-var credentials) and the sent log |
| `webapp.py` · `charts.py` · `templates/dashboard.html` | Dashboard (server-rendered SVG charts) |
| `train_classifier.py` · `experiments.py` · `sweep_exits.py` | The 4-Sep exploration, on `data/research/v1/` |
| `tests/` | 56 tests |
| `archive/` | Superseded code and data |

Docs: `docs/RESULTS.md`, `docs/MODEL.md`, `docs/PREREGISTRATION.md`,
`docs/LIVE_EDGE_CASES.md`, `README.md`.

---

## 4. Concepts you must be able to explain

**Opening Range Breakout.** The first minutes set a high and low; a close
beyond them is a breakout. Here the range is 09:20–09:35 and all three
5-minute candles must exist.

**Fibonacci pivot.** From yesterday: `P = (H+L+C)/3`, `R1 = P + 0.382(H−L)`,
`S1 = P − 0.382(H−L)`. A confirmation filter.

**Lookahead bias.** Using information that wasn't available at decision time.
We found **six** (§6).

**Survivorship bias.** Backtesting on today's index members includes the
winners that got promoted and drops the losers that got removed. Fixed with a
point-in-time universe.

**Train/serve skew.** The live system computing inputs differently from
training. Fixed by one shared implementation, and tested.

**Walk-forward.** Retrain before each month; score only that month. The only
evaluation that matches what you'd have lived through.

**Pre-registration.** Freezing every choice (and hashing the file) *before*
looking at test data, so the result can't be tuned. Standard in medicine;
rare in trading projects.

**Permutation test vs random picks.** Each day, draw at random as many trades
from that day's signals as the model took. If random draws often do as well,
the model isn't choosing — it's just along for the ride.

**Deflated Sharpe ratio.** The Sharpe you'd expect from the *best* of N
unskilled strategies grows with N. The DSR asks whether yours beats that bar,
given ~200 variants tried.

**Proxy vs objective.** A model can predict its label perfectly and still lose
money if the label isn't the thing you're paid on. This project hit that twice.

**Block bootstrap.** Trades on the same day share one market; resample days,
not trades, or the confidence interval is too narrow.

---

## 5. The story, in order

### Phase 1 — Real-time data (4 Sep)

yfinance is ~15 minutes late. Built `dhan_client.py` against the live API after
probing it: timestamps, the 5 req/s limit (6th returns `DH-904`), the 90-day
intraday cap (`DH-905`), batched LTP. Fixed a prev-close off-by-one (Dhan's
daily table excludes today) and a silent stale-price bug.

### Phase 2 — A real training set (4 Sep)

Replayed the live rules over 3 years by *importing* the live functions; 28/28
live signals reproduced. Signals stamped at candle **close** (09:35 candle →
09:40), so exits start on the next bar.

### Phase 3 — The first broken label (4 Sep)

`exit_reason == "Target (Midpoint)"` had correlation 0.03 with making money:
the target was a fixed level a late entry could already be past. AUC 0.81,
economically worthless. Switched to evaluating P&L, not AUC.

### Phase 4 — Features and exits (4 Sep)

Every exit rule gave +0.03–0.07% gross → the entry has little edge. Mined 28
features. Caught two lookahead bugs in my own feature code (+0.303% → honest
+0.109%).

### Phase 5 — Live realism (4 Sep)

"Top 1%" used a quantile over the whole test period — lookahead in the
evaluation. Replaced by an absolute threshold. Monthly walk-forward: 6 of 13
months profitable.

### Phase 6 — Making the live bot the tested system (23 Sep)

The review found **the live bot never used the model** — it sent every signal.
Rebuilt it on `live_engine.py`, which runs the same `replay_day()`,
`features.py` and model as the backtest, on completed candles only.

Twenty live bugs fixed (full table in `docs/LIVE_EDGE_CASES.md`). The ones to
remember:

- **Partial opening range**: bot started 09:30, range ends 09:35 → wrong range all day.
- **Train/serve skew**: ATR and previous-day levels looked up by the day's own
  daily row, which never exists live → NaN live, real in training. Fixed with
  one date-based lookup; a test compares both views.
- **Dhan date quirks**, found only by running against the real API: the intraday
  endpoint omits the **newest** session unless `toDate` is past it (the bot would
  have seen zero candles and shut down as a "holiday"), and the **daily bar is
  published late**, so the previous close would silently be two days old. Fixed
  with inclusive ranges and a nightly snapshot of NSE's *official* closing OHLC
  (the last 5-minute candle differs from the official close by up to ~0.3%).
- **Stale universe**: the Nifty 200 file was 329 days old — 13 removed stocks
  still traded, 12 new ones missed; `LTIM` had become `LTM`.
- Telegram rejected `M&M` under HTML parse mode; CI had no Telegram credentials;
  the CI loop killed itself every 5 minutes; dates used the server clock.

### Phase 7 — Honest evaluation (23 Sep)

**Survivorship.** No historical constituent files are fetchable (Wayback was
down), so membership is rebuilt from Nifty's own mechanics: at each Mar/Sep
rebalance, the top 200 ordinary shares by 6-month *median* traded value.
Validated against the two real lists we have: **83.5% and 82.5% overlap**. Two
fixes found by inspecting misses: ETFs (NIFTYBEES, GOLDBEES…) had ranked in, and
the median beats the mean (82% vs 77%). Residual bias: it favours busy mid-caps
and misses large quiet stocks (LICI, NESTLEIND); merged companies (HDFC Ltd)
aren't in Dhan at all.

**Data cleaning.** Pre-open auction candles (09:07), Muhurat evening sessions
and stray NIFTY stamps (02:15) were becoming "the day's open". Everything outside
09:15–15:30 is now dropped.

**Pre-registration v1.** Config frozen and hashed at 01:40, before downloading
2021–23. Clean periods: 2022-01 → 2023-08 and 2026-09-05 → 09-22.

**Result: v1 failed.** Clean test: −0.027% per trade, CI [−0.082%, +0.032%],
random picks did as well 83% of the time. Diagnosis: out of sample the model
ranked its label well (AUC 0.64–0.71), but higher scores meant **lower** P&L. It
favoured narrow-range setups that hit "+1.5× range" easily yet made little money,
while most profit comes from trades that drift for hours and exit at 15:15 —
which the label ignored. The second proxy-label failure, one level deeper.

**v2 — label = "made money under the exit rule".** Chosen *after* v1 failed, so
2022-23 is post-hoc for it. Development: +0.186%/trade, beats random picks
p = 0.004. Post-hoc 2022-23: +0.115%, p = 0.10. But:

- **Concentration.** Feb-2022 (the Ukraine crash) is 61% of its 2022-23 P&L;
  without it v2 = the pool (+0.052%, p = 0.30). June-2024 (election-result
  crash) is 48% of its development P&L; without it, still p = 0.025.
- **Late entries.** 34–46% of its trades enter after 15:00 (vs 4% of all
  signals) — 5-to-15-minute trades costs would eat. Before 15:00 it is strong in
  development (+0.294%, p < 0.001) and directionally right in 2022-23
  (+0.119% vs +0.051%, p = 0.15).
- **Deflated Sharpe 0.01** after ~200 variants.

v2 is registered with a forward test: every session from 2026-09-23, success =
≥150 GO trades, CI excluding zero, permutation p < 0.05.

**What *is* robust out of sample:** the ORBITAL rule set beats plain ORB and
random entry in every period (+0.054% [+0.027, +0.083] on the clean test) — a
real but tiny gross edge, below realistic costs.

### Phase 8 — Proving live = backtest (23 Sep)

`paper_trade.py` replayed 11 real sessions through the live engine, one 5-minute
cycle at a time, with the month's walk-forward model: **703/703 signals, 100%
same entry time, 100% same GO/SKIP**, max score difference 5×10⁻⁵ (float32).
Then a full day against the **real Dhan API** found the date-range bug above —
the one thing the cache-based paper test couldn't. After the fix, the real-API
run of 22 Sep matched the backtest on every signal both saw (100% same time,
price and decision; same single GO). The 25 signals only one side saw are all
explained by the universe: live trades today's real Nifty 200, the backtest the
rebuilt point-in-time list (83% overlap).

---

## 6. Every lookahead / leak we hit

| # | Where | Bug | Fix |
|---|---|---|---|
| 1 | Signal time | Candle close used at candle start | Stamp at candle end |
| 2 | Features | `time <= entry` included the entry-bar candle | `time < entry` |
| 3 | Breadth | End-of-day closes | Per-stamp, last completed |
| 4 | Evaluation | Quantile over whole test period | Absolute threshold from calibration |
| 5 | Labels | Forward labels could become inputs | Whitelist + test |
| 6 | ATR | 14-day window included today's high/low | Sessions strictly before the day |

Plus the non-lookahead biases: survivorship (§7), train/serve skew, ETFs in the
universe, out-of-session candles.

---

## 7. The numbers (know these)

**Data:** 2021-01 → 2026-09, 389 stocks ever in the index, 5-min candles;
110,753 ORBITAL signals; 336,483 plain-ORB signals.

**Clean test 2022-01 → 2023-08 (417 sessions), mean per trade [95% CI]:**

| | Mean | CI |
|---|---|---|
| Random entry | ≈ +0.01% | around 0 |
| Plain ORB | +0.030% | [+0.013, +0.047] |
| ORBITAL rules | +0.054% | [+0.027, +0.083] |
| **Model v1 (pre-registered)** | **−0.027%** | [−0.082, +0.032], p = 0.83 |
| Model v2 (post-hoc) | +0.115% | [−0.011, +0.270], p = 0.10 |

**Development 2023-09 → 2026-09:** v2 +0.186% [+0.045, +0.406], p = 0.004,
28/37 months positive; v1 +0.075%, p = 0.37.

**Live = backtest:** 703/703 signals, 100% same decisions.

**Survivorship proxy:** 83.5% / 82.5% overlap with real lists.

**Costs:** at 0.05% round trip the rules' edge is gone; v2 keeps +0.07–0.14%
before its concentration caveats.

---

## 8. Viva / interview Q&A

**"Walk me through it."** §1, then the architecture, then: real-time data →
3-year replay → first broken label → features → live realism → live bot rebuilt
→ survivorship + pre-registered clean test → v1 failed → diagnosis → v2
registered.

**"Your model failed its test. Why is that a good project?"** Because the
evaluation was built to be able to fail. Most projects report the in-sample
number. Mine pre-registered, tested on untouched data, reported the failure,
diagnosed it (label anti-aligned with P&L), and registered a fix for a forward
test instead of re-tuning on the test set.

**"How do you know the live bot is the backtested system?"** Shared code, a unit
test that runs the engine every 5 minutes through a day and compares it with
the batch replay, and a paper test on 11 real sessions: 703/703 identical.

**"How did you handle survivorship bias?"** Rebuilt membership at every
rebalance from traded value; validated at 83% against two real lists; listed
what's left (quiet large caps, merged companies).

**"Why the permutation test and not just a t-test?"** The question isn't "is P&L
above zero" — the rules alone are above zero. It's "does the model choose better
than chance from the same signals on the same days".

**"What's the deflated Sharpe?"** A Sharpe ratio adjusted for having tried ~200
variants. v2's is 0.01 — a reason not to trust it yet.

**"Hardest bug?"** The Dhan date quirk: requesting "today to today" returns
nothing for the newest session. Every offline test passed; only a dry run
against the real API found it. It would have made the bot declare a holiday on
a trading day.

**"What would you do next?"** v3: stop entries at ~14:30 so every trade has
time to work, then a regime gate for crash days, costs, and real option-chain
data — each as a new pre-registration.

---

## 9. Practical notes

- Dhan access tokens last ~24 h. The bot alerts and exits on 401.
- Run `python main.py --snapshot` after 15:40 (CI does it at 16:10) so tomorrow's
  previous close is the official one even if Dhan's daily bar is late.
- Replace `data/ind_nifty200list.csv` after each rebalance (end Mar / Sep).
- Dhan's most recent sessions currently lack their last three 5-min candles
  (15:15–15:25); older days are complete. Recent trades exit at the 15:10 close.
- `archive/` holds superseded code; `data/research/v1/` the 4-Sep dataset.

## 10. How to run it

```bash
python -m pytest                       # 56 tests
python run_pipeline.py                 # everything (~2 h first time)
python run_pipeline.py --from report   # just the numbers
python paper_trade.py --start 2026-09-05 --end 2026-09-22
python main.py --dry-run               # live, no Telegram
python main.py --snapshot              # after the close
python webapp.py                       # http://127.0.0.1:5050
```
