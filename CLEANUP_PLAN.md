# Repo cleanup plan

Phase 1 of the cleanup (2026-10-07): an inventory and a plan, nothing moved or deleted yet. Phase 2 applies it on
branch `chore/cleanup` after the owner approves.

## Inventory

- **115 tracked files.** No untracked files outside git-ignored paths.
- **What uses each file** was found by: Python imports between tracked modules; the VM's installed units
  (`/etc/systemd/system/orbital*`: `main.py --session`, `main.py --resnapshot`, `challenger.py`, `drift.py`,
  `variant_status.py`, waitress → `webapp:app`) and `/etc/logrotate.d/orbital`; the dashboard and guest view
  (`webapp.py`, `templates/`, `static/`); the test suite and CI; the doc generators (`model_report.py`,
  `late_entry_check.py`, `explain_model.py`, `report.py`); and every mention of the file's name in README and docs.
- **Never touched:** `data/` (including `data/live/`), the v2 / frozen-baseline and v2.1 model files, every
  protocol and results doc, `docs/INTERVIEW_PREP.md`, `config.ini`, token files, and everything git-ignored
  (including `archive/data/`, `models/walkforward/`, `logs/`).

## Owner decisions (2026-10-07, approved with changes)

1. `docs/change_report.html`: DELETE (git history and the local PDF are enough).
2. `shadow_backfill.py`: KEEP.
3. Telegram bot: obsolete, but **not removed in this cleanup**. Its pieces are read by more than the bot
   itself: the live bot's start-up (`main.py` refuses to run without a valid Telegram config unless
   `--dry-run`) and its Dhan-token failure alert both go through `notifier.py`, and the dashboard's live view
   reads the Telegram sent log (`sent_notifications.csv`) as a fallback. Removing it needs code changes in
   `main.py` and `webapp.py`; that's waiting on the owner.
4. Model files checked after the `.gitignore` change and after deploy (hashes on the VM).
5. No restructure of the root into packages now (in TASKS.md, after the late-October protocol decision).

## Counts

| Class | Files |
|---|---|
| KEEP | 98 |
| ARCHIVE | 5 |
| DELETE | 10 |
| NOT ORBITAL | 0 |
| UNSURE | 2 |

## Proposed layout

```
/                      live bot, dashboard, research pipeline (unchanged: imported by each other, the VM units
                       and the tests; moving them would change every unit and import for no gain)
deploy/                systemd units, Caddy, logrotate (unchanged)
docs/                  results, protocols, model docs, screenshots, figures (unchanged)
models/                orbital_model.* (v2, the frozen baseline and champion), variants/v2.1.* (unchanged)
templates/ static/     dashboard (unchanged)
tests/                 (unchanged)
research/              NEW: finished experiments, one folder each, each with a one-line README
  README.md            index: experiment → what it found → which doc reports it
  threshold_v2_1/      threshold_research.py
  exit_grace/          exit_grace_backtest.py
  late_entry/          late_entry_check.py
  v1_exploration/      experiments.py, sweep_exits.py
  v3/                  README only: the code changes live entry-rule files, so it stays on branch
                       research/v3-candidate (docs/V3_PROTOCOL.md, docs/V3_RESULTS.md there)
archive/               REMOVED from git (archive/data/ stays on disk, git-ignored)
```

Moved scripts get a two-line path shim so `python research/<x>/<script>.py` still imports the root modules;
their outputs keep writing to `docs/`. The AUC reconciliation stays inside `model_report.py` (it generates
docs/MODEL.md and is a pipeline step), so it's KEEP and listed in the research index only.

## DELETE

| File | Why |
|---|---|
| `archive/backtest_alpha.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/cross_reference.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/equity_curve.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/fetch_ohlc.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/models/orb_classifier.pkl` | the 4-Sep exploration's model; only archive/predict.py (deleted) loads it; regenerable by train_classifier.py |
| `archive/models/orb_features.pkl` | its feature list; same |
| `archive/plot.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/predict.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/temp.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |
| `archive/time_graph.py` | superseded pre-September code (CLAUDE.md: 'not maintained, don't import'); no imports, no unit, no test, no cited result; kept in git history |

## NOT ORBITAL

None found. Searched every tracked file for options-trading code (0DTE, option chains, strikes, Greeks,
implied volatility). The only hits are ORBITAL's own: `Lot_size.csv` (lot sizes in the Telegram message) and
`experiments.py`'s option-payoff experiment from the 4-Sep exploration.

## UNSURE

| File | Why |
|---|---|
| `docs/change_report.html` | → DELETE (owner, 2026-10-07): one-off 'ORBITAL Change Report' of 2026-09-29 |
| `shadow_backfill.py` | → KEEP (owner, 2026-10-07): the tool for replaying a missed session |

## ARCHIVE

| File | Moves to · what it found → where it's reported |
|---|---|
| `exit_grace_backtest.py` | research/exit_grace/exit_grace_backtest.py: trailing-stop grace period, not adopted → docs/EXIT_GRACE.md |
| `experiments.py` | research/v1_exploration/experiments.py: 4-Sep label/feature/option experiments → PROJECT_GUIDE.md §4–5 |
| `late_entry_check.py` | research/late_entry/late_entry_check.py: late entries and fill realism → docs/LATE_ENTRY.md |
| `sweep_exits.py` | research/v1_exploration/sweep_exits.py: 4-Sep exit-rule sweep → PROJECT_GUIDE.md §4 |
| `threshold_research.py` | research/threshold_v2_1/threshold_research.py: threshold sweep, live GO-rate and skew check → docs/THRESHOLD_RESULTS.md |

## KEEP

| File | Used by |
|---|---|
| `.github/workflows/tests.yml` | CI: runs the test suite (incl. signals-only) on every push |
| `.gitignore` | docs |
| `CLAUDE.md` | agent instructions |
| `LICENCE.md` | the licence |
| `Lot_size.csv` | main.py reads it for the Telegram lot size |
| `PROJECT_GUIDE.md` | docs |
| `README.md` | docs |
| `auth.py` | imported by code, deploy/CI, tests, docs |
| `baselines.py` | imported by code, docs |
| `build_historical_signals.py` | imported by code, tests, referenced in code, docs |
| `challenger.py` | imported by code, VM unit, deploy/CI, tests, referenced in code, docs |
| `charts.py` | imported by code, docs |
| `config.example.ini` | docs |
| `deploy/Caddyfile.orbital` | docs |
| `deploy/logrotate.orbital` | installed on the VM as /etc/logrotate.d/orbital |
| `deploy/orbital-challenger.service` | VM unit, deploy/CI |
| `deploy/orbital-challenger.timer` | VM unit, referenced in code |
| `deploy/orbital-drift.service` | VM unit, deploy/CI |
| `deploy/orbital-drift.timer` | VM unit, referenced in code |
| `deploy/orbital-snapshot.service` | VM unit, deploy/CI |
| `deploy/orbital-snapshot.timer` | installed on the VM (08:50 pre-open snapshot) |
| `deploy/orbital-variant-status.service` | VM unit, deploy/CI |
| `deploy/orbital-variant-status.timer` | VM unit, referenced in code |
| `deploy/orbital-web.service` | VM unit, docs |
| `deploy/orbital.service` | VM unit, deploy/CI, docs |
| `deploy/orbital.timer` | VM unit, deploy/CI, docs |
| `dhan_client.py` | imported by code, tests, docs |
| `docs/EXIT_GRACE.md` | tests, referenced in code, docs |
| `docs/LATE_CUT_PROTOCOL.md` | dashboard, tests, referenced in code, docs |
| `docs/LATE_ENTRY.md` | referenced in code, docs |
| `docs/LIVE_EDGE_CASES.md` | docs |
| `docs/MODEL.md` | dashboard, referenced in code, docs |
| `docs/MODEL_SHAP.md` | dashboard, referenced in code, docs |
| `docs/PREREGISTRATION.md` | dashboard, referenced in code, docs |
| `docs/RESULTS.md` | deploy/CI, dashboard, tests, referenced in code, docs |
| `docs/THRESHOLD_PROTOCOL.md` | dashboard, referenced in code, docs |
| `docs/THRESHOLD_RESULTS.md` | deploy/CI, tests, referenced in code, docs |
| `docs/V2_1_PROTOCOL.md` | dashboard, referenced in code, docs |
| `docs/figures/threshold_sweep.png` | referenced in code, docs |
| `docs/prereg/strategy_config_v1.py` | referenced in code, docs |
| `docs/screenshots/expanded-row.png` | docs |
| `docs/screenshots/live-scanner.png` | docs |
| `docs/screenshots/scorecard.png` | docs |
| `docs/summary.json` | referenced in code, docs |
| `drift.py` | imported by code, VM unit, deploy/CI, tests, referenced in code, docs |
| `exits.py` | imported by code, tests, referenced in code, docs |
| `explain_model.py` | referenced in code, docs |
| `features.py` | imported by code, tests, referenced in code, docs |
| `fetch_symbols.py` | imported by code, tests, referenced in code, docs |
| `guest.py` | deploy/CI, tests, referenced in code, docs |
| `history_cache.py` | imported by code, tests, docs |
| `live_engine.py` | imported by code, tests, referenced in code, docs |
| `main.py` | VM unit, deploy/CI, dashboard, tests, referenced in code, docs |
| `mine_features.py` | imported by code, referenced in code, docs |
| `model_report.py` | referenced in code, docs |
| `models/orbital_model.json` | referenced in code |
| `models/orbital_model.pkl` | referenced in code, docs |
| `models/variants/v2.1.json` | referenced in code |
| `models/variants/v2.1.pkl` | referenced in code, docs |
| `notifier.py` | imported by code, tests, docs |
| `paper_trade.py` | referenced in code, docs |
| `registry.py` | imported by code, tests, docs |
| `report.py` | referenced in code, docs |
| `requirements.txt` | deploy/CI, referenced in code, docs |
| `run_pipeline.py` | dashboard, referenced in code, docs |
| `shadow.py` | imported by code, tests, docs |
| `signal_generator.py` | imported by code, tests, docs |
| `start.sh` | deploy/CI, docs |
| `static/figures/shap_beeswarm.png` | referenced in code, docs |
| `static/figures/shap_global.png` | referenced in code, docs |
| `static/live.js` | dashboard, tests |
| `static/style.css` | dashboard, tests, docs |
| `static/tracker.js` | dashboard, tests, referenced in code, docs |
| `stats.py` | imported by code, tests, docs |
| `strategy_config.py` | imported by code, dashboard, tests, docs |
| `survivorship.py` | imported by code, referenced in code, docs |
| `templates/dashboard.html` | referenced in code, docs |
| `templates/live_fragment.html` | dashboard, referenced in code |
| `templates/login.html` | referenced in code |
| `tests/conftest.py` | pytest fixtures |
| `tests/test_challenger.py` | test file |
| `tests/test_dashboard_login.py` | test file |
| `tests/test_dashboard_tracker.py` | test file |
| `tests/test_drift.py` | test file |
| `tests/test_exits.py` | test file |
| `tests/test_features.py` | test file |
| `tests/test_guest.py` | test file |
| `tests/test_infrastructure.py` | test file |
| `tests/test_live_engine.py` | test file |
| `tests/test_regression_real_data.py` | test file |
| `tests/test_rules_and_replay.py` | test file |
| `tests/test_shadow.py` | test file |
| `tests/test_signals_only.py` | test file |
| `tests/test_variant_status.py` | test file |
| `train_classifier.py` | v1 research, but imported by model_report.py (AUC reconciliation) and tests/test_features.py |
| `variant_status.py` | imported by code, VM unit, deploy/CI, tests, docs |
| `walk_forward.py` | imported by code, tests, referenced in code, docs |
| `webapp.py` | tests, referenced in code, docs |

## Phase 2 follow-ups (after approval)

- Update imports and paths: `research/v1_exploration/experiments.py` imports `sweep_exits` beside it;
  `train_classifier.py`'s model output moves from `archive/models/` to a git-ignored path.
- Fix every README, PROJECT_GUIDE, CLAUDE.md and doc mention of a moved or deleted path.
- No systemd unit references any moved or deleted file, so the units don't change.
- `.gitignore`: `archive/models/`, `research/**/output/`, `*.pkl` except the tracked model files, `docs/*.html`.
- Verify: full test suite (incl. signals-only); the real-data replay (`tests/test_regression_real_data.py`,
  28/28 live signals of 2026-08-17); `model_report.py` and `late_entry_check.py` regenerate docs/MODEL.md and
  docs/LATE_ENTRY.md with no diff; the dashboard starts and every page loads; grep finds no stale path.
