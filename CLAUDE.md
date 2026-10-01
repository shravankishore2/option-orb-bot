# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

ORBITAL: an intraday Opening Range Breakout system for the Nifty 200 (B.Tech final-year project). Live bot + research pipeline over DhanHQ market data, with an XGBoost filter. See `README.md` (overview, results summary), `PROJECT_GUIDE.md` (full history and rationale), `docs/RESULTS.md`, `docs/PREREGISTRATION.md`, `docs/LIVE_EDGE_CASES.md`.

## Commands

Always use the project venv (`.venv/bin/python`, Python 3.13); the system Python lacks the dependencies. No linter/formatter is configured.

```bash
./start.sh [--dry-run] [--no-web]                               # bot + dashboard together; Ctrl+C stops both
.venv/bin/python -m pytest -q                                   # all tests (~7 s)
.venv/bin/python -m pytest tests/test_exits.py::test_forced_exit_at_1515   # one test
.venv/bin/python -m pytest -k prev_session                       # by keyword
.venv/bin/python run_pipeline.py                                 # rebuild all research outputs (~2 h first time)
.venv/bin/python run_pipeline.py --from report                   # resume from a step
.venv/bin/python run_pipeline.py --only paper                    # one step
.venv/bin/python main.py --dry-run [--once]                      # live bot alone, no Telegram (logs to live_decisions_dryrun.csv)
.venv/bin/python main.py --snapshot                              # store official session OHLC (run after 15:40 IST)
.venv/bin/python main.py --session                               # one session + closing snapshot, then exit (systemd)
.venv/bin/python fetch_symbols.py --refresh                      # download data/ind_nifty200list.csv + data/sectors.csv
.venv/bin/python challenger.py --dry-run                         # monthly champion/challenger, change nothing
.venv/bin/python shadow_backfill.py 2026-09-23 2026-10-01 --check   # replay sessions into the shadow log
.venv/bin/python webapp.py                                       # dashboard, http://127.0.0.1:5050
```

`tests/test_regression_real_data.py` skips unless `data/history/` (the candle cache) exists. Pipeline steps after `intraday` run with `ORBITAL_OFFLINE=1`, which makes `history_cache` serve only cached data and never call the API; set it yourself when running research scripts by hand.

Credentials: `config.ini` (git-ignored; template `config.example.ini`). Telegram under `[DEFAULT]`. Dhan either via `[dhan] token_file` (a JSON file another service refreshes — the VM mode; ORBITAL must never log in to Dhan itself) or a static `dhan_client_id / dhan_access_token`. Env vars `ORBITAL_DHAN_TOKEN_FILE`, `DHAN_ACCESS_TOKEN`, `DHAN_CLIENT_ID`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` override. An unusable token raises `DhanError` containing "credentials" (main.py matches that substring).

**Signals only.** `dhan_client.ALLOWED_PATH_PREFIXES` (`/charts/`, `/marketfeed/`) is an allowlist enforced in `_post` and in the HTTP session; `tests/test_signals_only.py` fails on any order endpoint. Never add a trading endpoint or broker SDK call.

**Dashboard login.** `auth.py` (same scheme as QuantRadar's): password from `ORBITAL_DASHBOARD_PASSWORD[_FILE]`; none set = open (local use), `ORBITAL_REQUIRE_LOGIN=1` refuses to start without one. `webapp.require_login` guards every endpoint except those in `OPEN_ENDPOINTS`.

**The repo is public.** Never commit `config.ini`, tokens, `data/`, logs, or trade/signal CSVs (see `.gitignore`). Aggregate results (`docs/RESULTS.md`, `docs/summary.json`) are fine. A Telegram token leaked here once (2025) and history had to be rewritten.

## Architecture

**One implementation, shared by live and backtest.** This is the central invariant — never duplicate this logic:
- Entry rules: `signal_generator.evaluate()` called by `build_historical_signals.replay_day()`. The live engine calls `replay_day()` itself.
- Model inputs: `features.py` (`geometry_features` = 9, `compute_features` = 19 context, `MarketContext` for breadth/sector/NIFTY). Used by `mine_features.py` (history) and `live_engine.py` (live).
- Exit rule: `exits.simulate()` — every P&L number in the project comes from it.
- Parity is tested (`test_live_cycles_match_the_batch_replay`) and checked on real data by `paper_trade.py`, which replays sessions cycle-by-cycle through `LiveEngine` with `CacheSource` and diffs against `data/research/walkforward.csv`.

**Data flow.** `dhan_client.py` (rate limit, retries, `regular_session()` filter) → `history_cache.py` (gzipped per-symbol cache in `data/history/`, fetches only missing ranges) → research: `survivorship.py` (point-in-time universe) → `build_historical_signals.py` → `mine_features.py` → `walk_forward.py` (monthly retrain; writes `models/walkforward/YYYY-MM.pkl` and, with `--train-live`, `models/orbital_model.pkl` + `.json`) → `report.py` (baselines, `stats.py`) → `docs/RESULTS.md` + `data/research/summary.json` (read by the dashboard). Live: `main.py` → `live_engine.LiveEngine` (`DhanSource`) → `notifier.py`; decisions logged to `live_decisions.csv`.

**Shadow tracking and model roles.** `shadow.py` logs every live signal (GO and NO-GO) at signal time to `data/live/shadow_signals.csv` (append-only: decision, both models' scores/versions, features as `x_*`), walks it with `exits.simulate` on the cycle's candles, and writes its label ONCE to `shadow_outcomes.csv` after the exit candle completes (`_label` asserts it). Training reads labels only via `shadow.labelled_rows()`. `registry.py`: the frozen **baseline** (v2, `models/orbital_model.pkl`, never replaced) and the **champion** (decides GO; `models/registry/champion.json`, absent = v2). `challenger.py` (monthly timer) is the only thing that changes the champion. `data/training/history.csv.gz` is the research training set exported at full precision (`challenger.py --export-history`) — refitting it reproduces v2 bit for bit; the VM has no `data/research/`.

**Universes differ by design.** Live uses today's real list (`data/ind_nifty200list.csv` via `fetch_symbols.get_symbols()`, with `ALIASES` such as LTIM→LTM). The backtest uses `data/nifty200_membership.csv`, a traded-value proxy rebuilt per rebalance (~83% overlap). Signal-set differences between live and backtest are expected to come only from this.

**Pre-registration.** `strategy_config.py` holds every strategy parameter; its SHA-256 is recorded in `docs/PREREGISTRATION.md` (v1 archived at `docs/prereg/strategy_config_v1.py`, v2 current and in a forward test from 2026-09-23). Changing any byte of `strategy_config.py` invalidates the v2 forward test and needs a new registration — confirm with the user first. The dashboard's About tab checks the hash.

## Point-in-time rules (lookahead has been found six times here)

- A 5-min candle stamped T covers T..T+5; a signal from it is timestamped T+5 and exits start on the next candle. Features use only candles with stamp **strictly before** the entry time.
- Previous session / ATR / prev-day high-low are looked up **by date, strictly before the day** (`build_historical_signals.prev_session`, `features.build_symbol_history`) — Dhan's daily table never contains the running session.
- `mine_features.py` writes forward-path labels (`features.LABELS`: `mfe_pct`, `hit_*`) into the same CSV as the features; the trainers whitelist `features.FEATURES`. Never let a label become a model input.
- Model thresholds are absolute (98th percentile of out-of-sample calibration scores), never quantiles over the test period.

## Dhan API quirks (verified against the live API)

- Intraday `toDate` omits the **most recent** session unless it is past it. `dhan_client.get_intraday` requests one day beyond and trims; any raw `_post("/charts/intraday", …)` call must do the same.
- Ranges with no candles (e.g. an unpublished daily bar) come back as HTTP 400 `DH-907`, not an empty list; `get_daily`/`get_intraday` convert that to an empty frame.
- The daily bar is published late (on 23 Sep, the 22 Sep bar was still missing at 15:10). Live prev-close falls back to `data/session_ohlc.csv` (official OHLC snapshot, `main.py --snapshot`), then to candle-derived values with a warning.
- Rate limit 5 req/s **per account** (limiter 4 alone; the VM config sets 3 plus `quiet_seconds = 57-4` because QuantRadar shares the account); intraday span ≤ 90 days per request.
- Data can include pre-open (09:07), Muhurat-session and stray index candles; `regular_session()` keeps 09:15–15:30 only.
- Recent sessions currently lack the 15:15–15:25 candles; older days are complete.
- `images.dhan.co` (instrument master) intermittently fails DNS on this machine; the client falls back to the cached `data/dhan_nse_equities.csv`.

## Repo notes

- `data/history/`, `data/research/`, `models/walkforward/`, `logs/` are git-ignored and regenerable. `data/research/v1/` holds the 4-Sep dataset used only by `train_classifier.py`, `experiments.py`, `sweep_exits.py`.
- `archive/` is superseded code/data, not maintained — don't import from it.
- Deployed on the Oracle VM (`ssh oracle`, `~/orbital`) next to QuantRadar (`~/newsalert`); units and Caddy site in `deploy/`. Commit only when asked.
- When waiting on a background process, poll its PID, not `pgrep -f <text>` — the waiting shell's own command line matches the text and the loop never ends.
