# Live bot — bugs fixed and edge cases

Every case below was found by reading or running the live path. Each has a
handling rule and, where it can be tested offline, a test in `tests/`.

## Bugs fixed (2026-09-23)

| # | Bug | Effect | Fix | Test |
|---|---|---|---|---|
| 1 | The model was never used live — `main.py` sent every rule signal | ~60–70 alerts a day; the ML filter existed only in research | `live_engine.py` scores every signal with the trained model; only GO is sent | `test_go_skip_by_threshold` |
| 2 | Bot started at 09:30 but the opening range ends 09:35 — the first run cached a **partial** range for the whole day | Wrong ORH/ORL all day | Range needs all 3 candles; first decision at 09:40 | `test_partial_opening_range_is_rejected` |
| 3 | Live decisions used the tick price at poll time; the backtest used candle closes | Live ≠ tested system | Live evaluates **completed 5-min candles** with the replay's own `replay_day()` | `test_live_cycles_match_the_batch_replay` |
| 4 | A still-forming candle could be read as if closed | Lookahead-in-reverse: acting on a price that isn't final | `completed()` drops any candle whose end is after *now* | `test_forming_candle_is_dropped`, `test_no_signal_before_the_breakout_candle_closes` |
| 5 | Cache treated "within 5 days" as up to date | Live bot could use **the day-before-yesterday's close** for the ±1.8% and pivot filters | Cache tail is exact | `test_cache_tail_is_exact_but_head_tolerant` |
| 6 | Previous-session lookup was by row position; Dhan's daily table never contains today | Last day silently skipped in the replay; every day live | Lookup by date, strictly before the day | `test_prev_session_works_when_today_is_not_in_the_table` |
| 7 | ATR included the signal day's own high/low | Lookahead in 2 of the 28 features | ATR from sessions strictly before the day | `test_atr_excludes_the_signal_days_own_range` |
| 8 | ATR / prev-day levels looked up by the day's daily row — absent live | Features NaN live, real in training (train/serve skew) | Same date-based lookup for both | `test_history_and_live_views_agree` |
| 9 | CI ran `main.py`'s infinite loop every 5 min with `cancel-in-progress` | Each run killed by the next; artifacts unreliable | `--once` mode; runs queue; cache + logs persisted | — (workflow) |
| 10 | CI had no Telegram credentials (config.ini removed from git) | CI alerts could never send | Env vars `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | `test_telegram_credentials_from_environment` |
| 11 | Telegram `parse_mode=HTML` | Messages containing `&` or `<` (e.g. **M&M**) rejected by Telegram | Plain text | `test_notifier_uses_ist_and_escapes_nothing` |
| 12 | "Today" came from the server clock | On a UTC machine the day rolls over at 05:30 IST | All dates in IST | `test_notifier_uses_ist_and_escapes_nothing` |
| 13 | Live universe file was 329 days old | Trading 13 stocks that had left the index, missing 12 that joined | Current list downloaded; warning when >200 days old | `test_live_universe_is_dealiased_and_warns_when_stale` |
| 14 | `LTIM` renamed to `LTM`; `DUMMYTATAM` placeholder | One index stock silently never scanned | Alias + placeholder list in `fetch_symbols.py` | same |
| 15 | Lot size was never filled in the Telegram message | Blank "Lot Size" | Read from `Lot_size.csv` | — |
| 16 | Dashboard "● LIVE" pill hardcoded green | Looked live when the market or bot was down | Computed market status + bot heartbeat, warning if no cycle in 12 min | — (web) |
| 17 | Flask ran with `debug=True` | Interactive debugger = code execution if ever exposed | Off unless `ORBITAL_DEBUG=1` | — |
| 18 | Dhan's intraday API omits the **most recent** session unless `toDate` is past it (22→22 Sep returned nothing; older days are inclusive) | Found by the real-API dry run: **0 candles for all 200 stocks** → the holiday check would have shut the bot down on a trading day | Every intraday request asks one day past the end and trims | `test_intraday_request_asks_past_the_end_and_trims` |
| 19 | Dhan publishes the **daily** bar late (22 Sep's bar still missing at 03:00 on the 23rd) | Previous close = the day-before-yesterday's → wrong ±1.8% filter and pivots on every signal | Snapshot of the quote feed's official OHLC after the close (`main.py --snapshot`, 16:10 CI job); the engine uses it when the daily bar is missing | `test_missing_daily_bar_uses_official_snapshot` |
| 21 | Dhan answers "no candles in this range" with HTTP 400 `DH-907` instead of an empty list | Found on the first market-hours run (23 Sep): the unpublished daily bar made **every** stock fail preparation | `DH-907` is treated as empty | `test_no_data_answer_is_an_empty_frame_not_an_error` |
| 22 | Preparation failures were skipped silently, and 0 prepared stocks looked like "no candles today" | The bot declared a **holiday on a trading day** and stopped | Failures are counted and logged; 0 prepared → alert and retry next cycle; the holiday check needs prepared stocks | `test_failed_preparation_is_reported_not_silent`, `test_zero_prepared_symbols_is_not_a_holiday` |
| 23 | Snapshot row stamped with a fixed +05:30 offset, Dhan data with `Asia/Kolkata` | Pandas merged them into a plain index → crash, only with real data | Same time zone as the table | `test_missing_daily_bar_uses_official_snapshot` (now uses Dhan's zone) |
| 24 | `--dry-run` wrote to the real signal log and decision log | Practice runs polluted the log and could stop a later real run from sending | Dry run logs to `live_decisions_dryrun.csv` and never touches the signal log | — |
| 20 | Fallback close from 5-minute candles is the last trade, not NSE's official close (RELIANCE 22 Sep: 1244.50 vs 1240.40, 0.33%) | Enough to flip a signal near the ±1.8% line | Only used if there's no daily bar AND no snapshot; counted and sent as a Telegram warning | `test_missing_daily_bar_without_snapshot_falls_back_to_candles` |

## Deployment hardening (2026-09-29)

| # | Issue | Fix | Test |
|---|---|---|---|
| 25 | A Telegram bot token and chat id were committed in `config.ini` (Oct 2025) and stayed in the public history after the file was deleted | Token rotated; `config.ini` and every trade/signal CSV purged from all history (`git filter-repo`) and force-pushed; `config.example.ini` ships instead | — |
| 26 | Nothing in code stopped an order call — "signals only" was a convention | Allowlist of market-data paths in `_post` and in the HTTP session; anything else raises `OrderPathBlocked` | `tests/test_signals_only.py` |
| 27 | Two services logging in to the same Dhan account would invalidate each other's tokens | ORBITAL reads the token file the VM's token service writes; waits while it's refreshed; after a 401 waits for a new token and retries once | `tests/test_signals_only.py` |
| 28 | Two services share the account's 5 req/s limit | ORBITAL at 3 req/s, silent at :57–:04 of each minute | — |
| 29 | The GitHub workflow ran the bot on a cron with a static token and uploaded trade logs as artifacts of a public repo | Workflow now runs the tests only | — |

## Edge cases and how they're handled

| Situation | Handling |
|---|---|
| Weekend / outside 09:40–15:16 | Loop idles; `--once` exits immediately |
| Exchange holiday on a weekday | After 09:50, if no stock has printed a candle, the bot stops for the day |
| Bot restarted mid-day | Today's decisions are reloaded from `live_decisions.csv`; nothing is re-scored or re-sent |
| Bot started late (e.g. 11:30) | Morning signals are found, logged as **STALE**, never sent — a 10:20 breakout at 11:30 isn't the tested trade |
| Only the first signal per stock/direction/day is scored | Matches the population the model was trained on; a SKIP is final for the day |
| Stock halted / missing opening-range candle | No range → no signal for that stock today |
| Stock with no daily history (new listing) | Skipped until it has a previous session |
| Newly listed stock without 20 days of volume history | Volume features are NaN → 0, exactly as in training |
| Dhan publishes a candle a few seconds late | Cycles run 20 s after each 5-min boundary |
| Dhan's daily bar not published by the open | Official OHLC from last evening's snapshot; if that's missing too, candle-derived values with a warning |
| Pre-open auction candles, Muhurat sessions, stray index timestamps | Everything outside 09:15–15:30 is dropped at the data layer |
| Transient network / DNS failure | `dhan_client` retries with back-off; if a cycle still fails it is skipped and the next one retries |
| Rate limit (Dhan: 5 req/s per account) | Thread-safe limiter: 4 req/s alone; 3 req/s plus a quiet window when the account is shared (`rate_per_sec`, `quiet_seconds`) |
| Dhan token expires (daily) | Token-file mode: wait up to 15 min for the refresher, retry once after a 401; then a Telegram alert and exit code 2 (systemd does not restart on 2) |
| Order endpoint called by mistake | `OrderPathBlocked` before any request is sent |
| Telegram API hiccup | One retry; failures are logged with status FAILED |
| Late entries | None after 15:10; everything flat at 15:15 |
| Index rebalance (end of Mar / Sep) | Warning after 200 days; replace `data/ind_nifty200list.csv` |

## Known limitations (not fixed)

- **Single-threaded fetch per cycle.** ~200 intraday requests at 4/s take ~50 s of each 5-minute cycle. Fine for 200 stocks; a larger universe would need Dhan's websocket feed.
- **The bot sends entries only.** Stops and trails are in the message, but it does not manage the position, and it cannot place orders.
- **Daily token.** Dhan access tokens last ~24 h. On the VM a separate token service renews it; a local static token must be renewed by hand.
