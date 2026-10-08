# Research: finished experiments

Each folder holds the code of one finished experiment. Its results are in `docs/`, and the code is kept
so they can be reproduced. Run every script from the repo root (`python research/<folder>/<script>.py`);
their outputs go to `docs/`. Nothing here is imported by the live bot, the dashboard or the pipeline.

| Experiment | Found | Reported in |
|---|---|---|
| [`threshold_v2_1/`](threshold_v2_1/) | Live GO rate matches history once the time-of-day mix is accounted for; a stale previous-close bug (fixed); lower thresholds raise total P&L at 0.05% cost but not per trade | `docs/THRESHOLD_RESULTS.md` |
| [`exit_grace/`](exit_grace/) | A trailing-stop grace period: 5 min is identical by construction, 10 min within noise; not adopted | `docs/EXIT_GRACE.md` |
| [`late_entry/`](late_entry/) | v2's entries at or after 15:00 are 42% of its trades but 8% of its P&L after 0.05% cost; prices are tradable | `docs/LATE_ENTRY.md` |
| [`v1_exploration/`](v1_exploration/) | The 4-Sep exploration: label choice, exit sweep, option payoff | `PROJECT_GUIDE.md` §4–5 |
| [`exit_candle/`](exit_candle/) | NSE's closing auction (from 3 Aug 2026) removed the 15:15–15:25 candles for F&O stocks; forced exits now fall back to 15:10 | `docs/EXIT_CANDLE.md` |
| [`v3_label/`](v3_label/) | No cost- or size-aware label beats v2 on total after 0.10% with a CI above zero; alignment with NIFTY doesn't pay after costs | `docs/V3_LABEL_RESULTS.md` |
| [`v3/`](v3/) | No v3 variant beat v2 under its pre-set rule | `docs/V3_RESULTS.md` on branch `research/v3-candidate` |
| AUC reconciliation (in `model_report.py`, a pipeline step) | 0.665 was a different label and dataset; v2's out-of-sample AUC is 0.535 | `docs/MODEL.md`, "Which AUC to quote" |
