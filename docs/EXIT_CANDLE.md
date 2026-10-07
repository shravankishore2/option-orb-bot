# The missing 15:15 candle

Investigation, 2026-10-08 (night after the 7 Oct session). Nothing was changed: no exit logic, live
code, logged data, champion, threshold or variant. The trade-level numbers come from
`research/exit_candle/exit_candle_check.py`, run on the cached candles and a read-only copy of the
VM's shadow log. The Dhan queries were raw reads that wrote nothing.

## Root cause: (c), an exchange change, not a data or fetch problem

**From Monday 3 Aug 2026, NSE runs a Closing Auction Session (CAS) for F&O stocks.** Continuous
trading in those stocks ends at **15:15**. The auction then runs to 15:35, and its single
equilibrium price becomes the official close. Stocks outside F&O still trade continuously to
15:30, and stock and index derivatives now close at 15:40.
([Goodreturns](https://www.goodreturns.in/news/nse-new-rule-from-today-closing-auction-session-begins-for-f-o-stocks-check-new-closing-price-rule-1525791.html),
[Outlook Business](https://www.outlookbusiness.com/markets/sebi-closing-auction-session-new-stock-market-timings-from-august-3),
[Dhan: what is CAS](https://dhan.co/support/general/market-session-status-and-timing/what-is-the-closing-auction-session/).)
The dhanhq changelog, as indexed in search results, says that for CAS instruments "minute candles
after 3:15 PM are not available through the Historical Data API"
([changelog](https://data.safetycli.com/changelogs/dhanhq); the page itself didn't load for a
direct read).

Evidence:

| Check | Result |
|---|---|
| **Stored data, 10 large stocks, every session from 1 Jul 2026** (Mac cache to 22 Sep, VM cache 1 Sep → 6 Oct) | 75 candles to 15:25 on every session through **31 Jul**; 72 candles ending at **15:10** on every session from **3 Aug** (the first trading day after). No exceptions |
| **Dhan's raw intraday response, re-queried now** (RELIANCE, INFY, HDFCBANK; 30 Jul → 6 Aug and 1 → 7 Oct; before any parsing) | Same: 15:15–15:25 present on 30–31 Jul, absent from 3 Aug, including for 7 Oct asked 9 hours after the close. 1-minute data ends at 15:14 (360 candles, not 375); 15- and 60-minute data end at 15:00 and 14:15. **It never arrives later**, so it isn't our fetch timing, the cache or our parser |
| **Which stocks still have 15:15** (the whole universe, 6 Oct; 22 Sep) | 6 Oct: 17 of 200 have it, 183 don't. Checked against NSE's public F&O list (`fo_mktlots.csv`): **all 183 without it are F&O stocks; none of the 17 with it is.** On 22 Sep, 57 of the 58 with it are outside today's F&O list (non-F&O stocks, ETFs, the NIFTY index). The exception is ENRIN, a recent listing that is on today's list and was presumably added after 22 Sep; all 204 without it are F&O |
| **Independent source** (Yahoo Finance 5-min chart API, no credentials) | RELIANCE and INFY: no volume in the 15:20 and 15:25 bars on 1, 5, 6 and 7 Oct. COROMANDEL (not F&O) trades through 15:25 |

So the conclusion is **(c)**. For F&O stocks the 15:15–15:25 candles don't exist because nothing trades
continuously then. The closing price of those stocks now comes from an auction, which Dhan's intraday
API doesn't show as a candle.

It plausibly also explains the stale closes found on 7 Oct (the 15:40 quote close still equal to
the previous session's for about half the stocks): an auction close set at 15:35 may not have
reached the quote by 15:40. That link isn't verified.

## What the exit code does with it

`exits.simulate` closes a position at the close of the first candle stamped 15:15 or later (TIME).
If the candles run out first, it closes at the close of the last candle it was given (LAST):

```
pnl = sign × (close[last] − entry) / entry × 100     # entry = the signal candle's close
```

For an F&O stock since 3 Aug, the last candle is 15:10, so the position closes at the 15:10
candle's close, the last continuous-trading price, about 15:14:59. The live tracker uses the same
function, so it labels these trades LAST at 15:10 when it finalises after the close.

### "Zero-length" trades

No trade enters and exits on the same candle. The entry price is the close of the signal candle,
and the exit walk starts on the next one. The edge case is an **entry at 15:10**, from the
15:05 candle. Before 3 Aug it was held two candles, 15:10 and 15:15, with the exit at 15:20. Now
it is held one candle and exits at 15:15 (the 15:10 candle's close): a 5-minute trade.

| GO trades exiting LAST at the 15:10 candle | Trades | Held one candle (entry 15:10) | Mean P&L of those | All LAST@15:10, mean |
|---|---|---|---|---|
| Walk-forward, 3 Aug → 22 Sep | 25 | 7 | −0.132% (2 of 7 won) | +0.188% |
| Shadow log since 3 Aug (replay 23 Sep → 1 Oct + live 5–7 Oct) | 5 | 2 (both replay) | −0.399% | — |
| Live only (5–7 Oct) | 1 | 0 | — | — |

Practical note: since CAS, Dhan auto-squares equity intraday positions at **15:10**
([Dhan](https://dhan.co/support/general/market-session-status-and-timing/what-are-the-dhan-intraday-auto-square-off-timings/)).
An intraday (MIS) order on a 15:10 GO signal can't really be held on Dhan.

## Impact

**1. Champion live mean per trade since 3 Aug** (champion GO trades, exit as labelled):

| Set | Trades | Mean (gross) | After 0.05% | Without the one-candle trades |
|---|---|---|---|---|
| Live sessions (5–7 Oct) | 3 | −0.785% | −0.835% | the same (none) |
| Live + replayed sessions (23 Sep → 7 Oct) | 12 | −0.360% | −0.410% | −0.352% (10 trades) |
| Walk-forward GO, 3 Aug → 22 Sep (history, same rules) | 28 | +0.138% | +0.088% | +0.228% (21 trades) |

These samples are far too small to say anything about performance. They're here to size the
one-candle effect, which is small next to the noise.

**2. Walk-forward GO trades, per-trade mean by exit and period:**

| Period | Exit | Trades | Mean | After 0.05% | Hit |
|---|---|---|---|---|---|
| before 3 Aug | TIME, 15:15 candle (out 15:20) | 1,648 | +0.280% | +0.230% | 61% |
| from 3 Aug | LAST, 15:10 candle (out ~15:15) | 25 | +0.188% | +0.138% | 52% |
| from 3 Aug | TIME, 15:15 candle (non-F&O stocks) | 2 | −0.179% | −0.229% | 0% |
| before 3 Aug | all GO | 2,095 | +0.169% | +0.119% | 53% |
| from 3 Aug | all GO | 28 | +0.138% | +0.088% | 46% |

To size the change itself on a large sample, I re-scored the pre-August GO trades with the forced
exit moved to the 15:10 candle (an analysis setting of `exits.simulate`, not a code change):

| Pre-3-Aug GO trades | Exit at 15:15 candle (published) | Exit at 15:10 candle | Difference (95% CI, day bootstrap) |
|---|---|---|---|
| All 2,095 | +0.169% | +0.140% | −0.029% [−0.041, −0.017] |
| The 1,649 that reached the forced exit | +0.280% | +0.233% | |
| Late slice (entry ≥ 15:00), 852 | +0.077% | +0.056% | |

So the earlier exit costs about 0.03% per trade on average in history: smaller, but not zero.
Exiting at the **official (auction) close** instead would be different again. For the 24 LAST trades
from 3 Aug with a daily bar, it gives +0.133% against +0.188% at the 15:10 close. The official close
differs from the last 5-minute close by 0.25–0.28% per trade on average (median 0), which is larger
than the strategy's edge per trade.

**3. The late-cut window (from 8 Oct), if nothing changes.** Every late-slice trade in an F&O stock
that isn't stopped first will close at 15:10, not 15:15.
- **Historical rates:** since 3 Aug, 15 of 18 late GO trades exited this way. Before 3 Aug, 90% of
  late trades reached the forced exit, and 92% of the universe is F&O. About 35% of late trades
  enter at 15:10 and become one-candle trades.
- **Projection:** of the first **50 late trades** (the protocol's floor), about **41–45 will close
  at the 15:10 candle**, and about **15–20 of them will be held one candle**. Since 3 Aug, 7 of the
  18 late GO trades were. In the live shadow log since 23 Sep,
  181 of the 197 signals that reached the forced exit closed at 15:10.

## Fixes

Every live window is already inside the CAS period:
- the v2 forward test, from 23 Sep;
- the shadow variants, from 7 Oct;
- the late-cut protocol, from 8 Oct.

Every live label in them has been computed with the 15:10 fallback (non-F&O stocks: 15:15). So the
live P&L definition hasn't changed during any test. **Any change to the exit now would change it
mid-test.**

| # | Fix | Live P&L definition mid-test? | Protocol amendment? |
|---|---|---|---|
| 1 | **Keep the code; pin and document the rule as it has run since 3 Aug.** The forced exit is the close of the last continuous-trading candle at or before 15:15: 15:10 for CAS stocks, 15:15 for others. Add a dated clarification to the three protocols and PREREGISTRATION.md, a test that pins the behaviour, and an exit-candle column on the scorecard. Report history both ways (published 15:15, and re-scored at 15:10) when comparing live with history | **No.** It describes what every live label already is | A dated **clarification** (no change of rule) to each protocol and to the v2 pre-registration |
| 2 | Move the forced exit to 15:10 for every stock (`FORCE_EXIT_TIME`) | Yes, for the ~8% non-F&O trades | Yes: `strategy_config.py` is hashed, so it's a **new registration**; live counts would restart |
| 3 | Exit F&O stocks at the official auction close (a closing-auction order), labelling with the daily close | Yes: every F&O forced exit changes, by ~0.25% per trade in absolute terms | Yes, an amendment; labels also wait for a daily bar Dhan publishes late. Counts restart |
| 4 | Stop entries earlier, e.g. last entry 15:05, so no trade is a one-candle trade and none is cut by Dhan's 15:10 auto square-off | Yes: removes the 15:10 GO signals | Yes, a new registration. It's also exactly what the late-cut protocol is testing, so it would pre-empt it |

## Recommendation

**Fix 1, now.** It changes no P&L, no decision and no live count. It removes an ambiguity:
- The protocols and the pre-registration say "flat by the 15:15 candle". For 183 of 200 stocks the
  data has meant "at the 15:10 candle's close" since 3 Aug.
- That's also roughly what is tradable: the last continuous price, and Dhan's own intraday
  square-off is at 15:10.

Keep fixes 2–4 as candidates for the next registration after the late-October decision. Fix 4 in
particular should be weighed with the late-cut result, since a 15:10 entry is a 5-minute trade
that Dhan won't let an intraday position hold.

Not implemented; waiting on the owner.
