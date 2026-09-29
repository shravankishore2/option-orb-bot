"""
features.py — the single feature engine, shared by training AND the live bot.

If the live bot computed features with different code from the one the model
was trained on, it would be scoring inputs the model has never seen (training/
serving skew). So there is exactly one implementation, here, and both
mine_features.py (history) and live_engine.py (live) call it.

Point-in-time rule: every feature uses only candles that had CLOSED by the
signal's entry time. A candle stamped T covers T..T+5min, so at entry time T
only candles stamped strictly before T are known.

`legacy=True` reproduces the 4-Sep-2026 implementation exactly (used only by the
regression test). It had two defects, fixed when legacy=False:
  1. ATR used a rolling window that INCLUDED the signal day's own high/low —
     not known until the close (lookahead). Now shifted by one session.
  2. hit_Nr labels counted a candle that touched both the target and the stop
     as a hit (optimistic). Now the stop wins, matching the exit simulator.
"""

import datetime as dt
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

import strategy_config as C

FEATURES = [
    "nifty_ret", "nifty_or_pos", "aligned_with_nifty", "breadth",
    "sector_ret", "rel_to_sector",
    "rel_vol_or", "rel_vol_entry", "vol_trend",
    "atr_pct", "atr_norm_range", "range_expansion",
    "vwap_dist", "move_from_open", "pos_in_day_range",
    "dist_pdh", "dist_pdl", "level_touches", "consec_bars",
]

LABELS = ["mfe_pct", "mae_pct", "hit_1r", "hit_1_5r", "hit_2r", "hit_3r"]

# The 9 opening-range "geometry" inputs, computed from the signal row alone.
GEOMETRY = ["orb_range_pct", "gap_pct", "breakout_strength", "entry_vs_mid",
            "prev_close_vs_orb", "signal_minutes", "direction_enc", "orb_range_abs",
            "entry_log"]

MODEL_FEATURES = GEOMETRY + FEATURES          # all 28 model inputs


def geometry_features(df):
    """Add the 9 geometry inputs. Needs columns orh, orl, prev_close,
    entry_price, direction, time (lower-case). Returns (df, GEOMETRY)."""
    df = df.copy()
    orb = df["orh"] - df["orl"]
    safe = orb.replace(0, np.nan)
    buy = df["direction"].astype(str).str.upper() == "BUY"

    df["orb_range_pct"] = orb / df["orl"] * 100                        # morning range width
    df["gap_pct"] = (df["orl"] - df["prev_close"]) / df["prev_close"] * 100
    df["breakout_strength"] = np.where(                                 # how far past the level
        orb == 0, 0.0,
        np.where(buy, (df["entry_price"] - df["orh"]) / safe, (df["orl"] - df["entry_price"]) / safe))
    df["entry_vs_mid"] = (df["entry_price"] - (df["orh"] + df["orl"]) / 2) / safe
    df["prev_close_vs_orb"] = (df["prev_close"] - df["orl"]) / safe

    def minutes(t):
        try:
            h, m = str(t).split(".")[0].split(":")[:2]
            return max(0, int(h) * 60 + int(m) - (9 * 60 + 15))
        except Exception:
            return 0

    df["signal_minutes"] = df["time"].apply(minutes)                    # since 09:15
    df["direction_enc"] = buy.astype(int)
    df["orb_range_abs"] = orb
    df["entry_log"] = np.log(df["entry_price"])
    return df, list(GEOMETRY)

HIT_MULTS = [(1.0, "hit_1r"), (1.5, "hit_1_5r"), (2.0, "hit_2r"), (3.0, "hit_3r")]


# ------------------------------------------------------------ per-symbol history
@dataclass
class SymbolHistory:
    """Everything about a symbol's PAST that features need, keyed by trading day."""
    atr: dict = field(default_factory=dict)        # day -> ATR(14) known at the open
    pdh: dict = field(default_factory=dict)        # day -> previous session high
    pdl: dict = field(default_factory=dict)        # day -> previous session low
    or_volume: dict = field(default_factory=dict)  # day -> that day's OR volume
    vol_norm: dict = field(default_factory=dict)   # day -> median OR volume, prior 20 days


def opening_window(day_candles):
    t = day_candles.index.time
    return day_candles[(t >= C.OR_START) & (t < C.OR_END)]


def build_symbol_history(candles, daily, legacy=False):
    """candles: intraday OHLCV over many days (may include today's completed
    candles). daily: daily OHLCV of COMPLETED sessions.

    ATR and previous-day levels are computed from sessions strictly BEFORE each
    day, looked up by date — so they work identically in history (where the
    day's own daily row exists) and live (where it never does).
    """
    hist = SymbolHistory()

    intraday_days = sorted(set(candles.index.date)) if candles is not None and not candles.empty else []

    if daily is not None and not daily.empty:
        d_days = np.array([d.date() for d in daily.index])
        ranges = (daily["High"] - daily["Low"]).to_numpy(float)
        highs = daily["High"].to_numpy(float)
        lows = daily["Low"].to_numpy(float)

        if legacy:
            # 4-Sep behaviour: the window INCLUDES the day's own range.
            rng = pd.Series(ranges).rolling(14, min_periods=5).mean().to_numpy()
            hist.atr = {d: float(v) for d, v in zip(d_days, rng) if pd.notna(v)}
            for n in range(1, len(d_days)):
                hist.pdh[d_days[n]] = float(highs[n - 1])
                hist.pdl[d_days[n]] = float(lows[n - 1])
        else:
            for day in sorted(set(d_days) | set(intraday_days)):
                k = int(np.searchsorted(d_days, day, side="left"))   # rows strictly before
                if k >= 5:
                    hist.atr[day] = float(ranges[max(0, k - 14):k].mean())
                if k >= 1:
                    hist.pdh[day] = float(highs[k - 1])
                    hist.pdl[day] = float(lows[k - 1])

    if intraday_days:
        for day, g in candles.groupby(candles.index.date):
            win = opening_window(g)
            if not win.empty:
                hist.or_volume[day] = float(win["Volume"].sum())

        ordered = sorted(hist.or_volume)
        series = pd.Series([hist.or_volume[d] for d in ordered], index=ordered)
        rolled = series.rolling(20, min_periods=5).median().shift(1)
        hist.vol_norm = {d: float(v) for d, v in rolled.items() if pd.notna(v)}

    return hist


# ------------------------------------------------------------ cross-sectional context
def returns_column(candles):
    """% move from the day's open at each 5-min stamp — compact (float32)."""
    opens = candles.groupby(candles.index.date)["Open"].transform("first")
    ret = ((candles["Close"] - opens) / opens * 100).astype("float32")
    ret.index = pd.MultiIndex.from_arrays([candles.index.date, candles.index.time],
                                          names=["day", "stamp"])
    return ret[~ret.index.duplicated(keep="last")]


class MarketContext:
    """Breadth, sector returns and NIFTY position as of each (day, stamp)."""

    def __init__(self, candles_by_symbol, sectors, nifty=None, members=None, returns=None):
        """
        candles_by_symbol: {symbol: intraday OHLCV (any number of days)}
        sectors:  {symbol: sector name}
        nifty:    NIFTY 50 intraday OHLCV (optional)
        members:  optional callable day -> set of index members that day, so
                  breadth and sector strength use the point-in-time universe
        returns:  optional {symbol: returns_column(...)} instead of candles —
                  lets the history pass avoid holding every candle in memory
        """
        cols = dict(returns or {})
        for sym, c in (candles_by_symbol or {}).items():
            if c is not None and not c.empty:
                cols[sym] = returns_column(c)

        matrix = pd.DataFrame(cols)

        if members is not None and not matrix.empty:
            days = matrix.index.get_level_values("day")
            uniq = pd.unique(days)
            rows = []
            for d in uniq:
                m = members(d)                       # once per day, not per stock
                rows.append([s in m for s in matrix.columns])
            member_tbl = pd.DataFrame(rows, index=uniq, columns=matrix.columns)
            mask = member_tbl.reindex(days).to_numpy()
            matrix = matrix.where(mask)

        self.matrix = matrix
        self.breadth = ((matrix > 0).sum(axis=1) / matrix.notna().sum(axis=1)) \
            if not matrix.empty else pd.Series(dtype=float)

        self.sector_of = {s: sectors.get(s, "UNKNOWN") for s in matrix.columns}
        self.sector_ret = matrix.T.groupby(self.sector_of).mean().T \
            if not matrix.empty else pd.DataFrame()

        self.nifty = {}
        if nifty is not None and not nifty.empty:
            for day, g in nifty.groupby(nifty.index.date):
                open_px = float(g.iloc[0]["Open"])
                win = opening_window(g)
                hi = float(win["High"].max()) if not win.empty else np.nan
                lo = float(win["Low"].min()) if not win.empty else np.nan
                span = hi - lo
                for stamp, close_px in zip(g.index, g["Close"].to_numpy(dtype=float)):
                    self.nifty[(day, stamp.time())] = (
                        (close_px - open_px) / open_px * 100,
                        (close_px - lo) / span if span and span > 0 else np.nan,
                    )

    def lookup(self, symbol, sector, key):
        nret, nor = self.nifty.get(key, (np.nan, np.nan))
        breadth = self.breadth.get(key, np.nan)
        sect = self.sector_ret[sector].get(key, np.nan) \
            if sector in self.sector_ret.columns else np.nan
        own = self.matrix[symbol].get(key, np.nan) \
            if symbol in self.matrix.columns else np.nan
        return nret, nor, breadth, sect, own


# ------------------------------------------------------------ features for one signal
def compute_features(signal, day_candles, hist, ctx, sector):
    """
    signal: dict with date, time (HH:MM:SS entry), symbol, direction,
            entry_price, ORH, ORL.
    Returns a dict of FEATURES, or None if there isn't enough data.
    """
    day = pd.to_datetime(signal["date"]).date()
    entry_time = dt.datetime.strptime(signal["time"], "%H:%M:%S").time()

    past = day_candles[day_candles.index.time < entry_time]
    if past.empty:
        return None

    prior_stamp = past.index[-1].time()
    key = (day, prior_stamp)

    entry = float(signal["entry_price"])
    orh, orl = float(signal["ORH"]), float(signal["ORL"])
    orb = orh - orl
    long = str(signal["direction"]).upper() == "BUY"
    sign = 1.0 if long else -1.0

    day_open = float(day_candles.iloc[0]["Open"])

    typical = (past["High"] + past["Low"] + past["Close"]) / 3
    vol = past["Volume"].replace(0, np.nan)
    vwap = float((typical * vol).sum() / vol.sum()) if vol.sum() > 0 else np.nan

    highs = past["High"].to_numpy(dtype=float)
    lows = past["Low"].to_numpy(dtype=float)
    closes = past["Close"].to_numpy(dtype=float)
    vols = past["Volume"].to_numpy(dtype=float)

    level = orh if long else orl
    touches = int(np.sum((highs >= level * 0.999) & (lows <= level * 1.001)))

    consec = 0
    for k in range(len(closes) - 1, 0, -1):
        if (long and closes[k] > closes[k - 1]) or (not long and closes[k] < closes[k - 1]):
            consec += 1
        else:
            break

    bar_ranges = highs - lows
    recent = bar_ranges[-3:].mean() if len(bar_ranges) >= 3 else np.nan
    baseline = bar_ranges.mean() if len(bar_ranges) else np.nan

    atr_val = hist.atr.get(day)
    norm = hist.vol_norm.get(day)
    or_vol = hist.or_volume.get(day)

    day_hi = float(past["High"].max())
    day_lo = float(past["Low"].min())
    span = day_hi - day_lo

    nret, nor, breadth, sect, own = ctx.lookup(signal["symbol"], sector, key)

    vmean = vols.mean()

    return {
        "nifty_ret": nret,
        "nifty_or_pos": nor,
        "aligned_with_nifty": (1 if (long and nret > 0) or (not long and nret < 0) else 0)
                              if pd.notna(nret) else np.nan,
        "breadth": breadth,
        "sector_ret": sect,
        "rel_to_sector": own - sect if pd.notna(own) and pd.notna(sect) else np.nan,
        "rel_vol_or": or_vol / norm if (norm and norm > 0 and or_vol) else np.nan,
        "rel_vol_entry": vols[-1] / vmean if vmean > 0 else np.nan,
        "vol_trend": vols[-3:].mean() / vmean if vmean > 0 else np.nan,
        "atr_pct": atr_val / entry * 100 if atr_val else np.nan,
        "atr_norm_range": orb / atr_val if atr_val and atr_val > 0 else np.nan,
        "range_expansion": recent / baseline if baseline and baseline > 0 else np.nan,
        "vwap_dist": (entry - vwap) / vwap * 100 * sign if pd.notna(vwap) and vwap > 0 else np.nan,
        "move_from_open": (entry - day_open) / day_open * 100 * sign,
        "pos_in_day_range": (entry - day_lo) / span if span > 0 else np.nan,
        "dist_pdh": (entry - hist.pdh[day]) / entry * 100 if day in hist.pdh else np.nan,
        "dist_pdl": (entry - hist.pdl[day]) / entry * 100 if day in hist.pdl else np.nan,
        "level_touches": touches,
        "consec_bars": consec,
    }


# ------------------------------------------------------------ forward labels (never features)
def forward_labels(signal, day_candles, legacy=False):
    """What the trade went on to do. Training targets only — never model inputs."""
    entry_time = dt.datetime.strptime(signal["time"], "%H:%M:%S").time()
    t = day_candles.index.time
    future = day_candles[(t >= entry_time) & (t <= C.FORCE_EXIT_TIME)]
    if future.empty:
        return None

    entry = float(signal["entry_price"])
    orb = float(signal["ORH"]) - float(signal["ORL"])
    long = str(signal["direction"]).upper() == "BUY"
    sign = 1.0 if long else -1.0

    fh = future["High"].to_numpy(dtype=float)
    fl = future["Low"].to_numpy(dtype=float)

    out = {
        "mfe_pct": (fh.max() - entry) / entry * 100 if long else (entry - fl.min()) / entry * 100,
        "mae_pct": (fl.min() - entry) / entry * 100 if long else (entry - fh.max()) / entry * 100,
    }

    for mult, name in HIT_MULTS:
        hit = 0
        if orb > 0:
            up = entry + sign * mult * orb
            down = entry - sign * 1.0 * orb
            for k in range(len(fh)):
                stopped = fl[k] <= down if long else fh[k] >= down
                reached = fh[k] >= up if long else fl[k] <= up
                if legacy:
                    if stopped and not reached:
                        break
                    if reached:
                        hit = 1
                        break
                else:
                    if stopped:          # pessimistic: stop wins a shared candle
                        break
                    if reached:
                        hit = 1
                        break
        out[name] = hit

    return out
