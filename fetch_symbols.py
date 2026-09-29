# fetch_symbols.py — the live trading universe (today's Nifty 200)
#
# data/ind_nifty200list.csv is NSE's constituent file. data/ is not in git, so
# download it (and the wider sector map) with
#   python fetch_symbols.py --refresh
# and again after each semi-annual rebalance (end of March and September).

import datetime as dt
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"
LIST_FILE = DATA_DIR / "ind_nifty200list.csv"

# NSE's file sometimes lags a symbol change, or carries placeholders.
ALIASES = {"LTIM": "LTM"}          # LTIMindtree now trades as LTM
PLACEHOLDERS = {"DUMMYTATAM"}      # not a tradable security

STALE_AFTER_DAYS = 200             # a rebalance happens every ~183 days

NSE_LISTS = "https://niftyindices.com/IndexConstituent/{}.csv"
SECTOR_SOURCES = ("ind_niftytotalmarket_list", "ind_niftymicrocap250_list", "ind_nifty500list")
SECTOR_FILE = DATA_DIR / "sectors.csv"


def get_symbols(path=LIST_FILE, quiet=False):
    """Today's Nifty 200 symbols, cleaned and de-aliased."""
    if not path.exists():
        raise FileNotFoundError(f"{path} not found — download ind_nifty200list.csv into data/.")

    df = pd.read_csv(path)
    col = next((c for c in df.columns if "symbol" in c.lower()), df.columns[0])

    raw = df[col].dropna().astype(str).str.strip().str.replace(" ", "").str.upper()
    symbols = sorted({ALIASES.get(s, s) for s in raw if s not in PLACEHOLDERS})

    if not quiet:
        age = (dt.datetime.now() - dt.datetime.fromtimestamp(path.stat().st_mtime)).days
        print(f"✅ Loaded {len(symbols)} Nifty 200 symbols.")
        if age > STALE_AFTER_DAYS:
            print(f"⚠️ {path.name} is {age} days old — the index has rebalanced since. "
                  f"Download the current list from niftyindices.com.")
    return symbols


def _download(name):
    import io
    import requests
    r = requests.get(NSE_LISTS.format(name), headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    if not {"Symbol", "Industry"} <= set(df.columns) or len(df) < 50:
        raise ValueError(f"{name}.csv doesn't look like an NSE constituent list")
    return df


def refresh():
    """Download today's Nifty 200 list and the wider symbol→industry map into data/."""
    DATA_DIR.mkdir(exist_ok=True)
    nifty200 = _download("ind_nifty200list")
    nifty200.to_csv(LIST_FILE, index=False)
    frames = [_download(name)[["Symbol", "Industry"]] for name in SECTOR_SOURCES]
    sectors = pd.concat(frames).drop_duplicates("Symbol").sort_values("Symbol")
    sectors.to_csv(SECTOR_FILE, index=False)
    print(f"✅ {LIST_FILE.name}: {len(nifty200)} rows · {SECTOR_FILE.name}: {len(sectors)} symbols")


if __name__ == "__main__":
    import sys
    if "--refresh" in sys.argv:
        refresh()
    print(get_symbols())
