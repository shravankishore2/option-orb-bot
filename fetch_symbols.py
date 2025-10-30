# fetch_symbols.py — simplified version (Nifty 200 only)
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"  # safer relative path


def get_symbols():
    """Load all Nifty 200 symbols from CSV (no F&O filter)."""
    path = DATA_DIR / "ind_nifty200list.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — please put ind_nifty200list.csv inside the data folder."
        )

    df = pd.read_csv(path)

    # Find correct column automatically
    for col in df.columns:
        if "symbol" in col.lower():
            symbols = (
                df[col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.replace(" ", "")
                .str.upper()
                .unique()
                .tolist()
            )
            print(f"✅ Loaded {len(symbols)} Nifty 200 symbols from CSV.")
            return sorted(symbols)

    # Fallback — use first column if no “symbol” column detected
    symbols = (
        df[df.columns[0]]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace(" ", "")
        .str.upper()
        .unique()
        .tolist()
    )
    print(f"✅ Loaded {len(symbols)} Nifty 200 symbols (from fallback column).")
    return sorted(symbols)


if __name__ == "__main__":
    print(get_symbols())