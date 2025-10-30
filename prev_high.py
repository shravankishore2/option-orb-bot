from fetch_symbols import get_symbols
import yfinance as yf
from fetch_ohlc import normalize_index_to_ist
from fetch_symbols import get_symbols
symbols = get_symbols()
def Prev_day_high(symbols):
    """Prev day high."""
    prev_highs = {}
    for sym in symbols:
        ticker = f"{sym}.NS"
        data = yf.download(ticker, period="2d", interval="1d", progress=False)
        if len(data) >= 2:
            prev_high = data['High'].iloc[-2]
        elif len(data) == 1:
            prev_high = data['High'].iloc[-1]
        else:
            prev_high = None

        if prev_high is not None:
            prev_highs[sym] = float(prev_high)

    return prev_highs


# store dictionary
prev_high_dict = Prev_day_high(symbols)

# Example use
print(prev_high_dict)