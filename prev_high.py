from fetch_symbols import get_symbols
import yfinance as yf
from fetch_ohlc import normalize_index_to_ist
from fetch_symbols import get_symbols
symbols = get_symbols()
def Prev_day_close(symbols):
    """Prev day high."""
    prev_closes = {}
    for sym in symbols:
        ticker = f"{sym}.NS"
        data = yf.download(ticker, period="2d", interval="1d", progress=False)
        if len(data) >= 2:
            prev_close = data['Close'].iloc[-2]
        elif len(data) == 1:
            prev_close = data['Close'].iloc[-1]
        else:
            prev_close = None

        if prev_close is not None:
            prev_closes[sym] = float(prev_close)

    return prev_closes


# store dictionary
prev_close_dict = Prev_day_close(symbols)

