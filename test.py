import pandas as pd
df = pd.read_csv("backtest_opening_range.csv")
df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S").dt.time
print(df["time"].value_counts().head(20))