import polars as po
import plotly as pl

import Cleaner

Cleaner.CleanAll()

conn, cursor = Cleaner.NewConn()
df = Cleaner.GetCleanFrame()

print(df)
