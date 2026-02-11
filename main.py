import polars as po
import plotly as pl

import Cleaner

Cleaner.CleanAll()
conn, cursor = Cleaner.NewConn()

df = po.read_database("SELECT * FROM Sales", conn)

print(df)
