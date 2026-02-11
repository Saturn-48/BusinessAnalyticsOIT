import pandas as pd
import plotly.express as px
from time import sleep
import Cleaner
Cleaner.CleanAll()

conn, cursor = Cleaner.NewConn()
df = Cleaner.GetCleanFrame()


dict_df = df.to_dicts()

pdf = pd.DataFrame(dict_df)

# Total Weekly Sales Over Time (all stores)
fig_total = px.line(
    pdf,
    x='Date',
    y='WeeklySales',
    title='Weekly Sales Over Time',
    labels={'WeeklySales': 'Weekly Sales', 'Date': 'Date'}
)
fig_total.show()
sleep(4)
# Total Weekly Sales: Holiday vs Non-Holiday
fig_holiday = px.line(
    pdf,
    x='Date',
    y='WeeklySales',
    color='HolidayType',
    title='Weekly Sales: Holiday vs Non-Holiday',
    labels={'WeeklySales': 'Weekly Sales', 'Date': 'Date', 'HolidayType': 'Holiday Status'}
)
fig_holiday.show()
sleep(4)
# Weekly Markdown vs Sales (scatter)
fig_markdown = px.scatter(
    pdf,
    x='TotalMarkDown',
    y='WeeklySales',
    color='HolidayType',
    title='Weekly Sales vs Total MarkDown',
    labels={'WeeklySales': 'Weekly Sales', 'TotalMarkDown': 'Total MarkDown'}
)
fig_markdown.show()
sleep(4)
# Optional: If you want a histogram of weekly sales
fig_hist = px.histogram(
    pdf,
    x='WeeklySales',
    nbins=50,
    title='Distribution of Weekly Sales'
)
fig_hist.show()

