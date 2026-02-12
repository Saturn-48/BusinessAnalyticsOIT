import pandas as pd
import plotly.express as px
import Cleaner
from time import sleep


# Build / refresh database
Cleaner.CreateDB()

# Load datasets
df_clean = Cleaner.GetCleanFrame()
df_stats = Cleaner.GetOverallStats()
df_holiday = Cleaner.GetHolidayStats()
df_corr = Cleaner.GetCorrelation()

# Convert to pandas for Plotly
pdf = pd.DataFrame(df_clean.to_dicts())

print("\nOverall Stats:")
print(df_stats)

print("\nHoliday vs Non-Holiday Stats:")
print(df_holiday)

print("\nCorrelation:")
print(df_corr)


# ---------------- CHARTS ---------------- #

#  Weekly Sales Over Time
fig_total = px.line(
    pdf,
    x='Date',
    y='WeeklySales',
    title='Weekly Sales Over Time',
)

fig_total.add_scatter(
    x=pdf[pdf['HolidayWindow'] == 1]['Date'],
    y=pdf[pdf['HolidayWindow'] == 1]['WeeklySales'],
    mode='markers',
    name='Holiday Window',
    hover_data=["IsHoliday"]
)

fig_total.show()
sleep(1)

#  Holiday vs Non-Holiday Line
fig_holiday = px.line(
    pdf,
    x='Date',
    y='WeeklySales',
    color='HolidayType',
    title='Weekly Sales: Holiday Window vs Normal Weeks'
)
fig_holiday.show()
sleep(1)


# Sales vs Markdowns
fig_markdown = px.scatter(
    pdf,
    x='TotalMarkDown',
    y='WeeklySales',
    color='HolidayType',
    title='Weekly Sales vs Total MarkDown'
)
fig_markdown.show()
sleep(1)


# Distribution
fig_hist = px.histogram(
    pdf,
    x='WeeklySales',
    nbins=30,
    title='Distribution of Weekly Sales'
)
fig_hist.show()
sleep(1)
