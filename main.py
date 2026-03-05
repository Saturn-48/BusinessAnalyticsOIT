import plotly.express as px
import Cleaner

# ---------------------------
# Build / Refresh Database
# ---------------------------

df = Cleaner.GetAll()
# Get aggregated weekly data
weekly_df = Cleaner.GetWeeklyAggregated()

# ---------------------------
# Base Line Chart
# ---------------------------
fig_weekly = px.line(
    weekly_df,
    x="WeekStart",
    y="WeeklyRevenue",
    title="Weekly Revenue with Holiday Window",
    hover_data=["IsHoliday", "HolidayWindow"]
)

# ---------------------------
# Holiday Window Scatter Layer
# ---------------------------
holiday_window_df = weekly_df[weekly_df["HolidayWindow"] == 1]

scatter_fig = px.scatter(
    holiday_window_df,
    x="WeekStart",
    y="WeeklyRevenue",
    hover_data=["IsHoliday", "HolidayWindow"]
)

# Add scatter trace(s) to main figure
for trace in scatter_fig.data:
    trace.name = "Holiday Window"
    trace.marker.size = 5
    trace.marker.color = "red"
    fig_weekly.add_trace(trace)

# ---------------------------
# Show Figure
# ---------------------------
fig_weekly.show()