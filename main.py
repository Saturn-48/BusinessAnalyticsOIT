import plotly.graph_objects as go
import Cleaner
import Prediction
import DiscountRevenueAnalysis as DRA

# ---------------------------
# Build Database
# ---------------------------
print("Cleaning CSV...")
Cleaner.CleanAndExportCSV("Amazon.csv")
print("Building database...")
Cleaner.BuildDatabase()

# ---------------------------
# Load Weekly Aggregated Data
# ---------------------------
weekly_df = Cleaner.GetWeeklyAggregated()

# ---------------------------
# Calculate 8-week Rolling Trend
# ---------------------------
weekly_df["Trend"] = weekly_df["WeeklyRevenue"].rolling(window=8).mean()

# ---------------------------
# Plot Weekly Revenue
# ---------------------------
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=weekly_df["WeekStart"],
    y=weekly_df["WeeklyRevenue"],
    name="Weekly Revenue",
    mode="lines",
    hovertemplate="<b>Week Start:</b> %{x|%Y-%m-%d}<br><b>Revenue:</b> $%{y:,.2f}<extra></extra>"
))

# Trend line
fig.add_trace(go.Scatter(
    x=weekly_df["WeekStart"],
    y=weekly_df["Trend"],
    name="8-Week Trend",
    mode="lines",
    line=dict(width=4),
    hoverinfo="skip"
))

# Holiday Window Points
holiday_df = weekly_df[weekly_df["HolidayWindow"] == 1]
fig.add_trace(go.Scatter(
    x=holiday_df["WeekStart"],
    y=holiday_df["WeeklyRevenue"],
    mode="markers",
    name="Holiday Window",
    marker=dict(color="red", size=8),
    hovertemplate="<b>Week Start:</b> %{x|%Y-%m-%d}<br><b>Revenue:</b> $%{y:,.2f}<extra></extra>"
))

fig.update_layout(
    title="Weekly Revenue with Holiday Windows",
    xaxis_title="Week",
    yaxis_title="Revenue",
    hovermode="x unified"
)

fig.show()

# ---------------------------
# Forecast Revenue Scenarios
# ---------------------------
forecast_df = Prediction.RunForecast(weekly_df)
forecast_fig = Prediction.PlotForecast(weekly_df, forecast_df)
forecast_fig.show()


weekly_df = Cleaner.GetWeeklyAggregated()

g1 = DRA.PlotDiscountRevenueRelationship(weekly_df)
g1.show()

g2 = DRA.PlotDiscountSalesRelationship(weekly_df)
g2.show()