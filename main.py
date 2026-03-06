import plotly.graph_objects as go
import Cleaner
import Prediction

# ---------------------------
# Build Database
# ---------------------------
print("Cleaning Set...")
Cleaner.CleanAndExportCSV("Amazon.csv")
print("Building database...")
Cleaner.BuildDatabase()

# ---------------------------
# Load Weekly Data
# ---------------------------
weekly_df = Cleaner.GetWeeklyAggregated()

# ---------------------------
# Calculate Rolling Trend
# ---------------------------
weekly_df["Trend"] = weekly_df["WeeklyRevenue"].rolling(window=8).mean()

# ---------------------------
# Base Revenue Chart with Hover Data
# ---------------------------
fig = go.Figure()

# Weekly Revenue Line
fig.add_trace(
    go.Scatter(
        x=weekly_df["WeekStart"],
        y=weekly_df["WeeklyRevenue"],
        name="Weekly Revenue",
        mode="lines",
        hovertemplate=
            "<b>Week Start:</b> %{x|%Y-%m-%d}<br>" +
            "<b>Week End:</b> %{customdata[0]|%Y-%m-%d}<br>" +
            "<b>Revenue:</b> $%{y:,.2f}<br>" +
            "<b>Avg Discount:</b> %{customdata[1]:.2%}<br>" +
            "<b>Holiday:</b> %{customdata[2]}<br>" +
            "<b>Holiday Window:</b> %{customdata[3]}<extra></extra>",
        customdata=weekly_df[["WeekEnd", "AvgDiscount", "IsHoliday", "HolidayWindow"]].values
    )
)

# Trend Line
fig.add_trace(
    go.Scatter(
        x=weekly_df["WeekStart"],
        y=weekly_df["Trend"],
        name="8 Week Trend",
        mode="lines",
        line=dict(width=4),
        hoverinfo="skip"  # optional: skip hover for trend
    )
)

# Holiday Window Points
holiday_df = weekly_df[weekly_df["HolidayWindow"] == 1]

fig.add_trace(
    go.Scatter(
        x=holiday_df["WeekStart"],
        y=holiday_df["WeeklyRevenue"],
        mode="markers",
        name="Holiday Window",
        marker=dict(color="red", size=8),
        hovertemplate=
            "<b>Week Start:</b> %{x|%Y-%m-%d}<br>" +
            "<b>Week End:</b> %{customdata[0]|%Y-%m-%d}<br>" +
            "<b>Revenue:</b> $%{y:,.2f}<br>" +
            "<b>Avg Discount:</b> %{customdata[1]:.2%}<br>" +
            "<b>Holiday:</b> %{customdata[2]}<br>" +
            "<b>Holiday Window:</b> %{customdata[3]}<extra></extra>",
        customdata=holiday_df[["WeekEnd", "AvgDiscount", "IsHoliday", "HolidayWindow"]].values
    )
)

fig.update_layout(
    title="Weekly Revenue with Holiday Windows",
    xaxis_title="Week",
    yaxis_title="Revenue",
    hovermode="x unified"
)

fig.show()

# ---------------------------
# Forecast
# ---------------------------
forecast_df = Prediction.RunForecast(weekly_df)
forecast_fig = Prediction.PlotForecast(weekly_df, forecast_df)
forecast_fig.show()