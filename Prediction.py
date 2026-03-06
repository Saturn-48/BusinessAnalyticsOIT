import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import plotly.graph_objects as go


def RunForecast(weekly_df):
    df = weekly_df.copy().sort_values("WeekStart")

    model = ExponentialSmoothing(
        df["WeeklyRevenue"],
        trend="add",
        seasonal="add",
        seasonal_periods=52
    )

    fit = model.fit()
    forecast = fit.forecast(104)

    forecast_dates = pd.date_range(
        start=df["WeekStart"].iloc[-1],
        periods=104,
        freq="W"
    )

    forecast_df = pd.DataFrame({
        "WeekStart": forecast_dates,
        "ForecastRevenue": forecast
    })

    return forecast_df


def PlotForecast(history_df, forecast_df):
    """
    Plots historical and forecast revenue with enhanced hover data.
    AvgDiscount, IsHoliday, and HolidayWindow are carried forward for forecast.
    """
    fig = go.Figure()

    # Historical Revenue
    fig.add_trace(
        go.Scatter(
            x=history_df["WeekStart"],
            y=history_df["WeeklyRevenue"],
            name="Historical Revenue",
            mode="lines",
            hovertemplate=
                "<b>Week Start:</b> %{x|%Y-%m-%d}<br>" +
                "<b>Week End:</b> %{customdata[0]|%Y-%m-%d}<br>" +
                "<b>Revenue:</b> $%{y:,.2f}<br>" +
                "<b>Avg Discount:</b> %{customdata[1]:.2%}<br>" +
                "<b>Holiday:</b> %{customdata[2]}<br>" +
                "<b>Holiday Window:</b> %{customdata[3]}<extra></extra>",
            customdata=history_df[["WeekEnd", "AvgDiscount", "IsHoliday", "HolidayWindow"]].values
        )
    )

    # Carry forward last known values for forecast hover
    last_row = history_df.iloc[-1]
    forecast_df = forecast_df.copy()
    forecast_df["WeekEnd"] = forecast_df["WeekStart"] + pd.Timedelta(days=6)
    forecast_df["AvgDiscount"] = last_row["AvgDiscount"]
    forecast_df["IsHoliday"] = 0
    forecast_df["HolidayWindow"] = 0

    # Forecast Revenue
    fig.add_trace(
        go.Scatter(
            x=forecast_df["WeekStart"],
            y=forecast_df["ForecastRevenue"],
            name="Forecast Revenue",
            mode="lines",
            hovertemplate=
                "<b>Week Start:</b> %{x|%Y-%m-%d}<br>" +
                "<b>Week End:</b> %{customdata[0]|%Y-%m-%d}<br>" +
                "<b>Forecast Revenue:</b> $%{y:,.2f}<br>" +
                "<b>Avg Discount:</b> %{customdata[1]:.2%}<br>" +
                "<b>Holiday:</b> %{customdata[2]}<br>" +
                "<b>Holiday Window:</b> %{customdata[3]}<extra></extra>",
            customdata=forecast_df[["WeekEnd", "AvgDiscount", "IsHoliday", "HolidayWindow"]].values
        )
    )

    fig.update_layout(
        title="Revenue Forecast (Holt-Winters)",
        xaxis_title="Week",
        yaxis_title="Revenue",
        hovermode="x unified"
    )

    return fig