from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd
import plotly.graph_objects as go


# --------------------------------------------------
# Generate future holiday flags
# --------------------------------------------------
def GenerateFutureCalendar(last_date, steps):

    future_dates = pd.date_range(
        start=last_date + pd.Timedelta(weeks=1),
        periods=steps,
        freq="W"
    )

    holidays = [(1, 1), (2, 14), (7, 4), (11, 25), (12, 25)]

    is_holiday = []

    for d in future_dates:
        flag = 0
        for m, day in holidays:
            h = pd.Timestamp(year=d.year, month=m, day=day)
            if d <= h <= d + pd.Timedelta(days=6):
                flag = 1

        is_holiday.append(flag)

    df = pd.DataFrame({
        "WeekStart": future_dates,
        "IsHoliday": is_holiday
    })
    df["HolidayWindow"] = 0

    return df


# --------------------------------------------------
# Apply post-holiday discount removal
# --------------------------------------------------
def ApplyPostHolidayDiscountRule(df, avg_discount):

    df = df.copy()

    df["AvgDiscount"] = avg_discount

    last_holiday = None

    for i in range(len(df)):

        if df.iloc[i]["IsHoliday"] == 1:
            last_holiday = i

        if last_holiday is not None:

            weeks_after = i - last_holiday

            if 1 <= weeks_after <= 2:
                df.at[df.index[i], "AvgDiscount"] = 0

    return df


# --------------------------------------------------
# Run Forecast
# --------------------------------------------------
def RunForecast(weekly_df):

    df = weekly_df.copy().sort_values("WeekStart")

    # Force numeric
    df["WeeklyRevenue"] = pd.to_numeric(df["WeeklyRevenue"])
    df["AvgDiscount"] = pd.to_numeric(df["AvgDiscount"])
    df["IsHoliday"] = pd.to_numeric(df["IsHoliday"])
    df["HolidayWindow"] = pd.to_numeric(df["HolidayWindow"])

    df = df.dropna()

    y = df["WeeklyRevenue"]

    exog = df[["AvgDiscount", "IsHoliday", "HolidayWindow"]]

    # -----------------------------
    # SARIMAX MODEL
    # -----------------------------
    model = SARIMAX(
        y,
        exog=exog,
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 52),
        enforce_stationarity=False,
        enforce_invertibility=False
    )

    fit = model.fit(disp=False)

    # -----------------------------
    # FUTURE DATA
    # -----------------------------
    steps = 104

    future_calendar = GenerateFutureCalendar(df["WeekStart"].iloc[-1], steps)

    avg_discount = df["AvgDiscount"].mean()

    # Baseline
    future_base = future_calendar.copy()
    future_base["AvgDiscount"] = avg_discount

    forecast_base = fit.forecast(
        steps=steps,
        exog=future_base[["AvgDiscount", "IsHoliday", "HolidayWindow"]]
    )

    # No post-holiday discounts
    future_no_discount = ApplyPostHolidayDiscountRule(
        future_calendar,
        avg_discount
    )

    forecast_no_discount = fit.forecast(
        steps=steps,
        exog=future_no_discount[["AvgDiscount", "IsHoliday", "HolidayWindow"]]
    )

    # Convert to dataframe
    forecast_df = pd.DataFrame({
        "WeekStart": future_calendar["WeekStart"],
        "ForecastBaseline": forecast_base.values,
        "ForecastNoPostHolidayDiscount": forecast_no_discount.values
    })

    return forecast_df


# --------------------------------------------------
# Plot Forecast
# --------------------------------------------------
def PlotForecast(history_df, forecast_df):

    fig = go.Figure()

    # Historical
    fig.add_trace(go.Scatter(
        x=history_df["WeekStart"],
        y=history_df["WeeklyRevenue"],
        mode="lines",
        name="Historical Revenue"
    ))

    # Baseline forecast
    fig.add_trace(go.Scatter(
        x=forecast_df["WeekStart"],
        y=forecast_df["ForecastBaseline"],
        mode="lines",
        name="Forecast Baseline"
    ))

    # No post holiday discount
    fig.add_trace(go.Scatter(
        x=forecast_df["WeekStart"],
        y=forecast_df["ForecastNoPostHolidayDiscount"],
        mode="lines",
        name="No Post-Holiday Discount"
    ))

    fig.update_layout(
        title="Revenue Forecast Comparison",
        xaxis_title="Week",
        yaxis_title="Revenue",
        hovermode="x unified"
    )

    return fig