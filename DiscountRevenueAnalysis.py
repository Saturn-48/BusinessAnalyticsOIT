import pandas as pd
import plotly.express as px
import numpy as np


# ----------------------------------------
# Graph 1: Discount vs Revenue
# ----------------------------------------
def PlotDiscountRevenueRelationship(weekly_df):

    df = weekly_df.copy()

    df["AvgDiscount"] = pd.to_numeric(df["AvgDiscount"])
    df["WeeklyRevenue"] = pd.to_numeric(df["WeeklyRevenue"])

    df = df.dropna()

    # Convert to percent
    df["DiscountPercent"] = df["AvgDiscount"] * 100

    # Correlation
    corr = df["DiscountPercent"].corr(df["WeeklyRevenue"])

    # Regression line
    z = np.polyfit(df["DiscountPercent"], df["WeeklyRevenue"], 1)
    p = np.poly1d(z)

    df["Trend"] = p(df["DiscountPercent"])

    fig = px.scatter(
        df,
        x="DiscountPercent",
        y="WeeklyRevenue",
        title=f"Discount vs Revenue (Correlation = {corr:.2f})",
        labels={
            "DiscountPercent": "Average Discount (%)",
            "WeeklyRevenue": "Weekly Revenue"
        },
        hover_data=["WeekStart"]
    )

    fig.add_scatter(
        x=df["DiscountPercent"],
        y=df["Trend"],
        mode="lines",
        name="Revenue Trend"
    )

    return fig


# ----------------------------------------
# Graph 2: Discount vs Sales Volume
# ----------------------------------------
def PlotDiscountSalesRelationship(weekly_df):

    df = weekly_df.copy()

    df["AvgDiscount"] = pd.to_numeric(df["AvgDiscount"])
    df["WeeklyUnits"] = pd.to_numeric(df["WeeklyUnits"])

    df = df.dropna()

    df["DiscountPercent"] = df["AvgDiscount"] * 100

    corr = df["DiscountPercent"].corr(df["WeeklyUnits"])

    z = np.polyfit(df["DiscountPercent"], df["WeeklyUnits"], 1)
    p = np.poly1d(z)

    df["Trend"] = p(df["DiscountPercent"])

    fig = px.scatter(
        df,
        x="DiscountPercent",
        y="WeeklyUnits",
        title=f"Discount vs Units Sold (Correlation = {corr:.2f})",
        labels={
            "DiscountPercent": "Average Discount (%)",
            "WeeklyUnits": "Units Sold Per Week"
        },
        hover_data=["WeekStart"]
    )

    fig.add_scatter(
        x=df["DiscountPercent"],
        y=df["Trend"],
        mode="lines",
        name="Sales Trend"
    )

    return fig