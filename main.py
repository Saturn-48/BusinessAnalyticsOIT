import polars as po
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import Cleaner

# -----------------------------
# Load & clean data (POLARS)
# -----------------------------
df = po.read_csv("Walmart_Store_sales.csv")
df_clean, outliers = Cleaner.clean_sales_data(df)

# Convert to Python-native records (NO pyarrow)
records = df_clean.to_dicts()

# -----------------------------
# Aggregations (still no pandas)
# -----------------------------

# 1. Weekly total sales (all stores)
weekly_totals = (
    df_clean
    .group_by("Date")
    .agg(po.col("Weekly_Sales").sum().alias("Total_Sales"))
    .sort("Date")
    .to_dicts()
)

# 2. Average sales: Holiday vs Non-Holiday
holiday_avg = (
    df_clean
    .group_by("Holiday_Flag")
    .agg(po.col("Weekly_Sales").mean().alias("Avg_Sales"))
    .to_dicts()
)

# 3. Weekly Holiday
weekly_holiday_flag = (
    df_clean
    .group_by("Date")
    .agg(
        po.when(po.col("Holiday_Flag") == "Holiday")
        .then(po.lit("Holiday Week"))
        .otherwise(po.lit("Non-Holiday Week"))
        .max()
        .alias("Holiday_Week")
    )
    .sort("Date")
    .to_dicts()
)

holiday_map = {r["Date"]: r["Holiday_Week"] for r in weekly_holiday_flag}

for r in weekly_totals:
    r["Holiday_Week"] = holiday_map.get(r["Date"], "Non-Holiday Week")


# 4. Top 5 stores by total sales
top_stores = (
    df_clean
    .group_by("Store")
    .agg(po.col("Weekly_Sales").sum().alias("Total_Sales"))
    .sort("Total_Sales", descending=True)
    .head(5)
)

top_store_ids = set(top_stores["Store"].to_list())

top_store_weekly = (
    df_clean
    .filter(po.col("Store").is_in(top_store_ids))
    .group_by(["Date", "Store"])
    .agg(po.col("Weekly_Sales").sum())
    .sort("Date")
    .to_dicts()
)

# -----------------------------
# Plotly dashboard (1 page)
# -----------------------------
fig = make_subplots(
    rows=3,
    cols=1,
    subplot_titles=[
        "Total Weekly Sales (All Stores)",
        "Average Weekly Sales: Holiday vs Non-Holiday",
        "Weekly Sales – Top 5 Stores"
    ]
)

fig.add_trace(
    go.Scatter(
        x=[r["Date"] for r in weekly_totals],
        y=[r["Total_Sales"] for r in weekly_totals],
        mode="lines",
        name="Total Sales",
        customdata=[r["Holiday_Week"] for r in weekly_totals],
        hovertemplate=(
            "<b>Date:</b> %{x}<br>"
            "<b>Total Sales:</b> $%{y:,.0f}<br>"
            "<b>Week Type:</b> %{customdata}<extra></extra>"
        )
    ),
    row=1,
    col=1
)

# --- Chart 2: Holiday vs Non-Holiday ---
fig.add_trace(
    go.Bar(
        x=[r["Holiday_Flag"] for r in holiday_avg],
        y=[r["Avg_Sales"] for r in holiday_avg],
        name="Avg Sales"
    ),
    row=2,
    col=1
)

# --- Chart 3: Top 5 stores ---
for store_id in top_store_ids:
    store_data = [r for r in top_store_weekly if r["Store"] == store_id]

    fig.add_trace(
        go.Scatter(
            x=[r["Date"] for r in store_data],
            y=[r["Weekly_Sales"] for r in store_data],
            mode="lines",
            name=f"Store {store_id}"
        ),
        row=3,
        col=1
    )

# -----------------------------
# Layout & output
# -----------------------------
fig.update_layout(
    height=1200,
    title_text="Walmart Sales – Descriptive Analysis",
    showlegend=True
)

fig.show()
