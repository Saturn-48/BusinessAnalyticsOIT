import polars as po

# -----------------------------
# 1. Null & empty checks
# -----------------------------
def remove_nulls_and_empties(df: po.DataFrame) -> po.DataFrame:
    df = df.drop_nulls()

    if "Date" in df.columns:
        df = df.filter(po.col("Date").str.len_chars() > 0)

    return df


# -----------------------------
# 2. Duplicate detection
# -----------------------------
def remove_duplicates(df: po.DataFrame) -> po.DataFrame:
    before = df.height
    df = df.unique()
    after = df.height

    print(f"Removed {before - after} duplicate rows")
    return df


# -----------------------------
# 3. Type enforcement
# -----------------------------
def enforce_types(df: po.DataFrame) -> po.DataFrame:
    df = df.with_columns([
        po.col("Store").cast(po.Int64),
        po.col("Weekly_Sales").cast(po.Float64),
        po.col("Holiday_Flag").cast(po.Int64),
        po.col("Temperature").cast(po.Float64),
        po.col("Fuel_Price").cast(po.Float64),
        po.col("CPI").cast(po.Float64),
        po.col("Unemployment").cast(po.Float64),
    ])

    return df


# -----------------------------
# 4. Date parsing
# -----------------------------
def parse_dates(df: po.DataFrame) -> po.DataFrame:
    df = df.with_columns(
        po.col("Date").str.strptime(po.Date, format="%d-%m-%Y")
    )
    return df


# -----------------------------
# 5. Decimal standardization
# -----------------------------
def standardize_decimals(df: po.DataFrame) -> po.DataFrame:
    df = df.with_columns([
        po.col("Weekly_Sales").round(2),
        po.col("Temperature").round(2),
        po.col("Fuel_Price").round(3),
        po.col("CPI").round(3),
        po.col("Unemployment").round(3),
    ])

    return df


# -----------------------------
# 6. Holiday flag labeling
# -----------------------------
def set_holiday_flag(df: po.DataFrame) -> po.DataFrame:
    df = df.with_columns(
        po.when(po.col("Holiday_Flag") == 1)
        .then(po.lit("Holiday"))
        .otherwise(po.lit("Non-Holiday"))
        .alias("Holiday_Flag")
    )

    return df


# -----------------------------
# 7. Outlier identification (IQR)
# -----------------------------
def identify_sales_outliers(df: po.DataFrame) -> po.DataFrame:
    q1 = df.select(po.col("Weekly_Sales").quantile(0.25)).item()
    q3 = df.select(po.col("Weekly_Sales").quantile(0.75)).item()
    iqr = q3 - q1

    upper = q3 + 1.5 * iqr

    outliers = df.filter(po.col("Weekly_Sales") > upper)
    print(f"Identified {outliers.height} high-sales outliers")

    return outliers


# -----------------------------
# 8. Full pipeline
# -----------------------------
def clean_sales_data(df: po.DataFrame):
    df = remove_nulls_and_empties(df)
    df = remove_duplicates(df)
    df = enforce_types(df)
    df = parse_dates(df)
    df = standardize_decimals(df)
    df = set_holiday_flag(df)

    outliers = identify_sales_outliers(df)

    return df, outliers
