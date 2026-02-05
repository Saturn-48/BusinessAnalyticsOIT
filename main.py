import polars as po
import Cleaner

po.Config.set_tbl_rows(10)
po.Config.set_tbl_cols(-1)
po.Config.set_float_precision(2)

df = po.read_csv("Walmart_Store_sales.csv")

print("Original rows:", df.height)
df_clean, sales_outliers = Cleaner.clean_sales_data(df)

print("Cleaned rows:", df_clean.height)

print("\nSample cleaned data:")
print(df_clean.head())

print("\nHigh-sales outliers (likely holidays):")
print(sales_outliers.select([
    "Date",
    "Store",
    "Weekly_Sales",
    "Holiday_Flag"
]))
