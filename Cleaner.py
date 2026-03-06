import duckdb as ddb
import pandas as pd

def NewConn():
    conn = ddb.connect("AmazonSales.duckdb")
    return conn, conn.cursor()


# ---------------------------
# Build Database
# ---------------------------


def CleanAndExportCSV(raw_csv="Amazon.csv", clean_csv="Amazon_clean.csv"):
    df = pd.read_csv(raw_csv, header=None, names=[
        "OrderID","OrderDate","CustomerID","CustomerName","ProductID","ProductName",
        "Category","Brand","Quantity","UnitPrice","Discount","Tax","ShippingCost",
        "TotalAmount","PaymentMethod","OrderStatus","City","State","Country","SellerID"
    ], low_memory=False)

    # ---------------------------
    # 1. Drop cancelled orders
    # ---------------------------
    df = df[df["OrderStatus"].str.lower() != "cancelled"]

    # ---------------------------
    # 2. Convert numeric columns
    # ---------------------------
    numeric_cols = ["Quantity","UnitPrice","Discount","Tax","ShippingCost","TotalAmount"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=numeric_cols)

    # ---------------------------
    # 3. Filter invalid numbers
    # ---------------------------
    df = df[df["TotalAmount"] > 0]
    df = df[(df["Discount"] >= 0) & (df["Discount"] <= 1)]
    df = df[df["Quantity"] > 0]

    # ---------------------------
    # 4. Standardize country names
    # ---------------------------
    country_map = {
        "United States": "United States",
        "US": "United States",
        "USA": "United States",
        "India": "India",
    }
    df["Country"] = df["Country"].map(lambda x: country_map.get(str(x).strip(), "Unknown"))

    # ---------------------------
    # 4b. Fix countries based on city/state (Option 1)
    # ---------------------------
    us_states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
                 "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
                 "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
                 "WA", "WV", "WI", "WY"]

    def fix_country(row):
        if row["State"] in us_states:
            return "United States"
        return row["Country"]  # keep original if not US state

    df["Country"] = df.apply(fix_country, axis=1)


    # ---------------------------
    # 5. Standardize dates
    # ---------------------------
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")
    df = df.dropna(subset=["OrderDate"])

    # ---------------------------
    # 6. Trim whitespace
    # ---------------------------
    str_cols = ["CustomerName","ProductName","Category","Brand","City","State","Country","PaymentMethod","OrderStatus"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # ---------------------------
    # 7. Save cleaned CSV
    # ---------------------------
    df.to_csv(clean_csv, index=False)
    print(f"Cleaned CSV exported to {clean_csv}")
    return df




def BuildDatabase():

    conn, cursor = NewConn()

    cursor.execute(f"""
    CREATE OR REPLACE TABLE Sales AS
    SELECT
        CAST(OrderDate AS DATE) AS OrderDate,
        Quantity,
        UnitPrice,
        Discount,
        TotalAmount,
        City,
        State,
        Country
    FROM read_csv('Amazon_clean.csv', header=TRUE)
    WHERE
        OrderDate IS NOT NULL
        AND Discount BETWEEN 0 AND 1
        AND TotalAmount > 0
        AND Quantity > 0
    """)

    conn.commit()
    AddHolidayFlags()
    conn.close()


# ---------------------------
# Holiday Flags
# ---------------------------

def AddHolidayFlags():

    conn, cursor = NewConn()
    cursor.execute("ALTER TABLE Sales ADD COLUMN IF NOT EXISTS IsHoliday INT")

    cursor.execute("""
    UPDATE Sales
    SET IsHoliday =
        CASE
            WHEN (EXTRACT(MONTH FROM OrderDate) = 1 AND EXTRACT(DAY FROM OrderDate) = 1)
            OR   (EXTRACT(MONTH FROM OrderDate) = 2 AND EXTRACT(DAY FROM OrderDate) = 14)
            OR   (EXTRACT(MONTH FROM OrderDate) = 7 AND EXTRACT(DAY FROM OrderDate) = 4)
            OR   (EXTRACT(MONTH FROM OrderDate) = 11 AND EXTRACT(DAY FROM OrderDate) = 25)
            OR   (EXTRACT(MONTH FROM OrderDate) = 12 AND EXTRACT(DAY FROM OrderDate) = 25)
            THEN 1
            ELSE 0
        END
    """)

    conn.commit()
    conn.close()


# ---------------------------
# Weekly Aggregation
# ---------------------------

def GetWeeklyAggregated():
    conn, cursor = NewConn()

    query = """
    WITH weekly AS (
        SELECT
            DATE_TRUNC('week', OrderDate) AS WeekStart,
            MAX(IsHoliday) AS IsHoliday,
            SUM(TotalAmount) AS WeeklyRevenue,
            AVG(Discount) AS AvgDiscount
        FROM Sales
        GROUP BY 1
    ),
    holiday_weeks AS (
        SELECT WeekStart
        FROM weekly
        WHERE IsHoliday = 1
    )
    SELECT
        w.WeekStart,
        w.WeekStart + INTERVAL '6 days' AS WeekEnd,  -- <-- week end
        w.IsHoliday,
        w.WeeklyRevenue,
        w.AvgDiscount,  -- <-- average discount
        CASE
            WHEN w.IsHoliday = 1 THEN 1
            WHEN w.WeekStart IN (
                SELECT WeekStart - INTERVAL '1 week' FROM holiday_weeks
            ) THEN 1
            WHEN w.WeekStart IN (
                SELECT WeekStart - INTERVAL '2 week' FROM holiday_weeks
            ) THEN 1
            ELSE 0
        END AS HolidayWindow
    FROM weekly w
    ORDER BY w.WeekStart
    """

    df = cursor.execute(query).fetchdf()
    conn.close()
    return df