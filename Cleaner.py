import duckdb as ddb
import polars as po


def NewConn():
    conn = ddb.connect("AmazonSales.duckdb")
    return conn


def CreateDB():
    conn = NewConn()

    query = """
    CREATE OR REPLACE TABLE Sales AS 
    WITH Base AS (
        SELECT 
            CAST(Date AS DATE) AS Date, 
            SUM(Weekly_Sales) AS WeeklySales, 
            CAST(MAX(IsHoliday) AS INT) AS IsHoliday, 
            SUM(Total_MarkDown) AS TotalMarkDown
        FROM read_xlsx('amazon_sales_dataset.xlsx', header=TRUE)
        WHERE Date IS NOT NULL
          AND Weekly_Sales IS NOT NULL
          AND IsHoliday IS NOT NULL
          AND Total_MarkDown IS NOT NULL
          AND EXTRACT(YEAR FROM Date) > 2019
        GROUP BY Date
    )

    SELECT *,
        CASE 
            WHEN IsHoliday = 1 THEN 1
            WHEN LEAD(IsHoliday, 1) OVER (ORDER BY Date) = 1 THEN 1
            WHEN LEAD(IsHoliday, 2) OVER (ORDER BY Date) = 1 THEN 1
            ELSE 0
        END AS HolidayWindow
    FROM Base
    ORDER BY Date;
    """

    conn.execute(query)
    conn.close()
    print("Database Created / Rebuilt")


# ---------------- DATA FOR PLOTS ----------------

def GetCleanFrame():
    conn = NewConn()

    query = """
    SELECT *,
        CASE
            WHEN HolidayWindow = 1 THEN 'HolidayWeek'
            ELSE 'Non-HolidayWeek'
        END AS HolidayType
    FROM Sales
    ORDER BY Date
    """

    df = po.read_database(query, conn)
    conn.close()
    return df


# ---------------- OVERALL SUMMARY STATS ----------------

def GetOverallStats():
    conn = NewConn()

    query = """
    SELECT
        AVG(WeeklySales) AS MeanSales,
        MEDIAN(WeeklySales) AS MedianSales,
        STDDEV(WeeklySales) AS StdSales,
        MIN(WeeklySales) AS MinSales,
        MAX(WeeklySales) AS MaxSales
    FROM Sales
    """

    df = po.read_database(query, conn)
    conn.close()
    return df


# ---------------- HOLIDAY VS NON-HOLIDAY STATS ----------------

def GetHolidayStats():
    conn = NewConn()

    query = """
    SELECT
        HolidayWindow,
        AVG(WeeklySales) AS MeanSales,
        MEDIAN(WeeklySales) AS MedianSales,
        STDDEV(WeeklySales) AS StdSales,
        MIN(WeeklySales) AS MinSales,
        MAX(WeeklySales) AS MaxSales
    FROM Sales
    GROUP BY HolidayWindow
    """

    df = po.read_database(query, conn)
    conn.close()
    return df


def GetCorrelation():
    conn = NewConn()

    query = """
    SELECT corr(WeeklySales, TotalMarkDown) AS Sales_Markdown_Correlation
    FROM Sales
    """

    df = po.read_database(query, conn)
    conn.close()
    return df
