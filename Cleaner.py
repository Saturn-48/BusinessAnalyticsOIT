import duckdb as ddb
import polars as po

def NewConn():
    conn = ddb.connect("AmazonSales.duckdb")
    cursor = conn.cursor()
    return conn, cursor


def CheckSpatial():
    conn, cursor = NewConn()
    try:
        cursor.execute("LOAD Spatial;")
        print("Spatial Installed. Loading Spatial...")
        conn.close()
        print("Done!")
    except:
        print("Spatial Not Installed. Installing Spatial...")
        cursor.execute("INSTALL Spatial;")
        cursor.execute("LOAD Spatial;")
        print("Done!")
        conn.close()


# This generates the db and removes null values. This combines departments to their respective stores per date/entry
def CreateDB():
    conn, cursor = NewConn()
    CheckSpatial()
    query = """
    CREATE TABLE IF NOT EXISTS Sales AS 
        SELECT 
        CAST(Date AS DATE) AS Date, 
        CAST(Store AS INT) AS Store, 
        SUM(Weekly_Sales) AS WeeklySales, 
        MAX(IsHoliday) AS IsHoliday, 
        SUM(Total_MarkDown) AS TotalMarkDown 
        FROM read_xlsx('amazon_sales_dataset.xlsx', header=TRUE)
        WHERE Date IS NOT NULL AND 
            Store IS NOT NULL AND 
            Weekly_Sales IS NOT NULL AND 
            IsHoliday IS NOT NULL AND 
            Total_MarkDown IS NOT NULL
        GROUP BY Date, Store"""

    cursor.execute(query)
    conn.commit()
    conn.close()


def CleanDupes():
    conn, cursor = NewConn()

    query = """
    DELETE FROM Sales
    WHERE (Date, Store) IN(
        SELECT Date, Store
        FROM Sales
        GROUP BY Date, Store
        HAVING COUNT(*) > 1)
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results



def CleanAll():
    CreateDB()
    CleanDupes()


def GetCleanFrame():
    conn, cursor = NewConn()

    query = """SELECT *,
            CASE
            WHEN IsHoliday = 1 THEN 'Holiday'
            ELSE 'Non-Holiday'
            END AS HolidayType
            FROM Sales"""

    df = po.read_database(query, conn)
    return df
