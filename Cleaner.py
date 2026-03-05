import duckdb as ddb

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
    except:
        print("Spatial Not Installed. Installing Spatial...")
        cursor.execute("INSTALL Spatial;")
        cursor.execute("LOAD Spatial;")
        print("Done!")
        conn.close()


# This generates the db and removes null values
def CreateDB():
    conn, cursor = NewConn()
    CheckSpatial()
    query = """
    CREATE TABLE IF NOT EXISTS Sales AS  
        SELECT CAST(OrderDate AS DATE) AS OrderDate, 
            UnitPrice, 
            Discount, 
            TotalAmount 
        FROM read_csv('Amazon.csv', header=TRUE)
        WHERE OrderDate IS NOT NULL AND 
            OrderDate IS NOT NULL AND 
            Discount IS NOT NULL AND 
            TotalAmount IS NOT NULL"""

    cursor.execute(query)
    conn.commit()
    conn.close()



# if a holiday is 1, set it to Holiday. If 0, set as Non-Holiday
def ChangeHoliday():
    conn, cursor = NewConn()

    #change the datatype from int to varchar in prep for the conversion
    alter_query = "ALTER TABLE Sales ADD COLUMN IF NOT EXISTS IsHoliday INT"
    cursor.execute(alter_query)
    conn.commit()

    # change the datatype from int to varchar in prep for the conversion
    alter_query = "ALTER TABLE Sales ADD COLUMN IF NOT EXISTS HolidayWindow INT"
    cursor.execute(alter_query)
    conn.commit()


    query = ("UPDATE Sales "
    "SET IsHoliday = "
        "CASE "
            "WHEN " 
                "(EXTRACT(MONTH FROM OrderDate) = 1  AND EXTRACT(DAY FROM OrderDate) = 1)  OR "
                "(EXTRACT(MONTH FROM OrderDate) = 2  AND EXTRACT(DAY FROM OrderDate) = 14) OR "
                "(EXTRACT(MONTH FROM OrderDate) = 7  AND EXTRACT(DAY FROM OrderDate) = 4)  OR "
                "(EXTRACT(MONTH FROM OrderDate) = 10 AND EXTRACT(DAY FROM OrderDate) = 31) OR "
                "(EXTRACT(MONTH FROM OrderDate) = 12 AND EXTRACT(DAY FROM OrderDate) = 25) "
            "THEN 1 "
            "ELSE 0 "
        "END")

    cursor.execute(query)
    conn.commit()
    results = cursor.fetchall()
    conn.close()
    return results



def GetWeeklyAggregated():
    conn, cursor = NewConn()

    query = """
    WITH weekly AS (
        SELECT 
            DATE_TRUNC('week', OrderDate) AS WeekStart, 
            MAX(IsHoliday) AS IsHoliday,  -- if any day is holiday, week = holiday 
            SUM(TotalAmount) AS WeeklyRevenue 
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
        w.IsHoliday, 
        w.WeeklyRevenue, 
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


def CleanAll():
    CreateDB()
    return ChangeHoliday()


def GetAll():
    CleanAll()
    conn, cursor = NewConn()
    query = "SELECT * FROM Sales"

    cursor.execute(query)
    results = cursor.fetchdf()
    conn.close()
    return results