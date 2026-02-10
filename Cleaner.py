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
        SELECT Date, 
        Store, 
        Weekly_Sales, 
        IsHoliday, 
        Year, 
        Month, 
        Week,
        max, 
        min, 
        mean, 
        median, 
        std, 
        Total_MarkDown 
        FROM read_xlsx('amazon_sales_dataset.xlsx', header=TRUE)
        WHERE Date IS NOT NULL AND 
            Store IS NOT NULL AND 
            Weekly_Sales IS NOT NULL AND 
            IsHoliday IS NOT NULL AND 
            Year IS NOT NULL AND 
            Month IS NOT NULL AND 
            Week IS NOT NULL AND 
            max IS NOT NULL AND 
            min IS NOT NULL AND 
            mean IS NOT NULL AND 
            median IS NOT NULL AND 
            std IS NOT NULL AND 
            Total_MarkDown IS NOT NULL"""

    cursor.execute(query)
    conn.commit()
    conn.close()



# if a holiday is 1, set it to Holiday. If 0, set as Non-Holiday
def ChangeHoliday():
    conn, cursor = NewConn()
    query = """
    UPDATE Sales 
    
    """