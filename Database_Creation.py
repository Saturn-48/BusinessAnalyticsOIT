import duckdb as ddb


def NewConn():
    conn = ddb.connect("SALES.duckdb")
    cursor = conn.cursor()
    return conn, cursor


def Setup():

    def ConvertCSV():
        conn, cursor = NewConn()

        query = ("CREATE TABLE IF NOT EXISTS Sales("
                 "SELECT "
                 "Date, "
                 "Weekly_Sales, "
                 "Holiday_Flag, "
                 "Temperature, "
                 "Fuel_Price, "
                 "Unemployment "
                 "FROM read_csv() "
                 "WHERE Date IS NOT NULL AND "
                 "Weekly_Sales IS NOT NULL AND "
                 "Holiday_Flag IS NOT NULL AND "
                 "Temperature IS NOT NULL AND "
                 "Fuel_Price IS NOT NULL AND "
                 "Unemployment IS NOT NULL)")

        cursor.execute(query)
        conn.commit()
        conn.close()

    def FormatData():
        conn, cursor = NewConn()
        query = ("UPDATE Sales "
                 "SET Weekly_Sales = ROUND(Weekly_Sales, 2)"
                 "UPDATE Sales "
                 "SET Temperature = ROUND(Temperature, 2)"
                 "UPDATE Sales "
                 "SET Fuel_Price = ROUND(Fuel_Price, 3)"
                 "UPDATE Sales "
                 "SET Unemployment = ROUND(Unemployment, 1)")
        cursor.executemany(query)
        conn.commit()
        conn.close()


    print("Starting conversion...")
    ConvertCSV()
    print("Converted to Database...\nStarting data cleaning...")
    FormatData()
    print("Data cleaned successfully!")