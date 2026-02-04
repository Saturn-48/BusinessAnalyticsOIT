import duckdb as ddb


def NewConn():
    conn = ddb.connect("SALES.duckdb")
    cursor = conn.cursor()
    return conn, cursor


def Setup():

    def ConvertCSV():
        conn, cursor = NewConn()

        query = ("CREATE TABLE IF NOT EXISTS Sales AS "
                 "SELECT "
                 "Date AS Date, "
                 "Weekly_Sales AS Weekly_Sales, "
                 "Holiday_Flag AS Holiday_Flag, "
                 "Temperature AS Temperature, "
                 "Fuel_Price AS Fuel_Price, "
                 "Unemployment AS Unemployment "
                 "FROM read_csv('Walmart_Store_Sales.csv', HEADER=True) "
                 "WHERE Date IS NOT NULL AND "
                 "Weekly_Sales IS NOT NULL AND "
                 "Holiday_Flag IS NOT NULL AND "
                 "Temperature IS NOT NULL AND "
                 "Fuel_Price IS NOT NULL AND "
                 "Unemployment IS NOT NULL")

        cursor.execute(query)
        conn.commit()
        conn.close()

    def FormatData():
        conn, cursor = NewConn()
        q1 = ("UPDATE Sales "
              "SET Weekly_Sales = ROUND(Weekly_Sales, 2) ")
        q2 = ("UPDATE Sales "
             "SET Temperature = ROUND(Temperature, 2)")

        q3 = ("UPDATE Sales "
             "SET Fuel_Price = ROUND(Fuel_Price, 3)")

        q4 = ("UPDATE Sales "
             "SET Unemployment = ROUND(Unemployment, 1)")
        cursor.execute(q1)
        cursor.execute(q2)
        cursor.execute(q3)
        cursor.execute(q4)

        conn.commit()
        conn.close()


    print("Starting conversion...")
    ConvertCSV()
    print("Converted to Database...\nStarting data cleaning...")
    FormatData()
    print("Data cleaned successfully!")