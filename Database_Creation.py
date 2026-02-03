import pandas as pd
import duckdb as ddb

def NewConn():
    conn = ddb.connect("SALES.duckdb")
    cursor = conn.cursor()
    return conn, cursor


def Setup():

    def ConvertCSV():
        conn, cursor = NewConn()

        query = ("CREATE TABLE IF NOT EXISTS Sales( "
                 "SELECT "
                 "Date, "
                 "Weekly_Sales, "
                 "Holiday_Flag , "
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