import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["overall-unemployment-rate-annual.csv"]

def is_table_exists(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            return True if result else False
    except Exception as ex:
        print(ex)

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE OverallUnemploymentRate (
    year INT NOT NULL,
    unemployment_rate DECIMAL(3,2),
    PRIMARY KEY (year) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        sql = """
INSERT INTO OverallUnemploymentRate 
(year, unemployment_rate)
VALUES 
(?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv(filename)
                for _, row in df.iterrows():
                    year = row["year"]
                    unemployment_rate = row["unemployment_rate"]

                    data = (year, unemployment_rate)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception as ex:
        print(ex)

def main():
    # Fill this with MS SQL Server address
    server = '35.198.240.57'
    database = 'KE5106'
    username = 'admin'
    password = '12345678!'
    # driver='/usr/local/lib/libmsodbcsql.13.dylib'

    # could be different (depends on OS)
    # Code example can be found here: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017
    driver='/usr/local/lib/libmsodbcsql.13.dylib'

    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "OverallUnemploymentRate"):
        create_table(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()