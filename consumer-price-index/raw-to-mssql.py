import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["consumer-price-index-base-year-2014-100-annual.csv"]

def is_table_exists(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            return True if result else False
    except Exception as ex:
        print(ex)

def get_level(connection):
    level = {}
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, description FROM ConsumerPriceIndexLevel"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                level[row[1]] = row[0]
    except Exception as ex:
        print(ex)

    return level

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE ConsumerPriceIndex (
    year INT NOT NULL,
    level_id INT NOT NULL,
    value DECIMAL(8,3),
    PRIMARY KEY (year, level_id) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        level_dict = get_level(connection)
        sql = """
INSERT INTO ConsumerPriceIndex 
(year, level_id, value)
VALUES 
(?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv(filename)
                df = df.applymap(lambda x: None if x == "na" else x)
                for _, row in df.iterrows():
                    year = row["year"]
                    level = row["level_1"]
                    level_id = level_dict[level]
                    value = row["value"]

                    data = (year, level_id, value)

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

    if not is_table_exists(connection, "ConsumerPriceIndex"):
        create_table(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()