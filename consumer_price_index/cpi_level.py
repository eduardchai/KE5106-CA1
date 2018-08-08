import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["cpi-level.csv"]

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
CREATE TABLE ConsumerPriceIndexLevel (
    id INT IDENTITY(1,1),
    description NVARCHAR(100) NOT NULL,
    PRIMARY KEY (id) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        sql = """
INSERT INTO ConsumerPriceIndexLevel 
(description)
VALUES 
(?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("consumer_price_index/" + filename)
                for _, row in df.iterrows():
                    desc = row["cpi_level"]

                    data = (desc)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into ConsumerPriceIndexLevel table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "ConsumerPriceIndexLevel"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")