import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["country.csv"]

def is_table_exists(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            return True if result else False
    except Exception as ex:
        print(ex)

def get_region(connection):
    region = {}
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, name FROM CountryRegion"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                region[row[1]] = row[0]
    except Exception as ex:
        print(ex)

    return region

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE Country (
    id INT IDENTITY(1,1),
    name NVARCHAR(100) NOT NULL,
    region_id INT NOT NULL FOREIGN KEY REFERENCES CountryRegion(id),
    PRIMARY KEY (id)
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        region_dict = get_region(connection)
        sql = """
INSERT INTO Country 
(name, region_id)
VALUES 
(?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("country/"+filename)
                for _, row in df.iterrows():
                    name = row["country"]
                    region = row["region"]
                    region_id = region_dict[region]
                    data = (name,region_id)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into Country table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "Country"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")