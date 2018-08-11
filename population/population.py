import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["population.totalresidents.csv"]

def is_table_exists(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            return True if result else False
    except Exception as ex:
        print(ex)

def get_country(connection):
    country = {}
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, name FROM Country"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                country[row[1]] = row[0]
    except Exception as ex:
        print(ex)

    return country

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE Population (
    year INT NOT NULL,
    country_id INT NOT NULL FOREIGN KEY REFERENCES Country(id),
    total_residents INT,
    PRIMARY KEY (year) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        country_dict = get_country(connection)
        sql = """
INSERT INTO Population 
(year, country_id, total_residents)
VALUES 
(?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("population/"+filename)
                df_transposed = df.set_index('Year').transpose().reset_index()
                # print(df_transposed)
                for _, row in df_transposed.iterrows():
                    year = row[0]
                    country_id = country_dict["Singapore"]
                    total_residents = row["Total Residents"].replace(",","")
                    data = (year, country_id, total_residents)
                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into Population table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "Population"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")