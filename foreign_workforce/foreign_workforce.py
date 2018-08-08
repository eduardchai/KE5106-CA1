import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["foreign-workforce-numbers.csv"]

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
CREATE TABLE ForeignWorkforce (
    year INT NOT NULL,
    employment_pass INT,
    s_pass INT,
    work_permit_total INT,
    work_permit_fdw INT,
    work_permit_construction INT,
    other_passes INT,
    total_workforce INT,
    total_workforce_exc_fdw INT,
    total_workforce_exc_fdw_construction INT,
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
INSERT INTO ForeignWorkforce 
(year,employment_pass,s_pass,work_permit_total,work_permit_fdw,work_permit_construction,other_passes,total_workforce,total_workforce_exc_fdw,total_workforce_exc_fdw_construction)
VALUES 
(?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("foreign_workforce/"+filename)
                df_transposed = df.set_index('Pass Type').transpose().reset_index()
                df_transposed = df_transposed.applymap(lambda x: None if x == "na" else x)
                for _, row in df_transposed.iterrows():
                    year = int(row["index"].split("-")[1]) + 2000
                    employment_pass = row["Employment Pass (EP)"].replace(",", "")
                    s_pass = row["S Pass"].replace(",", "")
                    work_permit_total = row["Work Permit (Total)"].replace(",", "")
                    work_permit_fdw = row["Work Permit (Foreign Domestic Worker)"].replace(",", "")
                    work_permit_construction = row["Work Permit (Construction)"].replace(",", "")
                    other_passes = row["Other Work Passes2"].replace(",", "")
                    total_workforce = row["Total Foreign Workforce"].replace(",", "")
                    total_workforce_exc_fdw = row["Total Foreign Workforce(excluding Foreign Domestic Workers) "].replace(",", "")
                    total_workforce_exc_fdw_construction = row["Total Foreign Workforce(excluding Foreign Domestic Workers & Construction) "].replace(",", "")

                    data = (year,employment_pass,s_pass,work_permit_total,work_permit_fdw,work_permit_construction,other_passes,total_workforce,total_workforce_exc_fdw,total_workforce_exc_fdw_construction)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into ForeignWorkforce table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "ForeignWorkforce"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")