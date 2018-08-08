import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["singles.csv"]

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
CREATE TABLE MaritalStatus (
    year INT NOT NULL,
    total INT,
    total_single INT,
    total_married INT,
    total_widowed INT,
    total_divorced_separated INT,
    male INT,
    male_single INT,
    male_married INT,
    male_widowed INT,
    male_divorced_separated INT,
    female INT,
    female_single INT,
    female_married INT,
    female_widowed INT,
    female_divorced_separated INT,
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
INSERT INTO MaritalStatus 
(year,total,total_single,total_married,total_widowed,total_divorced_separated,male,male_single,male_married,male_widowed,male_divorced_separated,female,female_single,female_married,female_widowed,female_divorced_separated)
VALUES 
(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("marital_status/"+filename)
                df_transposed = df.set_index('Variables').transpose().reset_index()
                df_transposed = df_transposed.applymap(lambda x: None if x == "na" else x)
                for _, row in df_transposed.iterrows():
                    year = row["index"]
                    total = row[1].replace(",", "")
                    total_single = row[2].replace(",", "")
                    total_married = row[3].replace(",", "")
                    total_widowed = row[4].replace(",", "")
                    total_divorced_separated = row[5].replace(",", "")
                    male = row[6].replace(",", "")
                    male_single = row[7].replace(",", "")
                    male_married = row[8].replace(",", "")
                    male_widowed = row[9].replace(",", "")
                    male_divorced_separated = row[10].replace(",", "")
                    female = row[11].replace(",", "")
                    female_single = row[12].replace(",", "")
                    female_married = row[13].replace(",", "")
                    female_widowed = row[14].replace(",", "")
                    female_divorced_separated = row[15].replace(",", "")

                    data = (year,total,total_single,total_married,total_widowed,total_divorced_separated,male,male_single,male_married,male_widowed,male_divorced_separated,female,female_single,female_married,female_widowed,female_divorced_separated)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into MaritalStatus table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "MaritalStatus"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")