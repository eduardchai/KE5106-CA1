import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["GDPpercapita_constant2010USD.csv"]

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
CREATE TABLE GDP (
    year INT NOT NULL,
    country_id INT NOT NULL FOREIGN KEY REFERENCES Country(id),
    gdp_value DECIMAL(15,5),
    PRIMARY KEY (year, country_id) 
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
INSERT INTO GDP 
(year, country_id, gdp_value)
VALUES 
(?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("gdp/"+filename)
                df = df.applymap(lambda x: None if x == ".." else x)
                headers = list(df.columns.values)

                years = headers[2:]
                for _, row in df.iterrows():
                    country = row['Country Name']
                    if country in country_dict:
                        country_id = country_dict[country]
                        for year in years:
                            value = row[year]
                            data = (year, country_id, value)
                            cursor.execute(sql, data)

            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into GDP table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "GDP"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")