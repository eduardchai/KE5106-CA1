import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["happiness-2015.csv", "happiness-2016.csv", "happiness-2017.csv", "happiness-2018.csv"]

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
CREATE TABLE HappinessIndex (
    year INT NOT NULL,
    country_id INT NOT NULL,
    rank INT NOT NULL,
    happiness_score DECIMAL(8,5) NOT NULL,
    gdp_per_capita DECIMAL(8,5) NOT NULL,
    family DECIMAL(8,5) NOT NULL,
    life_expectancy DECIMAL(8,5) NOT NULL,
    freedom DECIMAL(8,5) NOT NULL,
    government_corruption DECIMAL(8,5) NOT NULL,
    generosity DECIMAL(8,5) NOT NULL,
    dystopia_residual DECIMAL(8,5) NOT NULL,
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
INSERT INTO HappinessIndex 
(year,country_id,rank,happiness_score,gdp_per_capita,family,life_expectancy,freedom,government_corruption,generosity,dystopia_residual)
VALUES 
(?,?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("happiness_index/"+filename)
                for _, row in df.iterrows():
                    year = filename.split("-")[1].replace(".csv", "")
                    country = row["Country"]
                    country_id = country_dict[country]
                    rank = row["Happiness Rank"]
                    happiness_score = row["Happiness Score"]
                    gdp_per_capita = row["Economy (GDP per Capita)"]
                    family = row["Family"]
                    life_expectancy = row["Health (Life Expectancy)"]
                    freedom = row["Freedom"]
                    government_corruption = row["Trust (Government Corruption)"]
                    generosity = row["Generosity"]
                    dystopia_residual = row["Dystopia Residual"]

                    data = (year,country_id,rank,happiness_score,gdp_per_capita,family,life_expectancy,freedom,government_corruption,generosity,dystopia_residual)

                    cursor.execute(sql, data)
            connection.commit()
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into HappinessIndex table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "HappinessIndex"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")