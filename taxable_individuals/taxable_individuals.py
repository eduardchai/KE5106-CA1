import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["taxable-individuals-by-assessed-income-group.csv"]

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
CREATE TABLE TaxableIndividuals (
    year INT NOT NULL,
    country_id INT NOT NULL FOREIGN KEY REFERENCES Country(id),
    income_group NVARCHAR(50) NOT NULL,
    resident_type NVARCHAR(50) NOT NULL,
    number_of_taxpayers INT,
    assessable_income INT,
    chargeable_income INT,
    net_tax_assessed INT,
    PRIMARY KEY (year, income_group, resident_type) 
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
INSERT INTO TaxableIndividuals 
(year,country_id,income_group,resident_type,number_of_taxpayers,assessable_income,chargeable_income,net_tax_assessed)
VALUES 
(?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("taxable_individuals/"+filename)
                df = df.applymap(lambda x: None if x == "na" else x)
                for _, row in df.iterrows():
                    year = row["year_of_assessment"]
                    country_id = country_dict["Singapore"]
                    income_group = row["assessed_income_group"]
                    resident_type = row["resident_type"]
                    number_of_taxpayers = row["number_of_taxpayers"]
                    assessable_income = row["assessable_income"]
                    chargeable_income = row["chargeable_income"]
                    net_tax_assessed = row["net_tax_assessed"]

                    data = (year,country_id,income_group,resident_type,number_of_taxpayers,assessable_income,chargeable_income,net_tax_assessed)
                    
                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into TaxableIndividuals table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "TaxableIndividuals"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")