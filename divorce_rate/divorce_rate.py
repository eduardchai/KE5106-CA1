import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["Divorce Rate.csv"]

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
CREATE TABLE DivorceRate (
    year INT NOT NULL,
    country_id INT NOT NULL FOREIGN KEY REFERENCES Country(id),
    male_general DECIMAL(3,1),
    male_20_24 DECIMAL(3,1),
    male_25_29 DECIMAL(3,1),
    male_30_34 DECIMAL(3,1),
    male_35_39 DECIMAL(3,1),
    male_40_44 DECIMAL(3,1),
    male_45_49 DECIMAL(3,1),
    male_50 DECIMAL(3,1),
    female_general DECIMAL(3,1),
    female_20_24 DECIMAL(3,1),
    female_25_29 DECIMAL(3,1),
    female_30_34 DECIMAL(3,1),
    female_35_39 DECIMAL(3,1),
    female_40_44 DECIMAL(3,1),
    female_45_49 DECIMAL(3,1),
    female_50 DECIMAL(3,1),
    crude_divorce_rate DECIMAL(3,1),
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
INSERT INTO DivorceRate 
(year, country_id, male_general, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50, female_general, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50, crude_divorce_rate)
VALUES 
(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("divorce_rate/"+filename)
                df_transposed = df.set_index('Variables').transpose().reset_index()
                df_transposed = df_transposed.applymap(lambda x: None if str(x) == "na" else x)
                for _, row in df_transposed.iterrows():
                    year = row["index"]
                    country_id = country_dict["Singapore"]
                    male_general = row["Male General Divorce Rate (Per 1,000 Married Resident Aged 20 Years & Over)"]
                    male_20_24 = row["20-24 Years (Per 1,000 Married Resident Males)"]
                    male_25_29 = row["25-29 Years (Per 1,000 Married Resident Males)"]
                    male_30_34 = row["30-34 Years (Per 1,000 Married Resident Males)"]
                    male_35_39 = row["35-39 Years (Per 1,000 Married Resident Males)"]
                    male_40_44 = row["40-44 Years (Per 1,000 Married Resident Males)"]
                    male_45_49 = row["45-49 Years (Per 1,000 Married Resident Males)"]
                    male_50 = row["50 Years And Over (Per 1,000 Married Resident Males)"]
                    female_general = row["Female General Divorce Rate (Per 1,000 Married Resident Aged 20 Years & Over)"]
                    female_20_24 = row["20-24 Years (Per 1,000 Married Resident Females)"]
                    female_25_29 = row["25-29 Years (Per 1,000 Married Resident Females)"]
                    female_30_34 = row["30-34 Years (Per 1,000 Married Resident Females)"]
                    female_35_39 = row["35-39 Years (Per 1,000 Married Resident Females)"]
                    female_40_44 = row["40-44 Years (Per 1,000 Married Resident Females)"]
                    female_45_49 = row["45-49 Years (Per 1,000 Married Resident Females)"]
                    female_50 = row["50 Years And Over (Per 1,000 Married Resident Females)"]
                    crude_divorce_rate = row["Crude Divorce Rate (Per 1,000 Residents)"]

                    data = (year, country_id, male_general, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50, female_general, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50, crude_divorce_rate)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into DivorceRate table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "DivorceRate"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")