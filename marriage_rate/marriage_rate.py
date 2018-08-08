import pyodbc
import csv
import urllib3
import pandas as pd
import math

FILENAMES = ["Marriage Rate.csv"]

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
CREATE TABLE MarriageRate (
    year INT NOT NULL,
    male_general DECIMAL(8,2),
    male_15_19 DECIMAL(8,2),
    male_20_24 DECIMAL(8,2),
    male_25_29 DECIMAL(8,2),
    male_30_34 DECIMAL(8,2),
    male_35_39 DECIMAL(8,2),
    male_40_44 DECIMAL(8,2),
    male_45_49 DECIMAL(8,2),
    male_50_54 DECIMAL(8,2),
    male_55_59 DECIMAL(8,2),
    male_60_64 DECIMAL(8,2),
    male_65 DECIMAL(8,2),
    female_general DECIMAL(8,2),
    female_15_19 DECIMAL(8,2),
    female_20_24 DECIMAL(8,2),
    female_25_29 DECIMAL(8,2),
    female_30_34 DECIMAL(8,2),
    female_35_39 DECIMAL(8,2),
    female_40_44 DECIMAL(8,2),
    female_45_49 DECIMAL(8,2),
    female_50_54 DECIMAL(8,2),
    female_55_59 DECIMAL(8,2),
    female_60_64 DECIMAL(8,2),
    female_65 DECIMAL(8,2),
    crude_marriage_rate DECIMAL(8,2),
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
INSERT INTO MarriageRate 
(year, male_general, male_15_19, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50_54, male_55_59, male_60_64, male_65, female_general, female_15_19, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50_54, female_55_59, female_60_64, female_65, crude_marriage_rate)
VALUES 
(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv("marriage_rate/"+filename)
                df_transposed = df.set_index('Variables').transpose().reset_index()
                df_transposed = df_transposed.applymap(lambda x: None if x == "na" else x)
                for _, row in df_transposed.iterrows():
                    year = row["index"]
                    male_general = row["Male General Marriage Rate (Per 1,000 Unmarried Resident Males 15-49)"]
                    male_15_19 = row["15-19 Years (Per 1,000 Unmarried Resident Males)"]
                    male_20_24 = row["20-24 Years (Per 1,000 Unmarried Resident Males)"]
                    male_25_29 = row["25-29 Years (Per 1,000 Unmarried Resident Males)"]
                    male_30_34 = row["30-34 Years (Per 1,000 Unmarried Resident Males)"]
                    male_35_39 = row["35-39 Years (Per 1,000 Unmarried Resident Males)"]
                    male_40_44 = row["40-44 Years (Per 1,000 Unmarried Resident Males)"]
                    male_45_49 = row["45-49 Years (Per 1,000 Unmarried Resident Males)"]
                    male_50_54 = row["50-54 Years (Per 1,000 Unmarried Resident Males)"]
                    male_55_59 = row["55-59 Years (Per 1,000 Unmarried Resident Males)"]
                    male_60_64 = row["60-64 Years (Per 1,000 Unmarried Resident Males)"]
                    male_65 = row["65 Years & Over (Per 1,000 Unmarried Resident Males)"]
                    male_65 = None if math.isnan(male_65) else male_65
                    female_general = row["Female General Marriage Rate (Per 1,000 Unmarried Resident Females 15-49)"]
                    female_15_19 = row["15-19 Years (Per 1,000 Unmarried Resident Females)"]
                    female_20_24 = row["20-24 Years (Per 1,000 Unmarried Resident Females)"]
                    female_25_29 = row["25-29 Years (Per 1,000 Unmarried Resident Females)"]
                    female_30_34 = row["30-34 Years (Per 1,000 Unmarried Resident Females)"]
                    female_35_39 = row["35-39 Years (Per 1,000 Unmarried Resident Females)"]
                    female_40_44 = row["40-44 Years (Per 1,000 Unmarried Resident Females)"]
                    female_45_49 = row["45-49 Years (Per 1,000 Unmarried Resident Females)"]
                    female_50_54 = row["50-54 Years (Per 1,000 Unmarried Resident Females)"]
                    female_55_59 = row["55-59 Years (Per 1,000 Unmarried Resident Females)"]
                    female_60_64 = row["60-64 Years (Per 1,000 Unmarried Resident Females)"]
                    female_65 = row["65 Years & Over (Per 1,000 Unmarried Resident Females)"]
                    female_65 = None if math.isnan(female_65) else female_65
                    crude_marriage_rate = row["Crude Marriage Rate (Per 1,000 Residents)"]

                    data = (year, male_general, male_15_19, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50_54, male_55_59, male_60_64, male_65, female_general, female_15_19, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50_54, female_55_59, female_60_64, female_65, crude_marriage_rate)

                    cursor.execute(sql, data)
            connection.commit()    
    except Exception:
        raise

def main(server, database, username, password, driver):
    print("Loading data into MarriageRate table")
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "MarriageRate"):
        create_table(connection)

    try:
        populate_data(connection)
    except Exception:
        print("Error: duplicate data found!")