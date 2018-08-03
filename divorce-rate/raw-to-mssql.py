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

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE DivorceRate (
    year INT NOT NULL,
    male_general DECIMAL(3,1) NOT NULL,
    male_20_24 DECIMAL(3,1) NOT NULL,
    male_25_29 DECIMAL(3,1) NOT NULL,
    male_30_34 DECIMAL(3,1) NOT NULL,
    male_35_39 DECIMAL(3,1) NOT NULL,
    male_40_44 DECIMAL(3,1) NOT NULL,
    male_45_49 DECIMAL(3,1) NOT NULL,
    male_50 DECIMAL(3,1) NOT NULL,
    female_general DECIMAL(3,1) NOT NULL,
    female_20_24 DECIMAL(3,1) NOT NULL,
    female_25_29 DECIMAL(3,1) NOT NULL,
    female_30_34 DECIMAL(3,1) NOT NULL,
    female_35_39 DECIMAL(3,1) NOT NULL,
    female_40_44 DECIMAL(3,1) NOT NULL,
    female_45_49 DECIMAL(3,1) NOT NULL,
    female_50 DECIMAL(3,1) NOT NULL,
    crude_divorce_rate DECIMAL(3,1) NOT NULL,
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
INSERT INTO DivorceRate 
(year, male_general, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50, female_general, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50, crude_divorce_rate)
VALUES 
(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                with open(filename) as csvfile:
                    df = pd.read_csv(filename)
                    df_transposed = df.set_index('Variables').transpose().reset_index()
                    df_transposed = df_transposed.applymap(lambda x: None if str(x) == "." else x)
                    df_transposed.to_csv("test.csv", index=False)
                    for _, row in df_transposed.iterrows():
                        year = row["index"]
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

                        data = (year, male_general, male_20_24, male_25_29, male_30_34, male_35_39, male_40_44, male_45_49, male_50, female_general, female_20_24, female_25_29, female_30_34, female_35_39, female_40_44, female_45_49, female_50, crude_divorce_rate)

                        cursor.execute(sql, data)
            connection.commit()    
    except Exception as ex:
        print(ex)

def main():
    # Fill this with MS SQL Server address
    server = '35.198.240.57'
    database = 'KE5106'
    username = 'admin'
    password = '12345678!'
    # driver='/usr/local/lib/libmsodbcsql.13.dylib'

    # could be different (depends on OS)
    # Code example can be found here: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017
    driver='/usr/local/lib/libmsodbcsql.13.dylib'

    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "DivorceRate"):
        create_table(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()