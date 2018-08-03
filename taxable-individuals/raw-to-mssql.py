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

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE TaxableIndividuals (
    year INT NOT NULL,
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
        sql = """
INSERT INTO TaxableIndividuals 
(year,income_group,resident_type,number_of_taxpayers,assessable_income,chargeable_income,net_tax_assessed)
VALUES 
(?,?,?,?,?,?,?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv(filename)
                df = df.applymap(lambda x: None if x == "na" else x)
                for _, row in df.iterrows():
                    year = row["year_of_assessment"]
                    income_group = row["assessed_income_group"]
                    resident_type = row["resident_type"]
                    number_of_taxpayers = row["number_of_taxpayers"]
                    assessable_income = row["assessable_income"]
                    chargeable_income = row["chargeable_income"]
                    net_tax_assessed = row["net_tax_assessed"]

                    data = (year,income_group,resident_type,number_of_taxpayers,assessable_income,chargeable_income,net_tax_assessed)
                    
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

    if not is_table_exists(connection, "TaxableIndividuals"):
        create_table(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()