import pyodbc
import csv
import urllib3

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

def create_happiness_index(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE HappinessIndex (
    year INT NOT NULL,
    country NVARCHAR(50) NOT NULL,
    region NVARCHAR(50) NOT NULL,
    rank INT NOT NULL,
    standard_error DECIMAL(8,5) NOT NULL,
    gdp_per_capita DECIMAL(8,5) NOT NULL,
    family DECIMAL(8,5) NOT NULL,
    life_expectancy DECIMAL(8,5) NOT NULL,
    freedom DECIMAL(8,5) NOT NULL,
    government_corruption DECIMAL(8,5) NOT NULL,
    generosity DECIMAL(8,5) NOT NULL,
    dystopia_residual DECIMAL(8,5) NOT NULL,
    PRIMARY KEY (year, country) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        for filename in FILENAMES:
            with connection.cursor() as cursor:
                with open(filename) as csvfile:
                    reader = csv.reader(csvfile)
                    next(reader)
                    for row in reader:
                        year = filename.split("-")[1].replace(".csv", "")
                        country = row[0]
                        region = row[1]
                        rank = row[2]
                        standard_error = row[3]
                        gdp_per_capita = row[4]
                        family = row[5]
                        life_expectancy = row[6]
                        freedom = row[7]
                        government_corruption = row[8]
                        generosity = row[9]
                        dystopia_residual = row[10]
                        sql = f"""
INSERT INTO HappinessIndex 
(year, country, region, rank, standard_error, gdp_per_capita, family, life_expectancy, freedom, government_corruption, generosity, dystopia_residual)
VALUES 
({year}, '{country}', '{region}', {rank}, {standard_error}, {gdp_per_capita}, {family}, {life_expectancy}, {freedom}, {government_corruption}, {generosity}, {dystopia_residual})""" 
                        cursor.execute(sql)
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

    if not is_table_exists(connection, "HappinessIndex"):
        create_happiness_index(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()