import pyodbc
import csv
import urllib3

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
    id int IDENTITY(1,1),
    year int NOT NULL,
    happiness_index decimal(5,3) NOT NULL,
    PRIMARY KEY (id) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        with connection.cursor() as cursor:
            with open('happiness-index.csv') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                for row in reader:
                    year = row[0]
                    index = float(row[1])
                    sql = f"INSERT INTO HappinessIndex (year, happiness_index) VALUES ({year}, {index})" 
                    cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def main():
    # Fill this with MS SQL Server address
    server = ''
    database = ''
    username = ''
    password = ''
    # driver='/usr/local/lib/libmsodbcsql.13.dylib'

    # could be different (depends on OS)
    # Code example can be found here: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017
    driver='{ODBC Driver 13 for SQL Server}'

    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    if not is_table_exists(connection, "HappinessIndex"):
        create_happiness_index(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()