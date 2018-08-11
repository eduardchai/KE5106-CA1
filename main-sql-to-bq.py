from fact_tables import fact_employment,fact_happiness,fact_living_cost,fact_marital_status

if __name__ == "__main__":
    # Fill this with MS SQL Server address
    server = ''
    database = ''
    username = ''
    password = ''
    bq_dataset = ''
    
    # ODBC Driver could be different (depends on OS)
    # Code example can be found here: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017
    driver='{ODBC Driver 13 for SQL Server}' # For Windows
    # driver='/usr/local/lib/libmsodbcsql.13.dylib' # For MacOS

    fact_employment.main(server, database, username, password, driver, bq_dataset, "fact_employment")
    fact_happiness.main(server, database, username, password, driver, bq_dataset, "fact_happiness")
    fact_living_cost.main(server, database, username, password, driver, bq_dataset, "fact_living_cost")
    fact_marital_status.main(server, database, username, password, driver, bq_dataset, "fact_marital_status")