from cause_of_death import cause_of_death
from consumer_price_index import consumer_price_index,cpi_level
from country import country,region
from divorce_rate import divorce_rate
from foreign_workforce import foreign_workforce
from gdp import gdp
from happiness_index import happiness_index
from marital_status import marital_status
from marriage_rate import marriage_rate
from overall_unemployment_rate import overall_unemployment_rate
from population import population
from taxable_individuals import taxable_individuals
from unemployed_citizens_annual import unemployed_citizens_annual


if __name__ == "__main__":
    # Fill this with MS SQL Server address
    server = '35.240.235.125'
    database = 'ke5106'
    username = 'admin'
    password = '12345678'
    
    # ODBC Driver could be different (depends on OS)
    # Code example can be found here: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017
    # driver='{ODBC Driver 13 for SQL Server}' # For Windows
    driver='/usr/local/lib/libmsodbcsql.13.dylib' # For MacOS

    cause_of_death.main(server, database, username, password, driver)
    cpi_level.main(server, database, username, password, driver)
    consumer_price_index.main(server, database, username, password, driver)
    region.main(server, database, username, password, driver)
    country.main(server, database, username, password, driver)
    divorce_rate.main(server, database, username, password, driver)
    foreign_workforce.main(server, database, username, password, driver)
    gdp.main(server, database, username, password, driver)
    happiness_index.main(server, database, username, password, driver)
    marital_status.main(server, database, username, password, driver)
    marriage_rate.main(server, database, username, password, driver)
    overall_unemployment_rate.main(server, database, username, password, driver)
    population.main(server, database, username, password, driver)
    taxable_individuals.main(server, database, username, password, driver)
    unemployed_citizens_annual.main(server, database, username, password, driver)