import pyodbc
import csv
import gzip
import os
import subprocess

def load(connection):
    try:
        to_csv = []

        with connection.cursor() as cursor:
            sql = "SELECT * FROM HappinessIndex"
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                to_csv.append({
                    'year': row.year,
                    'country': row.country,
                    'region': row.region,
                    'rank': int(row.rank),
                    'happiness_score': float(row.happiness_score),
                    'gdp_per_capita': float(row.gdp_per_capita),
                    'family': float(row.family),
                    'life_expectancy': float(row.life_expectancy),
                    'freedom': float(row.freedom),
                    'government_corruption': float(row.government_corruption),
                    'generosity': float(row.generosity),
                    'dystopia_residual': float(row.dystopia_residual)
                })

        keys = to_csv[0].keys()
        tmp_output_file = '/tmp/fact_happiness.csv'
        with open(tmp_output_file, 'w') as file:
            dict_writer = csv.DictWriter(file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(to_csv)

        print("Run bq load")
        # Shell out to bq CLI to perform BigQuery import.
        subprocess.check_call(
            'bq load --source_format CSV '
            '--replace '
            '--autodetect '
            '{dataset}.{table} {files}'.format(
                dataset="ca_1", table="fact_happiness", files=tmp_output_file
            ).split())

        os.remove(tmp_output_file)

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

    load(connection)

if __name__ == "__main__":
    main()