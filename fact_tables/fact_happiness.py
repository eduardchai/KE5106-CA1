import pyodbc
import csv
import gzip
import os
import subprocess

def load(connection, bq_dataset, bq_tablename):
    try:
        to_csv = []

        with connection.cursor() as cursor:
            sql = """
SELECT
	H.year,
	C.name as country,
	CR.name as region,
	H.rank,
	H.happiness_score,
	H.gdp_per_capita,
	H.family,
	H.life_expectancy,
	H.freedom,
	H.government_corruption,
	H.generosity,
	H.dystopia_residual
FROM HappinessIndex H
LEFT JOIN Country C ON H.country_id = C.id
LEFT JOIN CountryRegion CR ON C.region_id = CR.id
            """
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

        print("Load to BQ table: fact_happiness")
        # Shell out to bq CLI to perform BigQuery import.
        subprocess.check_call(
            'bq load --source_format CSV '
            '--replace '
            '--autodetect '
            '{dataset}.{table} {files}'.format(
                dataset=bq_dataset, table=bq_tablename, files=tmp_output_file
            ).split())

        os.remove(tmp_output_file)

    except Exception:
        raise

def main(server, database, username, password, driver, bq_dataset, bq_tablename):
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)

    try:
        load(connection, bq_dataset, bq_tablename)
    except Exception as ex:
        print(ex)