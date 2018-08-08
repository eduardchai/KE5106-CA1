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
	uc.year, 
	uc.unemployed, 
	wf.total_workforce, 
	pl.total_residents,
	CAST(CAST(uc.unemployed AS decimal)*100/CAST(pl.total_residents AS decimal) as decimal(5,2)) as unemployment_rate,
	CAST(CAST(wf.total_workforce_exc_fdw_construction AS decimal)*100/CAST(pl.total_residents AS decimal) as decimal(5,2)) as foreign_workforce_per_100_resident
FROM UnemployedCitizens as uc, ForeignWorkforce as wf, Population as pl
WHERE uc.year = wf.year AND uc.year = pl.year
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                to_csv.append({
                    'year': row.year,
                    'unemployed': float(row.unemployed),
                    'total_workforce': float(row.total_workforce),
                    'total_residents': float(row.total_residents),
                    'unemployment_rate': float(row.unemployment_rate),
                    'foreign_workforce_per_100_resident': float(row.foreign_workforce_per_100_resident)
                })

        keys = to_csv[0].keys()
        tmp_output_file = '/tmp/fact_employment.csv'
        with open(tmp_output_file, 'w') as file:
            dict_writer = csv.DictWriter(file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(to_csv)

        print("Load to BQ table: fact_employment")
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