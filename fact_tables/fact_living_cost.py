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
    cpi.year, 
    cpi.value as cpi_index, 
    CAST(CAST(ti.number_of_taxpayers AS decimal)*100/CAST(pl.total_residents AS decimal) as decimal(5,2)) as taxpayers_rate 
FROM ConsumerPriceIndex as cpi, TaxableIndividuals as ti, Population as pl
WHERE 
    cpi.year = ti.year AND 
    cpi.year = pl.year AND 
    cpi.level_id = 1 AND 
    ti.resident_type = 'Tax Resident' AND 
    ti.income_group = '20,001 - 25,000';
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                to_csv.append({
                    'year': row.year,
                    'cpi_index': float(row.cpi_index),
                    'taxpayers_rate': float(row.taxpayers_rate),
                })

        keys = to_csv[0].keys()
        tmp_output_file = '/tmp/fact_living_cost.csv'
        with open(tmp_output_file, 'w') as file:
            dict_writer = csv.DictWriter(file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(to_csv)

        print("Load to BQ table: fact_living_cost")
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