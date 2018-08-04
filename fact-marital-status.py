import pyodbc
import csv
import gzip
import os
import subprocess

def load(connection):
    try:
        to_csv = []

        with connection.cursor() as cursor:
            sql = """
SELECT  
	P.year,
    M.total_single,
	CAST(CAST(M.total_single AS DECIMAL) / CAST(P.total_residents AS DECIMAL) * 100 AS DECIMAL(5,2)) AS single_rate,
    M.total_married,
	CAST(CAST(M.total_married AS DECIMAL) / CAST(P.total_residents AS DECIMAL) * 100 AS DECIMAL(5,2)) AS married_rate,
    M.total_divorced_separated,
	CAST(CAST(M.total_divorced_separated AS DECIMAL) / CAST(P.total_residents AS DECIMAL) * 100 AS DECIMAL(5,2)) AS divorce_rate,
    P.total_residents
FROM Population P
INNER JOIN MaritalStatus M
ON P.year = M.year
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                to_csv.append({
                    'year': row.year,
                    'total_single': float(row.total_single),
                    'single_rate': float(row.single_rate),
                    'total_married': float(row.total_married),
                    'married_rate': float(row.married_rate),
                    'total_divorced_separated': float(row.total_divorced_separated),
                    'divorce_rate': float(row.divorce_rate),
                    'total_residents': float(row.total_residents)
                })

        keys = to_csv[0].keys()
        tmp_output_file = '/tmp/fact_marital_status.csv'
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
                dataset="ca_1", table="fact_marital_status", files=tmp_output_file
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