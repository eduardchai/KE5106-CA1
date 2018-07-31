# KE5106-CA1

## Prerequisite

1. Python >= 3.6

## SDK and Libraries Dependencies

1. Install ODBC Driver for your OS: [Windows](https://www.microsoft.com/en-us/sql-server/developer-get-started/python/windows/) | [Mac](https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/) | [Others](https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-2017)

2. Install Google Cloud SDK for your OS: [Windows](https://cloud.google.com/sdk/docs/#windows) | [Mac](https://cloud.google.com/sdk/docs/#mac) | [Others](https://cloud.google.com/sdk/docs/)

3. Install pyodbc
```
pip install pyodbc
```

## How to run script

1. Open one of the script, for example `happiness-index/raw-to-mssql.py`
2. Fill this part with your MS SQL Server address:
```
# Fill this with MS SQL Server address
server = ''
database = ''
username = ''
password = ''
```
3. Run the script
```
python happiness-index/raw-to-mssql.py
```