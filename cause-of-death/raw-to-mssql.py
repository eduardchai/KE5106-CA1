import pyodbc
import csv
import urllib3
import pandas as pd

FILENAMES = ["causeofdeath-2000.csv","causeofdeath-2010.csv","causeofdeath-2015.csv","causeofdeath-2016.csv"]

def is_table_exists(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            return True if result else False
    except Exception as ex:
        print(ex)

def get_country(connection):
    country = {}
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, name FROM Country"
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                country[row[1]] = row[0]
    except Exception as ex:
        print(ex)

    return country

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            sql = """
CREATE TABLE CauseOfDeath (
    year INT NOT NULL,
    country_id INT NOT NULL,
    population INT NOT NULL,
    all_causes DECIMAL(8,2),
    nutritional_conditions DECIMAL(8,2),
    infectious_diseases DECIMAL(8,2),
    respiratory_infectious DECIMAL(8,2),
    maternal_conditions DECIMAL(8,2),
    neonatal_conditions DECIMAL(8,2),
    nutritional_deficiencies DECIMAL(8,2),
    noncommunicable_diseases DECIMAL(8,2),
    malignant_neoplasms DECIMAL(8,2),
    other_neoplasms DECIMAL(8,2),
    diabetes_mellitus DECIMAL(8,2),
    endocrine_blood_immune_disorders DECIMAL(8,2),
    mental_substance_use_disorders DECIMAL(8,2),
    neurological_conditions DECIMAL(8,2),
    sense_organ_diseases DECIMAL(8,2),
    cardiovascular_diseases DECIMAL(8,2),
    respiratory_diseases DECIMAL(8,2),
    digestive_diseases DECIMAL(8,2),
    genitourinary_diseases DECIMAL(8,2),
    skin_diseases DECIMAL(8,2),
    musculoskeletal_diseases DECIMAL(8,2),
    congenital_anomalies DECIMAL(8,2),
    oral_conditions DECIMAL(8,2),
    sudden_infant_death_syndrome DECIMAL(8,2),
    injuries DECIMAL(8,2),
    unintentional_injuries DECIMAL(8,2),
    intentional_injuries DECIMAL(8,2),
    PRIMARY KEY (year, country_id) 
)
            """
            cursor.execute(sql)
        
        connection.commit()
    except Exception as ex:
        print(ex)

def populate_data(connection):
    try:
        country_dict = get_country(connection)
        sql = """
INSERT INTO CauseOfDeath 
(year, country_id, population, all_causes, nutritional_conditions, infectious_diseases, respiratory_infectious, maternal_conditions, neonatal_conditions, nutritional_deficiencies, noncommunicable_diseases, malignant_neoplasms, other_neoplasms, diabetes_mellitus, endocrine_blood_immune_disorders, mental_substance_use_disorders, neurological_conditions, sense_organ_diseases, cardiovascular_diseases, respiratory_diseases, digestive_diseases, genitourinary_diseases, skin_diseases, musculoskeletal_diseases, congenital_anomalies, oral_conditions, sudden_infant_death_syndrome, injuries, unintentional_injuries, intentional_injuries)
VALUES 
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        for filename in FILENAMES:
            with connection.cursor() as cursor:
                df = pd.read_csv(filename)
                df_transposed = df.set_index('Country').transpose().reset_index()
                df_transposed = df_transposed.applymap(lambda x: None if str(x) == "." else x)
                for _, row in df_transposed.iterrows():
                    year = filename.split("-")[1].replace(".csv", "")
                    country = row["index"]
                    if country in country_dict:
                        country_id = country_dict[country]
                        population = row[" Population ('000) (2) "]
                        all_causes = row["All Causes"]
                        nutritional_conditions = row["I. Communicable, maternal, perinatal and nutritional conditions"]
                        infectious_diseases = row["A. Infectious and parasitic diseases"]
                        respiratory_infectious = row["B. Respiratory Infectious "]
                        maternal_conditions = row["C. Maternal conditions"]
                        neonatal_conditions = row["D. Neonatal conditions"]
                        nutritional_deficiencies = row["E. Nutritional deficiencies"]
                        noncommunicable_diseases = row["II. Noncommunicable diseases"]
                        malignant_neoplasms = row["A. Malignant neoplasms"]
                        other_neoplasms = row["B. Other neoplasms"]
                        diabetes_mellitus = row["C. Diabetes mellitus"]
                        endocrine_blood_immune_disorders = row["D. Endocrine, blood, immune disorders"]
                        mental_substance_use_disorders = row["E. Mental and substance use disorders"]
                        neurological_conditions = row["F. Neurological conditions"]
                        sense_organ_diseases = row["G. Sense organ diseases"]
                        cardiovascular_diseases = row["H. Cardiovascular diseases"]
                        respiratory_diseases = row["I. Respiratory diseases"]
                        digestive_diseases = row["J. Digestive diseases"]
                        genitourinary_diseases = row["K. Genitourinary diseases"]
                        skin_diseases = row["L. Skin diseases"]
                        musculoskeletal_diseases = row["M. Musculoskeletal diseases"]
                        congenital_anomalies = row["N. Congenital anomalies"]
                        oral_conditions = row["O. Oral conditions"]
                        sudden_infant_death_syndrome = row["P. Sudden infant death syndrome"]
                        injuries = row["III. Injuries"]
                        unintentional_injuries = row["A. Unintentional injuries"]
                        intentional_injuries = row["B. Intentional injuries"]

                        data = (year, country_id, population, all_causes, nutritional_conditions, infectious_diseases, respiratory_infectious, maternal_conditions, neonatal_conditions, nutritional_deficiencies, noncommunicable_diseases, malignant_neoplasms, other_neoplasms, diabetes_mellitus, endocrine_blood_immune_disorders, mental_substance_use_disorders, neurological_conditions, sense_organ_diseases, cardiovascular_diseases, respiratory_diseases, digestive_diseases, genitourinary_diseases, skin_diseases, musculoskeletal_diseases, congenital_anomalies, oral_conditions, sudden_infant_death_syndrome, injuries, unintentional_injuries, intentional_injuries)

                        cursor.execute(sql, data)
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

    if not is_table_exists(connection, "CauseOfDeath"):
        create_table(connection)

    populate_data(connection)

if __name__ == "__main__":
    main()