import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

driver = "{ODBC Driver 17 for SQL Server}"
server = "REQUIEM\\SQLEXPRESS"
database = "AdventureWorksDW2022"

pg_uid = os.getenv('PG_UID')
pg_pwd = os.getenv('PG_PWD')
pg_server = os.getenv('PG_SERVER')
pg_port = os.getenv('PG_PORT')
pg_database = os.getenv('PG_DATABASE')

engine = create_engine(f'postgresql://{pg_uid}:{pg_pwd}@{pg_server}:{pg_port}/{pg_database}')
schema_name = 'dev'

create_schema_sql = text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

try:
    with engine.connect() as connection:
        connection.execute(create_schema_sql)
        print(f"Schema '{schema_name}' created successfully.")
except Exception as e:
    print(f"Error creating schema: {e}")

def extract():
    try:
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        src_engine = create_engine(connection_url)
        src_conn = src_engine.connect()

        # Execute query
        query = """ SELECT t.name as table_name
                    FROM sys.tables t
                    WHERE t.name in ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales') """
        src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']

        for id in src_tables:
            table_name = src_tables[id]
            df = pd.read_sql_query(f'select * FROM {table_name}', src_conn)
            load(df, table_name)

    except Exception as e:
        print("Data extract error: " + str(e))

def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{pg_uid}:{pg_pwd}@{pg_server}:{pg_port}/{pg_database}')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')

        for col in df.columns:
            if df[col].dtype == 'object':
                # Attempt to convert to integers if possible
                try:
                    df[col] = df[col].astype('Int64')
                except ValueError:
                    pass
            elif df[col].dtype == 'float64':
                df[col] = df[col].astype('Float64')

        schema_name = 'dev'  # Replace with your created schema name
        df.to_sql(f'stg_{tbl}', engine, schema=schema_name, if_exists='replace', index=False, chunksize=100000)
        rows_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))


try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
