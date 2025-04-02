import os
import psycopg2
import logging

# Database connection details
DB_TYPE = "postgresql"
DB_HOST = os.getenv("DBT_POSTGRES_HOST")
DB_USER = os.getenv("DBT_POSTGRES_USER")
DB_PASSWORD = os.getenv("DBT_POSTGRES_PASSWORD")
DB_PORT = os.getenv("DBT_POSTGRES_PORT")
DB_NAME = os.getenv("DBT_POSTGRES_DB")

#Bronze target
bronze_schema= "bronze_sf"
bronze_table_name="fire_incidents"

#Gold target
gold_schema="gold_sf"

def load_csv_to_postgres(file_path: str, schema: str, table_name: str, conn: psycopg2.connect) -> int:
    """
    Loads data from a CSV file into a PostgreSQL table.
    
    Parameters:
    file_path (str): Path to the CSV file to be loaded.
    host (str): Database host.
    user (str): Database user.
    password (str): Database password.
    port (int): Database port.
    dbname (str): Database name.
    schema (str): Database schema where the table resides.
    """

    logging.info(f"Copying {file_path} to table {schema}.{table_name}")

    # Load CSV data
    sql_copy = f"""COPY {schema}.{table_name} 
        FROM '/{file_path}' 
        DELIMITER ',' 
        CSV HEADER;"""
    
    try:
        out_code=0
        cursor = conn.cursor()
        cursor.execute(sql_copy)
        conn.commit()
        logging.info(f"File {file_path} loaded into {schema}.{table_name} succesfully")
    except Exception as e:
        logging.error(f"{e}")
        out_code=-1
    finally:
        cursor.close()
        conn.close()
        return out_code
