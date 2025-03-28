import subprocess
import datetime
import psycopg2
import os
from utils import *

##############################
# Download the historical data from the source datasource till current date
# Load data into local postgres db

current_date = datetime.datetime.now().strftime("%Y%m%d")
url = f"https://data.sfgov.org/api/views/wr8u-xric/rows.csv?fourfour=wr8u-xric&cacheBust=1743067381&date={current_date}&accessType=DOWNLOAD"
csv_path = "data.csv"

def create_table(conn: psycopg2.connect, schema: str, table_name: str) -> int:
    cursor = conn.cursor()
    create_table_query = f'''
        CREATE SCHEMA IF NOT EXISTS {schema};

        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        incident_number TEXT,
        exposure_number TEXT,
        id TEXT,
        address TEXT,
        incident_date TEXT,
        call_number TEXT,
        alarm_dttm TEXT,
        arrival_dttm TEXT,
        close_dttm TEXT,
        city TEXT,
        zipcode TEXT,
        battalion TEXT,
        station_area TEXT,
        box TEXT,
        suppression_units TEXT,
        suppression_personnel TEXT,
        ems_units TEXT,
        ems_personnel TEXT,
        other_units TEXT,
        other_personnel TEXT,
        first_unit_on_scene TEXT,
        estimated_property_loss TEXT,
        estimated_contents_loss TEXT,
        fire_fatalities TEXT,
        fire_injuries TEXT,
        civilian_fatalities TEXT,
        civilian_injuries TEXT,
        number_of_alarms TEXT,
        primary_situation TEXT,
        mutual_aid TEXT,
        action_taken_primary TEXT,
        action_taken_secondary TEXT,
        action_taken_other TEXT,
        detector_alerted_occupants TEXT,
        property_use TEXT,
        area_of_fire_origin TEXT,
        ignition_cause TEXT,
        ignition_factor_primary TEXT,
        ignition_factor_secondary TEXT,
        heat_source TEXT,
        item_first_ignited TEXT,
        human_factors_associated_with_ignition TEXT,
        structure_type TEXT,
        structure_status TEXT,
        floor_of_fire_origin TEXT,
        fire_spread TEXT,
        no_flame_spread TEXT,
        number_of_floors_with_minimum_damage TEXT,
        number_of_floors_with_significant_damage TEXT,
        number_of_floors_with_heavy_damage TEXT,
        number_of_floors_with_extreme_damage TEXT,
        detectors_present TEXT,
        detector_type TEXT,
        detector_operation TEXT,
        detector_effectiveness TEXT,
        detector_failure_reason TEXT,
        automatic_extinguishing_system_present TEXT,
        automatic_extinguishing_system_type TEXT,
        automatic_extinguishing_system_performance TEXT,
        automatic_extinguishing_system_failure_reason TEXT,
        number_of_sprinkler_heads_operating TEXT,
        supervisor_district TEXT,
        neighborhood_district TEXT,
        point TEXT,
        data_as_of TIMESTAMP,
        data_loaded_at TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS data_as_of_idx ON {schema}.{table_name} (data_as_of);
    CLUSTER {schema}.{table_name} USING data_as_of_idx;
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    return 0

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
    
    # Load CSV data
    cursor = conn.cursor()

    sql_copy = f"""COPY {schema}.{table_name} 
        FROM '/{file_path}' 
        DELIMITER ',' 
        CSV HEADER;"""
    cursor.execute(sql_copy)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("CSV data loaded into PostgreSQL.")
    return 0


if __name__=="__main__":
    #Download .csv file to local directory
    #subprocess.run(["curl", "-o", csv_path, url], check=True)
    
    #Move .csv to container to be read by the database
    #subprocess.run(["docker", "cp", csv_path, "my_postgres:data.csv"], check=True)


    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        dbname=DB_NAME
    )

    #Load into postgres
    create_table(conn, bronze_schema, bronze_table_name)
    load_csv_to_postgres(csv_path, bronze_schema, bronze_table_name, conn)
    conn.close()