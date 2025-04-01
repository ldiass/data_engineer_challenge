from datetime import datetime
import psycopg2
import requests
from utils import *

##############################
# Download the historical data from the source datasource till current date
# Load data into local postgres db
file_prefix="daily_data/sfgov"


def fetch_sfgov_data(data_as_of: str, file_prefix:str):
    """
    Fetch data from the SF Gov API where data_as_of is greater than the given timestamp.
    
    :param data_as_of: Timestamp in ISO format (YYYY-MM-DDTHH:MM:SS.000)
    :return: Pandas DataFrame with filtered data
    """
    base_url = "https://data.sfgov.org/resource/wr8u-xric.csv"
    
    # Build the query parameter
    query_url = f"{base_url}?$where=data_as_of>'{data_as_of}'"
    
    try:
        response = requests.get(query_url)
        response.raise_for_status()  # Check for HTTP request errors

        current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        filename = f"{file_prefix}_{current_timestamp}.csv"

        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        return 0
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def get_latest_load_timestamp(conn, schema, table_name):
        cursor = conn.cursor()
        cursor.execute(f"select max(data_as_of) from {schema}.{table_name}")
        r=cursor.fetchall()
        cursor.close()
        latest_load=r[0][0].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        return latest_load

if __name__=="__main__":

    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        dbname=DB_NAME
    )
    
    last_load_ts=get_latest_load_timestamp(conn, bronze_schema, bronze_table_name)
    fetch_sfgov_data(last_load_ts, file_prefix)
    load_csv_to_postgres(file_prefix, bronze_schema, bronze_table_name, conn)
    conn.close()