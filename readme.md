# Data Engineer Challenge - SF Fire Incidents

This project aims to ingest and process fire incident call data from San Francisco.

## Data Architecture

The data pipeline follows the medallion architecture and is organized into three layers:

Bronze Layer: A raw copy of the data source, where both historical (backfill) and daily data are loaded.

Silver Layer: This layer handles deduplication. For this scenario, it was assumed the incident_number should be unique, and, therefore was used as deduplication criteria. The latest record, the source of true, is determined using an auto-incrementing ID. Additionally, data type enforcement and constraint validations are applied in this layer. After these transformations, data quality tests are executed.

Gold Layer: This layer contains aggregated results. In this repo, a table summarizing the incidents by battalion and month was created. For materialization, a materialized view was chosen due to its ease of maintenance, deployment, and modification. The output of this tranformation is available at the reports folder.

## Soft delete implementation

For solving the case in which events would be deleted, an update was implemented. In this scenario, the data model ingests an hypothetical `deleted_at` field, containing the timestamp of the deletion event. This field would be materialized in the silver layer, updating the values of the records previously inserted. Then, in the gold layer, the deleted records would be wiped off, since the materialized view strategy performs a full load every time it is refreshed.

## Technologies

Extraction: Python scripts fetch data from an API and save it to a data lake-like file system. In this project, the scripts were executed manually via the terminal. In a production environment, an orchestrator such as Apache Airflow would be used to automate the process and store files in an object storage service.

Data Warehouse: The data processing and modeling are implemented in PostgreSQL. This setup is a simplification, as a real-world scenario would typically use an OLAP database such as Amazon Redshift, Snowflake, or Databricks.

Transformations & Testing: All transformations and data quality tests within the data warehouse are managed using DBT Core.


## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`DBT_POSTGRES_USER`

`DBT_POSTGRES_PASSWORD`

`DBT_POSTGRES_HOST`

`DBT_POSTGRES_PORT`

`DBT_POSTGRES_DB`

`DBT_POSTGRES_SCHEMA`
