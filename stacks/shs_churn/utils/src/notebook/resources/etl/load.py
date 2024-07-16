from google.cloud import bigquery
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import time


def create_temp_table(
    bq_client, 
    project_id: str,
    dataset_id: str,
    table_id: str,
    df_to_load: pd.DataFrame,
    hours_to_expiry: int = 6
) -> str:
    """        
    Creates a temporary table in BigQuery and inserts data from a pandas DataFrame.

    Parameters:
        - client: bigquery client instance
        - project_id (str): The ID of the BigQuery project.
        - dataset_id (str): The ID of the BigQuery dataset where the temporary table will be created.
        - table_id (str): The ID of the temporary table.
        - df_to_load (pd.DataFrame): The pandas DataFrame containing the data to be loaded into the temporary table.
        - hours_to_expiry (int, optional): The number of hours until the temporary table expires. Default is 6 hours.
    
    Returns:
        - Name of temporary table
   
    """

    # create temp table
    temp_table_name = f'{project_id}.{dataset_id}.{table_id}_temp_{int(time.time())}'
    tmp_table_def = bigquery.Table(temp_table_name)
    tmp_table_def.expires = datetime.now() + timedelta(hours=hours_to_expiry)
    # bq_client = bigquery.Client(project=project_id)
    table = bq_client.create_table(tmp_table_def)
    time.sleep(2)
    print(
        f'Created temp table {table.project}.{table.dataset_id}.{table.table_id}'
    )

    # insert data to temp table
    config = bigquery.LoadJobConfig()
    config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = bq_client.load_table_from_dataframe(df_to_load, tmp_table_def, job_config=config)
    job.result()  # wait for the job to complete.
    time.sleep(2)
    table = bq_client.get_table(tmp_table_def)
    print(
        f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {tmp_table_def}'
    )

    return temp_table_name


def insert_from_temp_table(
    bq_client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    temp_table_id: str,
    current_part_dt: str,
    pth_drop_query: Path,
    pth_insert_query: Path,
):
    """
    Inserts data from a temporary table into a main table in BigQuery.

    Parameters:
        - project_id (str): The ID of the BigQuery project.
        - dataset_id (str): The ID of the BigQuery dataset where the tables reside.
        - table_id (str): The ID of the main table where the data will be inserted.
        - temp_table_id (str): The ID of the temporary table containing the data to be inserted.
        - current_part_dt (str): The current partition date to insert.
        - pth_drop_query (Path): The path to the SQL file containing the query to drop existing data.
        - pth_insert_query (Path): The path to the SQL file containing the query to insert data.

    Returns:
        None
    """

    # bq_client = bigquery.Client(project=project_id)

    # remove existing part_dt to avoid duplicates
    sql_drop = pth_drop_query.read_text().format(
        project_id=project_id, dataset_id=dataset_id, 
        table_id=table_id, current_part_dt=current_part_dt
    )
    query_job = bq_client.query(sql_drop)
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        print(row.name)
    time.sleep(2)

    # insert data to main table
    sql_insert = pth_insert_query.read_text().format(
        project_id=project_id, dataset_id=dataset_id, 
        table_id=table_id, temp_table_id=temp_table_id
    )
    query_job = bq_client.query(sql_insert)
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        print(row.name)
    time.sleep(2)