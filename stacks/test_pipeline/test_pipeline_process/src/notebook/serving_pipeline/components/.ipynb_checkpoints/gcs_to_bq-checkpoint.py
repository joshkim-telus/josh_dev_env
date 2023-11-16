import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)

from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta

from typing import NamedTuple

import google
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.aiplatform import pipeline_jobs

# Create Training Dataset for training pipeline
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="gcs_to_bq.yaml",
)
def gcs_to_bq(project_id: str,
              dataset_id: str,
              table_id: str, 
              region: str, 
              file_bucket: str, 
              local_path: str, 
              file_name: str,
              token: str, 
              write: str = 'overwrite') -> NamedTuple("output", [("num_records", int)]):

    import os
    import google
    import pandas as pd 
    import numpy as np 
    from google.cloud import storage
    from google.cloud import bigquery
    from google.cloud.bigquery import SchemaField
    from pathlib import Path

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()
    
#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()
    
    df = pd.read_csv(f'gs://{local_path}{file_name}')
    
    # Query schema of the table
    query =\
        f'''
        SELECT
          column_name,
          data_type,
          is_nullable,
          column_default
        FROM
          {dataset_id}.INFORMATION_SCHEMA.COLUMNS
        WHERE
          table_name = '{table_id}';
        '''
    
    # Contain schema table in df_schema
    df_schema = client.query(query, job_config=job_config).to_dataframe()
    
    # dtype mapping for schema
    str_dtype_bq_mapping = { 
        'INT64': 'INTEGER', 
        'FLOAT64': 'FLOAT', 
        'FLOAT32': 'FLOAT', 
        'OBJECT': 'STRING', 
        'BOOL': 'BOOLEAN', 
        'DATE': 'DATE', 
    } 
    
    # nullable mapping for schema
    nullable_mapping = { 
        'YES': 'NULLABLE',
        'NO': 'REQUIRED'
    } 

    # built schema_list
    schema_list = [] 

    for idx, row in df_schema.iterrows(): 
        schema_list.append(bigquery.SchemaField(row['column_name'], str_dtype_bq_mapping[row['data_type']], nullable_mapping[row['is_nullable']], None, None, (), None))
    
    # write mode
    if write == 'overwrite': 
        write_type = 'WRITE_TRUNCATE' 
    elif write == 'append': 
        write_type = 'WRITE_APPEND' 
    
    # destination table name
    dest_tbl = f'{project_id}.{dataset_id}.{table_id}'
    
    # load df to dest_tbl
    try: 
        # Sending to bigquery 
        client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
        job_config = bigquery.LoadJobConfig(schema=schema_list, write_disposition=write_type) 
        print(f'table_id: {dest_tbl}')
        job = client.load_table_from_dataframe(df, dest_tbl, job_config=job_config) 
        job.result() 
        table = client.get_table(dest_tbl) # Make an API request 
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)) 

    except NameError as e: 
        print(f"Error : {e}")
        
    num_records = df.shape[0]
    
    return (num_records,)
    