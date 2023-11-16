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
              write: str = 'overwrite') -> NamedTuple("output", [("num_records", int)]):

    import os
    import google
    import pandas as pd 
    import numpy as np 
    from google.cloud import storage
    from google.cloud import bigquery
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

    schema_list = [bigquery.SchemaField('Customer_ID', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('write_off_ind_n_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('write_off_ind_y_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_c_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_ca_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_cc_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_d_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_dd_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('payment_method_cd_r_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_payment_method_cd_c_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_payment_method_cd_d_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_payment_method_cd_r_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('auto_payment_method_cd_ca_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('auto_payment_method_cd_cc_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('auto_payment_method_cd_dd_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_auto_payment_method_cd_c_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_auto_payment_method_cd_d_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('kb_auto_payment_method_cd_r_ind_current_month', 'INTEGER', 'NULLABLE', None, None, (), None)]

    dtype_bq_mapping = { 
        np.dtype('int64'): 'INTEGER', 
        np.dtype('float64'): 'FLOAT', 
        np.dtype('float32'): 'FLOAT', 
        np.dtype('object'): 'STRING', 
        np.dtype('bool'): 'BOOLEAN', 
        np.dtype('datetime64[ns]'): 'DATE', 
        pd.Int64Dtype(): 'INTEGER' 
    } 
    
    if write == 'overwrite': 
        write_type = 'WRITE_TRUNCATE' 
    elif write == 'append': 
        write_type = 'WRITE_APPEND' 

    dest_tbl = f'{project_id}.{dataset_id}.{table_id}'
    
    try: 
        # Sending to bigquery 
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
    