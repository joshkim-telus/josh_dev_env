import kfp
import pandas as pd
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_import_df.yaml",
)
def bq_import_tbl_to_df(project_id: str
              , dataset_id: str
              , table_id: str
              , save_data_path: str
              , token: str 
              ): 
 
    from google.cloud import bigquery
    import logging
    from datetime import datetime
    
    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()
    
#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()
    
    # Change dataset / table + sp table name to version in bi-layer
    query =\
        f'''
            SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    df.to_csv(save_data_path, index=False) 

    col_list = list([col for col in df.columns])
    
    return (col_list,)
