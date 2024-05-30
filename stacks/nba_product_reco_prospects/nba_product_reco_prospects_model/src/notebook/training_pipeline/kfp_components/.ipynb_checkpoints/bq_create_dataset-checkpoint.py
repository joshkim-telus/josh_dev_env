import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_create_dataset.yaml",
)
def bq_create_dataset(from_date: str
                      , to_date: str 
                      , project_id: str
                      , dataset_id: str
                      , table_id: str
                      , region: str
                      , token: str
                      ) -> NamedTuple("output", [("col_list", list)]):
 
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
        f"""
            DECLARE _from_dt DATE DEFAULT '{from_date}';
            DECLARE _to_dt DATE DEFAULT '{to_date}';
        
            -- Run this once the training pipeline deploys in BI Layer
            -- CALL `{project_id}.{dataset_id}.{table_id}`(_from_dt, _to_dt);
            
            SELECT *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='{table_id}'
        """
    
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info(df.to_string())
    
    ######################### Save column list_##########################
    query =\
        f"""
           SELECT
                *
            FROM {dataset_id}.{table_id}
            LIMIT 1
        """
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    col_list = list([col for col in df.columns])
    
    return (col_list,)
    