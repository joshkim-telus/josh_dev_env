import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_create_dataset.yaml",
)
def bq_create_dataset(score_date: str,
                      score_date_delta: int,
                      project_id: str,
                      dataset_id: str,
                      region: str,
                      promo_expiry_start: str, 
                      promo_expiry_end: str, 
                      v_start_date: str,
                      v_end_date: str, 
                      token: str) -> NamedTuple("output", [("col_list", list)]):
 
    import google
    from google.cloud import bigquery
    from datetime import datetime
    import logging 
    import os 
    import re 
    from google.oauth2 import credentials

    CREDENTIALS = google.oauth2.credentials.Credentials(token) # get credentials from token
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

    # Change dataset / table + sp table name to version in bi-layer
    query =\
        f'''
            DECLARE score_date DATE DEFAULT "{score_date}";
            DECLARE promo_expiry_start DATE DEFAULT "{promo_expiry_start}";
            DECLARE promo_expiry_end DATE DEFAULT "{promo_expiry_end}";
            DECLARE start_date DATE DEFAULT "{v_start_date}";
            DECLARE end_date DATE DEFAULT "{v_end_date}";
        
            -- Change dataset / sp name to the version in the bi_layer
            CALL {dataset_id}.bq_sp_ctr_pipeline_dataset(score_date, promo_expiry_start, promo_expiry_end, start_date, end_date);

            SELECT
                *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='bq_ctr_pipeline_dataset'
            
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info(df.to_string())
    
    logging.info(f"Loaded {df.total_rows[0]} rows into \
             {df.table_catalog[0]}.{df.table_schema[0]}.{df.table_name[0]} on \
             {datetime.strftime((df.last_modified_time[0]), '%Y-%m-%d %H:%M:%S') } !")
    
    ######################################## Save column list_##########################
    query =\
        f'''
           SELECT
                *
            FROM {dataset_id}.bq_ctr_pipeline_dataset

        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    col_list = list([col for col in df.columns])
    return (col_list,)
    