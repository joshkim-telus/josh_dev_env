import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_run_test_sp.yaml",
)
def bq_run_test_sp(score_date_dash: str,
                  project_id: str,
                  dataset_id: str,
                  region: str, 
                  token: str) -> NamedTuple("output", [("num_records", int)]):
 
    from datetime import datetime
    from google.cloud import bigquery
    import logging 
    
    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()
    
#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()
    
    # Execute stored procedure in BQ
    query =\
        f'''
            DECLARE score_date DATE DEFAULT "{score_date_dash}";
        
            -- Change dataset / sp name to the version in the bi_layer
            CALL {dataset_id}.bq_sp_wls_training_dataset(score_date);
            
            SELECT
                *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='bq_wls_training_dataset'
            
        '''
    
    # Contain query result into df and log
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info(df.to_string())
    
    logging.info(f"Loaded {df.total_rows[0]} rows into \
             {df.table_catalog[0]}.{df.table_schema[0]}.{df.table_name[0]} on \
             {datetime.strftime((df.last_modified_time[0]), '%Y-%m-%d %H:%M:%S') } !")
    
    # Report number of rows added
    query =\
        f'''
           SELECT *
           FROM {dataset_id}.bq_wls_training_dataset
           WHERE part_dt = "{score_date_dash}" 
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    num_records = df.shape[0]
    
    return (num_records,)
    