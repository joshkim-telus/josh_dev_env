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
                  region: str) -> NamedTuple("output", [("num_records", int)]):
 
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
    
    # Change dataset / table + sp table name to version in bi-layer
    query =\
        f'''
            DECLARE score_date DATE DEFAULT "{score_date_dash}";
        
            -- Change dataset / sp name to the version in the bi_layer
            CALL {dataset_id}.bq_sp_campaign_data_delivery(score_date);
            
            SELECT
                *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='bq_campaign_records_hcr'
            
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info(df.to_string())
    
    logging.info(f"Loaded {df.total_rows[0]} rows into \
             {df.table_catalog[0]}.{df.table_schema[0]}.{df.table_name[0]} on \
             {datetime.strftime((df.last_modified_time[0]), '%Y-%m-%d %H:%M:%S') } !")
    
    # Report Number Of Rows Added 
    query =\
        f'''
           SELECT *
           FROM {dataset_id}.bq_campaign_data_element
           WHERE initiative_name = "high_churn_risk_email" 
           AND date_of_run = "{score_date_dash}" 
           AND field_1 = "test"
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    num_records = df.shape[0]
    return (num_records,)
    