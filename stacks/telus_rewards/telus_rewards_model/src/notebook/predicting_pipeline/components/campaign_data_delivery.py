import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# Upload to divg_compaign_element.bq_campaign_data_element 
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="bq_create_dataset.yaml",
)
def campaign_data_delivery(score_date: str,
                      score_date_delta: int,
                      project_id: str,
                      dataset_id: str,
                      region: str):
 
    from google.cloud import bigquery
    import logging 
    from datetime import datetime
    # For wb
    # import google.oauth2.credentials
    # CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, location=region)
    job_config = bigquery.QueryJobConfig()
    
    # Change dataset / table + sp table name to version in bi-layer
    query =\
        f'''
        
            DECLARE score_date DATE DEFAULT "{score_date}";

            CALL call_to_retention_dataset.bq_sp_campaign_data_element_pxp(score_date); 
            CALL call_to_retention_dataset.bq_sp_campaign_data_element_hcr(score_date);
            
            SELECT
                *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='bq_call_to_retention_scores'
            
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    print('......data loaded to divg_compaign_element.bq_campaign_data_element')

    
    
    
    
    