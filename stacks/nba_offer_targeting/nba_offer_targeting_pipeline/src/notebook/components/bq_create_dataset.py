import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# Create IRPC Digital 1P, Digital 2P, and Casa base tables
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_create_dataset.yaml",
)
def bq_create_dataset(project_id: str
                      , dataset_id: str
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
            -- Change dataset / sp name to the version in the bi_layer
            CALL nba_offer_targeting.bq_sp_irpc_digital_1p_base();
            
            CALL nba_offer_targeting.bq_sp_irpc_digital_2p_base(); 
            
            CALL nba_offer_targeting.bq_sp_irpc_casa_base(); 

            SELECT
                *
            FROM {dataset_id}.INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name='bq_irpc_digital_1p_base'
        '''
    
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info(df.to_string())
    
    logging.info(f"Loaded {df.total_rows[0]} rows into \
             {df.table_catalog[0]}.{df.table_schema[0]}.{df.table_name[0]} on \
             {datetime.strftime((df.last_modified_time[0]), '%Y-%m-%d %H:%M:%S') } !")
    