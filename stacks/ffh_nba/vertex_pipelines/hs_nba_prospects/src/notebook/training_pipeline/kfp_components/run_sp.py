import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple
# Create Training Dataset for training pipeline
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="run_sp.yaml",
)
def run_sp(from_date: str
          , to_date: str 
          , project_id: str
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
            BEGIN
              DECLARE date_exists BOOL DEFAULT FALSE;

              -- Check if the date '2023-05-01' exists in the from_dt column
              SET date_exists = (
                SELECT COUNT(1) > 0
                FROM `{project_id}.nba_features_prospect.master_features_set_prospect`
                WHERE from_dt = '{from_date}'
              );

              -- Conditionally execute the script if the date does not exist
              IF date_exists = FALSE THEN
                EXECUTE IMMEDIATE FORMAT('''
                  DECLARE _from_dt DATE DEFAULT '{from_date}';
                  DECLARE _to_dt DATE DEFAULT '{to_date}';
                  CALL `{project_id}.nba_targets.sp_persist_targets_prospect`(_from_dt, _to_dt);
                  CALL `{project_id}.nba_features_prospect.sp_persist_master_features_set`(_from_dt, _to_dt);
                ''');
              END IF;

              SELECT * FROM `{project_id}.nba_features_prospect.master_features_set_prospect` LIMIT 1000; 

            END;
        """
    
    df = client.query(query, job_config=job_config).to_dataframe()

    col_list = list([col for col in df.columns])
    
    return (col_list,)
    