import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# 
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_output_validation.yaml",
)
def output_validation(project_id: str
       , dataset_id: str
       , query: str
       , token: str
      ):
    
    print('1')
    from google.cloud import bigquery
    import logging
    from datetime import datetime
    
     #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)

    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

    #### For prod
    #    client = bigquery.Client(project=project_id)
    #    job_config = bigquery.QueryJobConfig()
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    if df.shape[0] < 1:
        validation = 'pass'
    else:
        validation = 'fail'
        
    print(validation)
        
    logging.info(df.to_string())

#     logging.info(f"Loaded {df.total_rows[0]} rows into \
#              {df.table_catalog[0]}.{df.table_schema[0]}.{df.table_name[0]} on \
#              {datetime.strftime((df.last_modified_time[0]), '%Y-%m-%d %H:%M:%S')} !")
    
    return validation



