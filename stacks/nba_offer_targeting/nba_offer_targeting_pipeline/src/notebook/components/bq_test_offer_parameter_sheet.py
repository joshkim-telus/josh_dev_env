import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# Create IRPC Digital 1P, Digital 2P, and Casa base tables
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="bq_test_offer_parameter_sheet.yaml",
)
def bq_test_offer_parameter_sheet(project_id: str
                      , dataset_id: str
                      , token: str
                      )-> NamedTuple("output", [("result", str)]):
 
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
        with max_dt as (
            SELECT 
            max(part_dt) as part_dt
            FROM `nba_offer_targeting.bq_offer_targeting_params_upd` 
        )
        , tdy as (
            select a.* 
            from `nba_offer_targeting.bq_offer_targeting_params_upd` a 
            inner join max_dt b
            on a.part_dt = b.part_dt
            where a.if_active = 1 and a.HS_filters is not null
        )

        select 
        count(*) as col1
        , count(distinct ncid) as col2
        , count(distinct promo_seg) as col3
        from tdy
        '''

    df = client.query(query, job_config=job_config).to_dataframe()

    dict_df = df.to_dict(orient='list')

    def checker_logic(dict_df): 
        col1 = dict_df['col1']
        col2 = dict_df['col2']
        col3 = dict_df['col3']

        if col1 == col2 == col3: 
            return 'pass'
        else: 
            return 'fail'

    result = checker_logic(dict_df)
        
    print(result) 
    
    if result == 'fail':
        raise ValueError("initial checks failed. please review the `nba_offer_targeting.bq_offer_targeting_params_upd` table.")

    return (result, )
    