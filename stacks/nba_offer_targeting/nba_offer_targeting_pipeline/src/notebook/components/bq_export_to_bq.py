import kfp
from kfp import dsl
# from kfp.v2.dsl import (Model, Input, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output,HTML,
                        OutputPath, ClassificationMetrics, Metrics, component)
from typing import NamedTuple

# postprocess - attach "Category", "Subcategory", "rpp_hsia_end_dt", "ASSMT_VALID_START_TS", "ASSMT_VALID_END_TS", "rk" to 3 offers
# ***pay attention to the rows that have "None" values in the promo_seg1, promo_seg2, promo_seg3
# concatenate irpc_reco1+ irpc_reco2 + irpc_reco3
# write .csv to gcs (digital_1p_base_irpc_offers.csv)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="postprocess.yaml",
)
def bq_export_to_bq(project_id: str
              , dataset_id: str
              , table_id: str
              , temp_table: str
              , digital_1p_data_path: str
              , digital_2p_data_path: str
              , casa_data_path: str
              , save_data_path: str
              , token: str
              ): 

    import pandas as pd 
    import numpy as np 

    from google.cloud import bigquery
    import logging
    import datetime as dt

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()
    
#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()    

    def if_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    # read bq_irpc_digital_1p_base_postprocess.csv
    digital_1p_base = pd.read_csv(digital_1p_data_path, index_col=None)
    
    # read bq_irpc_digital_2p_base_postprocess.csv
    digital_2p_base = pd.read_csv(digital_2p_data_path, index_col=None)
    
    # read bq_irpc_casa_base_postprocess.csv
    casa_base = pd.read_csv(casa_data_path, index_col=None)
    
    # concatenate all three files -- irpc_offers_assigned
    dfs = [digital_1p_base, digital_2p_base, casa_base]

    # Concatenate the DataFrames
    irpc_offers_assigned = pd.concat(dfs, ignore_index=True)
    irpc_offers_assigned.reset_index(inplace=False)
    
    # write the final file to csv irpc_offers_assigned.csv
    irpc_offers_assigned.to_csv(save_data_path, index=False)
    
    # insert irpc_offers_assigned again to a bq table -- qua_base_hs 
    table_ref = f'{project_id}.{dataset_id}.{table_id}'
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    table = client.get_table(table_ref)
    schema = table.schema

    ll = []
    for item in schema:
        col = item.name
        d_type = item.field_type
        if 'float' in str(d_type).lower():
            d_type = 'FLOAT64'
        ll.append((col, d_type))

        if 'integer' in str(d_type).lower():
            irpc_offers_assigned[col] = irpc_offers_assigned[col].fillna(0).astype(int)
        elif 'float' in str(d_type).lower():
            irpc_offers_assigned[col] = irpc_offers_assigned[col].fillna(0.0).astype(float)
        elif 'string' in str(d_type).lower():
            irpc_offers_assigned[col] = irpc_offers_assigned[col].fillna('').astype(str)
        elif 'timestamp' in str(d_type).lower(): 
            irpc_offers_assigned[col] = pd.to_datetime(irpc_offers_assigned[col], errors='coerce')
        elif 'date' in str(d_type).lower():
            irpc_offers_assigned[col] = pd.to_datetime(irpc_offers_assigned[col], errors='coerce').dt.date

    table_ref = '{}.{}.{}'.format(project_id, dataset_id, temp_table)
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    if if_tbl_exists(client, table_ref):
        client.delete_table(table_ref)

    client.create_table(table_ref)
    config = bigquery.LoadJobConfig(schema=schema)
    config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    bq_table_instance = client.load_table_from_dataframe(irpc_offers_assigned, table_ref, job_config=config)
    
    # drop_sql = f"""delete from `{project_id}.{dataset_id}.{table_id}` where true"""  # .format(project_id, dataset_id, score_date_dash)
    # client.query(drop_sql)
    
    load_sql = f"""insert into `{project_id}.{dataset_id}.{table_id}`
                  select * from `{project_id}.{dataset_id}.{temp_table}`"""
    client.query(load_sql)
    