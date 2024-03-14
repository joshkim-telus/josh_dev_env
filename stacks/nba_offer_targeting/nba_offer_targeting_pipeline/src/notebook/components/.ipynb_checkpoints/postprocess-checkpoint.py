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
def postprocess(project_id: str
              , dataset_id: str
              , table_id: str
              , read_data_path: str
              , save_data_path: str
              , base_type: str # digital_1p, digital_2p, casa 
              ): 

    import pandas as pd 
    import numpy as np 

    from google.cloud import bigquery
    import logging
    import datetime as dt

    df = pd.read_csv(read_data_path)

    date_cols = ['candate', 'rpp_hsia_end_dt', 'rpp_ttv_end_dt']
    df[date_cols] = df[date_cols].apply(pd.to_datetime)

    if base_type == "digital_1p" or base_type == "casa": 
        rk = [11, 21, 31]
    elif base_type == "digital_2p":
        rk = [10, 20, 30]
    else: 
        print("""a parameter 'base type' can only accept 'digital_1p', 'digital_2p', or 'casa' as input values""")

    def create_dict_from_df(df):

        # Convert DataFrame to a list of dictionaries
        records = df.to_dict(orient='records')

        # Create the desired dictionary format
        result_dict = {record[df.columns[0]]: [record[df.columns[1]], record[df.columns[2]]] for record in records}

        return result_dict

    #### this code block is only for a personal workbench 

    import google.oauth2.credentials
    token = !gcloud auth print-access-token
    token_str = token[0]

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token_str)

    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

    #     #### For prod 
    #     client = bigquery.Client(project=project_id)
    #     job_config = bigquery.QueryJobConfig()

    # Change dataset / table + sp table name to version in bi-layer
    query =\
        f'''
            SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
        '''

    df_offer_details = client.query(query, job_config=job_config).to_dataframe()
    dict_offer_details = create_dict_from_df(df_offer_details)

    ### promo_seg1
    irpc_reco1 = df[df['promo_seg1'] !='']
    new_df1 = irpc_reco1['promo_seg1'].apply(lambda x: dict_offer_details.get(x, ['', '']))
    promo_seg1 = pd.Series([item[0] for item in new_df1])
    offer_code1 = pd.Series([item[1] for item in new_df1])
    irpc_reco1['promo_seg'] = promo_seg1
    irpc_reco1['offer_code'] = offer_code1
    irpc_reco1['Category'] = 'Digital Renewal'
    irpc_reco1['Subcategory'] = 'Internet'
    irpc_reco1.loc[irpc_reco1["rpp_hsia_end_dt"].isnull(), "digital_category"] = 'Re-contracting'
    irpc_reco1.loc[irpc_reco1["rpp_hsia_end_dt"].dt.date > dt.date.today(), "digital_category"] = 'Renewal'
    irpc_reco1['ASSMT_VALID_START_TS'] = dt.datetime.now()
    irpc_reco1['ASSMT_VALID_END_TS'] = dt.datetime.now() + dt.timedelta(days=90)
    irpc_reco1['rk'] = rk[0]

    ### promo_seg2
    irpc_reco2 = df[ df['promo_seg2'] !='']
    new_df2 = irpc_reco2['promo_seg2'].apply(lambda x: dict_offer_details.get(x, ['', '']))
    promo_seg2 = pd.Series([item[0] for item in new_df2])
    offer_code2 = pd.Series([item[1] for item in new_df2])
    irpc_reco1['promo_seg'] = promo_seg2
    irpc_reco1['offer_code'] = offer_code2
    irpc_reco2['Category'] = 'Digital Renewal'
    irpc_reco2['Subcategory'] = 'Internet'
    irpc_reco2.loc[irpc_reco2["rpp_hsia_end_dt"].isnull(), "digital_category"] = 'Re-contracting'
    irpc_reco2.loc[irpc_reco2["rpp_hsia_end_dt"].dt.date > dt.date.today(), "digital_category"] = 'Renewal'
    irpc_reco2['ASSMT_VALID_START_TS'] = dt.datetime.now()
    irpc_reco2['ASSMT_VALID_END_TS'] = dt.datetime.now() + dt.timedelta(days=90)
    irpc_reco2['rk'] = rk[1]

    ### promo_seg3
    irpc_reco3 = df[df['promo_seg3'] !='']
    new_df3 = irpc_reco3['promo_seg3'].apply(lambda x: dict_offer_details.get(x, ['', '']))
    promo_seg3 = pd.Series([item[0] for item in new_df3])
    offer_code3 = pd.Series([item[1] for item in new_df3])
    irpc_reco1['promo_seg'] = promo_seg3
    irpc_reco1['offer_code'] = offer_code3
    irpc_reco3['Category'] = 'Digital Renewal'
    irpc_reco3['Subcategory'] = 'Internet'
    irpc_reco3.loc[irpc_reco2["rpp_hsia_end_dt"].isnull(), "digital_category"] = 'Re-contracting'
    irpc_reco3.loc[irpc_reco2["rpp_hsia_end_dt"].dt.date > dt.date.today(), "digital_category"] = 'Renewal'
    irpc_reco3['ASSMT_VALID_START_TS'] = dt.datetime.now()
    irpc_reco3['ASSMT_VALID_END_TS'] = dt.datetime.now() + dt.timedelta(days=90)
    irpc_reco3['rk'] = rk[2]

    # concatenate irpc_reco1 + irpc_reco2 + irpc_reco3 
    # write .csv to gcs
    irpc_recos = pd.concat([irpc_reco1, irpc_reco2, irpc_reco3], ignore_index=True)
    irpc_recos.reset_index(inplace=True)

    irpc_recos.to_csv(save_data_path, index=False)
    