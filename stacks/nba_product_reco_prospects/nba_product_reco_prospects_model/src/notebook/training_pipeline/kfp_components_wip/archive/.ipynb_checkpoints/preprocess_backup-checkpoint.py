from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="nba_product_reco_prospects_model_preprocess.yaml"
)
def preprocess(pipeline_dataset: str
               , save_data_path: str
               , project_id: str
               , dataset_id: str
               , score_date_dash: str
               , token:str
               ):
    
    from google.cloud import bigquery
    import pandas as pd
    import numpy as np
    import gc
    import time

    def to_categorical(df, cat_feature_names): 

        df_dummies = pd.get_dummies(df[cat_feature_names]) 
        df_dummies.columns = df_dummies.columns.str.replace('&', 'and')
        df_dummies.columns = df_dummies.columns.str.replace(' ', '_')

        df.drop(columns=cat_feature_names, axis=1, inplace=True)

        df = df.join(df_dummies)

        #column name clean-up
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('-', '_')

        return df
    
    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()

    # pipeline_dataset
    pipeline_dataset_name = f"{project_id}.{dataset_id}.{pipeline_dataset}" 
    build_df_pipeline_dataset = f'SELECT * FROM `{pipeline_dataset_name}`'
    df_pipeline_dataset = client.query(build_df_pipeline_dataset).to_dataframe()
    
    # demo columns
    df_pipeline_dataset['demo_urban_flag'] = df_pipeline_dataset.demo_sgname.fillna('').str.lower().apply(lambda x: 1 if 'urban' in x and 'suburban' not in x else 0).astype(int)
    df_pipeline_dataset['demo_rural_flag'] = df_pipeline_dataset.demo_sgname.fillna('').str.lower().apply(lambda x: 1 if 'suburban' in x or 'rural' in x or 'town' in x else 0).astype(int)
    df_pipeline_dataset['demo_family_flag'] = df_pipeline_dataset.demo_lsname.str.lower().str.contains('families').fillna(0).astype(int)

    # categorical variables to dummy variables
    cat_feature_names = ['revenue_band', 'payment_mthd', 'ebill_ind', 'dvc_non_telus_ind', 'credit_class', 'contract_type', 'bacct_delinq_ind', 'urbn_rur_ind',
                     'dnc_sms_ind', 'dnc_em_ind', 'data_usg_trend', 'wls_data_plan_ind', 'wls_data_shr_plan_ind', 'demo_lsname']
    
    df_pipeline_dataset = to_categorical(df_pipeline_dataset, cat_feature_names)

    df_join = df_pipeline_dataset.copy()
    
    # set up df_target 
    # sql_target = ''' SELECT * FROM `{}.{}.bq_telus_postpaid_churn_targets` '''.format(project_id, dataset_id) 
    # df_target = client.query(sql_target).to_dataframe()
    # df_target = df_target.loc[
    #     df_target['YEAR_MONTH'] == '-'.join(score_date_dash.split('-')[:2])]  
    # df_target['ban'] = df_target['ban'].astype('int64')
    # df_target['subscriber_no'] = df_target['subscriber_no'].astype('str')
    # df_target = df_target.groupby(['ban', 'subscriber_no']).tail(1)
    
    # set up df_final
    # df_final = df_join.merge(df_target[['ban', 'subscriber_no', 'target_ind']], on=['ban', 'subscriber_no'], how='left')

    df_final = df_join.copy()
    df_final.rename(columns={'target_ind': 'target'}, inplace=True) 
    df_final['target'].fillna(0, inplace=True) 
    df_final['target'] = df_final['target'].astype(int) 
    print(df_final.shape)

    # delete df_join
    del df_join
    gc.collect()
    print('......df_final done')

    for f in df_final.columns:
        df_final[f] = list(df_final[f])

    df_final.to_csv(save_data_path, index=True) 
    del df_final
    gc.collect()
    print(f'......csv saved in {save_data_path}')
    time.sleep(120)

    

    

    

