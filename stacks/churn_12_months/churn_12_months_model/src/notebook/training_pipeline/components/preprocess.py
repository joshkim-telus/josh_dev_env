from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="churn_12_months_model_preprocess.yaml"
)
def preprocess(pipeline_dataset: str
               , save_data_path: str
               , project_id: str
               , dataset_id: str
               , score_date_dash: str
               ):
    
    from google.cloud import bigquery
    import pandas as pd
    import numpy as np
    import gc
    import time

    client = bigquery.Client(project=project_id)
    
    # pipeline_dataset
    pipeline_dataset_name = f"{project_id}.{dataset_id}.{pipeline_dataset}" 
    build_df_pipeline_dataset = f'SELECT * FROM `{pipeline_dataset_name}`'
    df_pipeline_dataset = client.query(build_df_pipeline_dataset).to_dataframe()
    df_pipeline_dataset = df_pipeline_dataset.set_index('ban') 
    
    # demo columns
    df_pipeline_dataset['demo_urban_flag'] = df_pipeline_dataset.demo_sgname.str.lower().str.contains('urban').fillna(0).astype(int)
    df_pipeline_dataset['demo_rural_flag'] = df_pipeline_dataset.demo_sgname.str.lower().str.contains('rural').fillna(0).astype(int)
    df_pipeline_dataset['demo_family_flag'] = df_pipeline_dataset.demo_lsname.str.lower().str.contains('families').fillna(0).astype(int)

    df_income_dummies = pd.get_dummies(df_pipeline_dataset[['demo_lsname']]) 
    df_income_dummies.columns = df_income_dummies.columns.str.replace('&', 'and')
    df_income_dummies.columns = df_income_dummies.columns.str.replace(' ', '_')

    df_pipeline_dataset.drop(columns=['demo_sgname', 'demo_lsname'], axis=1, inplace=True)

    df_pipeline_dataset = df_pipeline_dataset.join(df_income_dummies)

    df_join = df_pipeline_dataset.copy()

    #column name clean-up
    df_join.columns = df_join.columns.str.replace(' ', '_')
    df_join.columns = df_join.columns.str.replace('-', '_')
    
    # set up df_target 
    sql_target = ''' SELECT * FROM `{}.{}.bq_churn_12_months_targets` '''.format(project_id, dataset_id) 
    df_target = client.query(sql_target).to_dataframe()
    df_target = df_target.loc[
        df_target['YEAR_MONTH'] == '-'.join(score_date_dash.split('-')[:2])]  # score_date_dash = '2022-08-31'
    df_target['ban'] = df_target['ban'].astype('int64')
    df_target = df_target.groupby('ban').tail(1)
    
    # set up df_final
    df_final = df_join.merge(df_target[['ban', 'target_ind']], on='ban', how='left')
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

    
    
    
    
    
    