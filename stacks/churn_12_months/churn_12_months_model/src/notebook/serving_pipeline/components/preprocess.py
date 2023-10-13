from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="churn_12_months_model_preprocess.yaml"
)
def preprocess(
        pipeline_dataset: str, 
        save_data_path: str,
        project_id: str,
        dataset_id: str, 
        service_type: str, 
        file_bucket: str 
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

    #df_final
    df_final = df_join.copy()
    del df_join
    gc.collect()
    print('......df_final done')

    for f in df_final.columns:
        df_final[f] = list(df_final[f])

    df_final.to_csv(save_data_path, index=True, compression='gzip')
    
    df_final.to_csv('gs://{}/{}/{}_score_monitoring.csv'.format(file_bucket, service_type, service_type))  

#     # define dtype_bq_mapping
#     dtype_bq_mapping = {np.dtype('int64'): 'INTEGER', 
#     np.dtype('float64'):  'FLOAT', 
#     np.dtype('float32'):  'FLOAT', 
#     np.dtype('object'):  'STRING', 
#     np.dtype('bool'):  'BOOLEAN', 
#     np.dtype('datetime64[ns]'):  'DATE', 
#     pd.Int64Dtype(): 'INTEGER'} 
    
#     # export df_final to bigquery 
#     schema_list = [] 
#     for column in df_final.columns: 
#         schema_list.append(bigquery.SchemaField(column, dtype_bq_mapping[df_final.dtypes[column]], mode='NULLABLE')) 
#     print(schema_list) 
    
#     dest_table = f'{dataset_id}.{table_id}'

#     # Sending to bigquery 
#     job_config = bigquery.LoadJobConfig(schema=schema_list, write_disposition='WRITE_TRUNCATE') 
#     job = client.load_table_from_dataframe(df_final, dest_table, job_config=job_config) 
#     job.result() 
#     table = client.get_table(dest_table) # Make an API request 
#     print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)) 
    
    del df_final
    gc.collect()
    print(f'......csv saved in {save_data_path}')
    time.sleep(120)