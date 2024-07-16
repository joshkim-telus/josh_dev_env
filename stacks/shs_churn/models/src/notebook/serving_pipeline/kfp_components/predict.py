from kfp.v2.dsl import component
from typing import Any

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="predict.yaml"
)
def predict(project_id: str,
            dataset_id: str,
            score_table_id: str,
            resource_bucket: str,
            file_bucket: str, 
            stack_name: str,
            service_type: str, 
            model_id: str, 
            pipeline_type: str,
            training_pipeline_path: str,
            serving_pipeline_path: str,
            preprocess_output_csv: str, 
            utils_path: str, 
            score_file_name: str, 
            token: str
            ):
    
    """
    Machine learning predict pipeline.
    """

    # Import global modules
    from google.cloud import storage
    from pathlib import Path
    from yaml import safe_load
    import sys
    import os
    import pandas as pd
    import pickle
    from google.cloud import bigquery
    
    # for prod
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    pth_queries = pth_project / 'queries'
    sys.path.insert(0, pth_project.as_posix())
    
    # init gcp clients
    storage_client = storage.Client()

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

    # #### For prod 
    # client = bigquery.Client(project=project_id)
    # job_config = bigquery.QueryJobConfig()

    def extract_dir_from_bucket(
        bucket: Any, local_path: Path, prefix: str, split_prefix: str = 'serving_pipeline' 
    ):
        """
        Download files from a specified bucket to a local path, excluding a specified prefix.

        Parameters:
        - bucket: The bucket object from which to download files.
        - local_path: The local path where the files will be downloaded to.
        - prefix: The prefix to filter the files in the bucket. Only files with this prefix will be downloaded.
        - split_prefix: The prefix to exclude from the downloaded file paths. Default is 'serving_pipeline'.
        """
        for blob in bucket.list_blobs(prefix=prefix):
            if not blob.name.endswith("/"):
                path = local_path / blob.name.split(f'{split_prefix}/')[-1]
                str_path = path.as_posix()
                Path(str_path[:str_path.rindex('/')]).mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str_path)

    # download utils and model config locally
    storage_client = storage.Client()
    bucket = storage_client.bucket(resource_bucket)
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{utils_path}', split_prefix='resources'
    ) 
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{serving_pipeline_path}/queries', split_prefix=pipeline_type
    )
    
    blob = bucket.blob(f"{stack_name}/{serving_pipeline_path}/model_config.yaml" )
    blob.download_to_filename(pth_model_config)
    
    # download model pickle file
    model_name = f'{service_type}_xgb_models_latest.pkl'
    bucket = storage_client.bucket(file_bucket)
    blob = bucket.blob(f'models/{model_name}')
    blob.download_to_filename(pth_project / model_name)

    # load model
    d_model_config = safe_load(pth_model_config.open())
    with open(pth_project / model_name, "rb") as f:
        models_dict = pickle.load(f)
    model = models_dict['model']

    # load data from bucket
    df_features = pd.read_csv(f'gs://{file_bucket}/{pipeline_type}/{preprocess_output_csv}', index_col=None)
    
    print(f'Features df.shape {df_features.shape}')

    # select features and model predict  
    l_feature_names = [f['name'] for f in d_model_config['features']]

    # make predictions on df_features
    pred_prob = model.predict_proba(df_features[l_feature_names], ntree_limit=model.best_iteration)[:, 1]

    # import local modules
    from etl.load import create_temp_table, insert_from_temp_table
    
    # build result dataframe
    result = pd.DataFrame(columns=['ban', 'score_date', 'model_id', 'score'])
    result['score'] = list(pred_prob)
    result['score'] = result['score'].fillna(0.0).astype('float64')
    result['ban'] = list(df_features['ban'])
    result['ban'] = result['ban'].astype('str')
    result['score_date'] = df_features['part_dt']
    result['model_id'] = model_id
    
    # save data to pipeline bucket
    location_to_save = f'gs://{file_bucket}/{pipeline_type}/{score_file_name}'
    result.to_csv(location_to_save, index=False)
    print(f'Scores saved into {location_to_save}')
    
    # create temp table in bq
    temp_table_name = create_temp_table(
        bq_client=client, project_id=project_id, dataset_id=dataset_id, table_id=score_table_id, df_to_load=result
    )
    
    print(f'created a temp table {temp_table_name}')

    # insert data from temp into main table
    current_part_dt = str(result['score_date'].max())
    insert_from_temp_table(
        bq_client=client, project_id=project_id, dataset_id=dataset_id, table_id=score_table_id, temp_table_id=temp_table_name, current_part_dt=current_part_dt,
        pth_drop_query=pth_queries / 'drop_current_part_dt.sql', pth_insert_query=pth_queries / 'insert_from_temp_score_table.sql'
    )

    