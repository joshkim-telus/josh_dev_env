from kfp.v2.dsl import component
from typing import Any

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="preprocess.yaml"
)
def preprocess(
    project_id: str,
    dataset_id: str,
    table_id: str,
    file_bucket: str,
    resource_bucket: str,
    stack_name: str,
    pipeline_path: str,
    utils_path: str, 
    model_type: str,
    load_sql: str, 
    preprocess_output_csv: str,
    pipeline_type: str, 
    training_mode=bool, 
    token: str
):
    """
    Preprocess data for a machine learning training pipeline.
    """
    
    # import global modules
    from google.cloud import storage
    from google.cloud import bigquery
    from pathlib import Path
    from yaml import safe_load
    import sys
    import os
    
    # for prod
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    pth_queries = pth_project / 'queries'
    sys.path.insert(0, pth_project.as_posix())
    
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
        bucket, pth_project, f'{stack_name}/{pipeline_path}/queries', split_prefix=pipeline_type
    )

    blob = bucket.blob(f'{stack_name}/{pipeline_path}/model_config.yaml')
    blob.download_to_filename(pth_model_config)
    
    # import local modules
    from etl.extract import extract_bq_data
    from modeling.features_preprocessing_v2 import process_features
    
    # load model config
    d_model_config = safe_load(pth_model_config.open())
    
    # select columns to query
    target_column = d_model_config['target']
    str_feature_names = ','.join([f"cast({f['name']} as {f['type']}) as {f['name']}" for f in d_model_config['features']])
    str_customer_ids = ','.join([f"cast({f['name']} as {f['type']}) as {f['name']}" for f in d_model_config['customer_ids']])
    
    # extract training data
    sql = (pth_queries / load_sql).read_text().format(
        project_id=project_id
        , dataset_id=dataset_id
        , table_id=table_id
        , target_column=target_column
        , customer_ids=str_customer_ids
        , feature_names=str_feature_names
    )
    
    # save sql to gcs bucket
    file_name = f'{pipeline_type}_queries/{load_sql}_formatted.sql'

    # Convert the string to bytes
    content_bytes = sql.encode('utf-8')

    # Upload the file to GCS
    bucket = storage_client.bucket(file_bucket)
    blob = bucket.blob(file_name)
    blob.upload_from_string(content_bytes)
    
    df = extract_bq_data(client, sql)
    print(f"Training dataset df.shape {df.shape}")
    
    # process features
    df_processed = process_features(
        df, d_model_config, training_mode=training_mode, model_type=model_type, target_name=target_column
    )
    print(f"Training dataset processed df.shape {df_processed.shape}")

    # save data to pipeline bucket
    df_processed.to_csv(
        f'gs://{file_bucket}/{pipeline_type}/{preprocess_output_csv}', index=False
    )
    print(f'Training data saved into {file_bucket}')

    
