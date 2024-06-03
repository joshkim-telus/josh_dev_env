from kfp.v2.dsl import component
from typing import Any


@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="hs_nba_existing_customers_preprocess.yaml"
)
def preprocess(
    project_id: str,
    resource_bucket: str,
    pipeline_bucket: str,
    stack_name: str,
    pipeline_path: str,
    hs_nba_utils_path: str,
    source_features_table: str
):
    """
    Preprocess data for a machine learning pipeline.
    """
    
    # import global modules
    from google.cloud import storage
    from google.cloud import bigquery
    from pathlib import Path
    from yaml import safe_load
    import sys
    import os
    
    # set global vars
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    sys.path.insert(0, pth_project.as_posix())

    # init gcp clients
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)

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
    bucket = storage_client.bucket(resource_bucket)
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{hs_nba_utils_path}', split_prefix='notebook'
    )  
    blob = bucket.blob(f'{stack_name}/{pipeline_path}/model_config.yaml')
    blob.download_to_filename(pth_model_config)

    # import local modules
    from hs_nba_utils.etl.extract import extract_bq_data
    from hs_nba_utils.modeling.hs_features_preprocessing import process_hs_features

    # load model config
    d_model_config = safe_load(pth_model_config.open())

    # select columns to query
    str_feature_names = ','.join([f['name'] for f in d_model_config['features']])
    str_customer_ids = ','.join(d_model_config['customer_ids'])

    # extract data
    sql = f"""
        select ref_dt as part_dt, {str_customer_ids}, {str_feature_names}
            from `{source_features_table}`
    """
    df_features = extract_bq_data(bq_client, sql)
    print(f'Features df.shape {df_features.shape}')

    # process features
    df_processed = process_hs_features(df_features, d_model_config)
    print(f'Features processed  df.shape {df_features.shape}')

    # save data to pipeline bucket
    location_to_save = f'gs://{pipeline_bucket}/data/features.csv'
    df_processed.to_csv(location_to_save, index=False)
    print(f'Data saved into {location_to_save}')