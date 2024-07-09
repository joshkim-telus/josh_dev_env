from kfp.v2.dsl import component
from typing import Any


@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="hs_nba_existing_customers_postprocess.yaml"
)
def postprocess(
    project_id: str,
    output_dataset_id: str,
    score_table_id: str,
    resource_bucket: str,
    file_bucket: str,
    stack_name: str,
    model_type: str,
    pipeline_type: str,
    pipeline_path: str,
    hs_nba_utils_path: str, 
    # token: str
    ):
    """
    Postprocess data for a machine learning pipeline.
    """
    
    # import global modules
    from google.cloud import storage
    from pathlib import Path
    from yaml import safe_load
    import sys
    import os
    import pandas as pd

    # set global vars
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    pth_queries = pth_project / 'queries'
    sys.path.insert(0, pth_project.as_posix())

    # init gcp clients
    storage_client = storage.Client()

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
        bucket, pth_project, f'{stack_name}/{hs_nba_utils_path}', split_prefix='notebook'
    )
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{pipeline_path}/queries'
    )
    blob = bucket.blob(f'{stack_name}/{pipeline_path}/model_config.yaml')
    blob.download_to_filename(pth_model_config)

    # import local modules
    from hs_nba_utils.etl.load import create_temp_table, insert_from_temp_table

    # load model config
    d_model_config = safe_load(pth_model_config.open())

   # load data from bucket
    df_scores = pd.read_csv(f'gs://{file_bucket}/{pipeline_type}/df_score.csv')
    
    print(f'Scores df.shape {df_scores.shape}')

    # insert model id and and set unavailable targets to None
    df_scores['model_id'] = d_model_config['model_id']
    l_unavailable_targets = d_model_config['unavailable_target_variables']
    if l_unavailable_targets:
        df_scores[l_unavailable_targets] = [None] * len(l_unavailable_targets)
    
    # create temp table in bq
    temp_table_name = create_temp_table(
        project_id, output_dataset_id, score_table_id, df_scores
    )
    
    print(f'created a temp table {temp_table_name}')

    # insert data from temp into main table
    current_part_dt = str(df_scores['part_dt'].max())
    insert_from_temp_table(
        project_id, output_dataset_id, score_table_id, temp_table_name, current_part_dt,
        pth_queries / 'drop_current_part_dt.sql', pth_queries / 'insert_from_temp_table.sql'
    )
    
    