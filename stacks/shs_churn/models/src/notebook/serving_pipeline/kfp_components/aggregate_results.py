from kfp.v2.dsl import component
from typing import Any


@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="hs_nba_existing_customers_aggregate_results.yaml"
)
def aggregate_results(
    project_id: str,
    output_dataset_id: str,
    aggregate_results_table_id: str,
    resource_bucket: str,
    stack_name: str,
    pipeline_path: str,
    hs_nba_utils_path: str, 
    # token: str
):
    """
    Aggregate hs_nba_existing_customers and hs_nba_existing_customers_tiers 
    model scores  and insert results into bq output table
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
    pth_output_table_config = pth_project / 'output_table_config.yaml'
    pth_queries = pth_project / 'queries'
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

    # download utils and output table config locally
    storage_client = storage.Client()
    bucket = storage_client.bucket(resource_bucket)
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{hs_nba_utils_path}', split_prefix='notebook'
    )
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{pipeline_path}/queries'
    )
    blob = bucket.blob(f'{stack_name}/{pipeline_path}/output_table_config.yaml')
    blob.download_to_filename(pth_output_table_config)

    # import local modules
    from hs_nba_utils.etl.extract import extract_bq_data
    from hs_nba_utils.modeling.postprocessing import build_output_dataframe
    from hs_nba_utils.etl.load import create_temp_table, insert_from_temp_table
    
    # load output table config
    d_output_table_config = safe_load(pth_output_table_config.open())
    
    # load scores from bq
    pth_extract_scores = pth_queries / 'extract_model_scores.sql'
    df_scores = extract_bq_data(bq_client, pth_query=pth_extract_scores)
    
    # postprocess output
    df_to_load = build_output_dataframe(df_scores, d_output_table_config)
    
    # create temp table in bq
    temp_table_name = create_temp_table(
        project_id, output_dataset_id, aggregate_results_table_id, df_to_load
    )

    # insert data from temp into main table
    current_part_dt = str(df_to_load['part_dt'].max())
    insert_from_temp_table(
        project_id, output_dataset_id, aggregate_results_table_id, temp_table_name, current_part_dt,
        pth_queries / 'drop_current_part_dt.sql', pth_queries / 'insert_from_temp_table_aggregate_results.sql'
    )

    

