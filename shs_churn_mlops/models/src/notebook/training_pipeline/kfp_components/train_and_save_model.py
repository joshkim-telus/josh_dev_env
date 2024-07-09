import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                        Metrics, component)
from typing import NamedTuple

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="nba_product_reco_prospects_xgb_train_model.yaml",
)
def train_and_save_model(file_bucket: str
                        , resource_bucket: str
                        , stack_name: str
                        , service_type: str
                        , project_id: str
                        , dataset_id: str
                        , model_type: str
                        , pipeline_type: str 
                        , preprocess_output_csv: str
                        , save_file_name: str
                        , stats_file_name: str
                        , pipeline_path: str
                        , hs_nba_utils_path: str
                        , metrics: Output[Metrics]
                        , metricsc: Output[ClassificationMetrics]
                        , model: Output[Model]
                        # , token: str
                        )-> NamedTuple("output", [("col_list", list), ("model_uri", str)]):

    #### Import Libraries ####
    import os 
    import gc
    import sys
    import time
    import pickle
    import pandas as pd
    import numpy as np
    import xgboost as xgb

    from pathlib import Path
    from yaml import safe_load

    from datetime import datetime
    from google.cloud import storage
    from google.cloud import bigquery

    # for prod
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    sys.path.insert(0, pth_project.as_posix())

#     #### For wb
#     import google.oauth2.credentials
#     CREDENTIALS = google.oauth2.credentials.Credentials(token)
#     client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
#     job_config = bigquery.QueryJobConfig()
    
    #### For prod
    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig()
    
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
        bucket, pth_project, f'{stack_name}/{pipeline_path}/queries', split_prefix='training_pipeline'
    )
    
    blob = bucket.blob(f'{stack_name}/{pipeline_path}/model_config.yaml')
    blob.download_to_filename(pth_model_config)

    # import local modules
    from hs_nba_utils.modeling.train import train
    from hs_nba_utils.modeling.evaluate import evaluate
    from hs_nba_utils.modeling.save_model import save_model

    # load model config
    d_model_config = safe_load(pth_model_config.open())

    # train    
    df_result, xgb_model = train(file_bucket=file_bucket, 
                stack_name=stack_name, 
                pipeline_path=pipeline_path, 
                service_type=service_type, 
                model_type=model_type, 
                pipeline_type=pipeline_type, 
                d_model_config=d_model_config, 
                preprocess_output_csv=preprocess_output_csv, 
                save_file_name=save_file_name
                ) 
    
    print('training step successfully completed')
    
    # evaluate
    df_stats = evaluate(df_result=df_result, 
                file_bucket=file_bucket, 
                stack_name=stack_name, 
                pipeline_path=pipeline_path, 
                service_type=service_type, 
                model_type=model_type, 
                d_model_config=d_model_config, 
                stats_file_name=stats_file_name
                )
    
    print('evaluate step successfully completed')
    
    # save model 
    col_list, model_uri = save_model(model=xgb_model, 
                file_bucket=file_bucket, 
                stack_name=stack_name, 
                pipeline_path=pipeline_path, 
                service_type=service_type, 
                d_model_config=d_model_config
                )
    
    print('save model step successfully completed')
    
    print(model_uri)

    return (col_list, model_uri)