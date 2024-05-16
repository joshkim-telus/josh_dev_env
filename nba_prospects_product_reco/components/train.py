from kfp.v2.dsl import component
from typing import Any


@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-xgboost-slim:latest",
    output_component_file="hs_nba_existing_customers_predict.yaml"
)
def predict(
    project_id: str,
    resource_bucket: str,
    pipeline_bucket: str,
    stack_name: str,
    pipeline_path: str,
    hs_nba_utils_path: str
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
    
    # set global vars
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
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
    bucket = storage_client.bucket(resource_bucket)
    extract_dir_from_bucket(
        bucket, pth_project, f'{stack_name}/{hs_nba_utils_path}', split_prefix='notebook'
    )  
    blob = bucket.blob(f"{stack_name}/{pipeline_path}/model_config.yaml" )
    blob.download_to_filename(pth_model_config)

    # download model pickle file
    model_name = 'xgb_model.pkl'
    bucket = storage_client.bucket(pipeline_bucket)
    blob = bucket.blob(f'models/{model_name}')
    blob.download_to_filename(pth_project / model_name)

    # load model
    d_model_config = safe_load(pth_model_config.open())
    with open(pth_project / model_name, "rb") as f:
        model = pickle.load(f)

    # load data from bucket
    df_features = pd.read_csv(f'gs://{pipeline_bucket}/data/features.csv')
    print(f'Features df.shape {df_features.shape}')

    # select features and model predict
    l_feature_names = [f['name'] for f in d_model_config['features']]
    np_preds = model.predict_proba(df_features[l_feature_names])
    
    # map target index with target names
    d_rename_mapping = {
        d_target_info['class_index']: d_target_info['name']
        for d_target_info in d_model_config['target_variables']
    }

    # build result dataframe
    df_preds = pd.DataFrame(np_preds)
    df_preds = df_preds.rename(columns=d_rename_mapping)

    # add customer ids and partition date
    df_preds[d_model_config['customer_ids']] = df_features[d_model_config['customer_ids']]
    df_preds['part_dt'] = df_features['part_dt']

    # save data to pipeline bucket
    location_to_save = f'gs://{pipeline_bucket}/data/scores.csv'
    df_preds.to_csv(location_to_save, index=False)
    print(f'Scores saved into {location_to_save}')