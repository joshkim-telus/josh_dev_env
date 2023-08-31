
import pandas as pd 
import numpy as np 
from google.cloud import storage 

def download_data_from_gcs(project_id, bucket_name, gcs_path, local_path): 
    
    """
    Example:
    Example Uploading to BI-Layer GCS Bucket:
    PROJECT_ID = 'bi-srv-aaaie-pr-c0c268' 
    BUCKET_NAME = 'customer_personas_vertex_ai_pipelines'
    GCS_PATH = 'speed_tiers_models/'
    QUANTILE_TRANSFORMER_LOCAL_PATH = 'quantile_transformer.joblib'

    download_data(
                  project_id = PROJECT_ID,
                  bucket_name = BUCKET_NAME,
                  gcs_path =  GCS_PATH + QUANTILE_TRANSFORMER_LOCAL_PATH,
                  local_path = QUANTILE_TRANSFORMER_LOCAL_PATH)
    quantile_transformer = joblib.load(QUANTILE_TRANSFORMER_LOCAL_PATH)
    """
    
    bucket = storage.Client(project=project_id).bucket(bucket_name) 
    blob = bucket.blob(gcs_path) 
    blob.download_to_filename(local_path) 
    
def upload_data_to_gcs(project_id, bucket_name, gcs_path, local_path): 
    
    """
    Example Uploading to BI-Layer GCS Bucket:
    PROJECT_ID = 'bi-srv-aaaie-pr-c0c268' 
    BUCKET_NAME = 'customer_personas_vertex_ai_pipelines'
    GCS_PATH = 'speed_tiers_models/'
    QUANTILE_TRANSFORMER_LOCAL_PATH = 'quantile_transformer.joblib'

    joblib.dump(quantile_transformer, QUANTILE_TRANSFORMER_LOCAL_PATH)

    upload_data(
                project_id = PROJECT_ID,
                bucket_name = BUCKET_NAME,
                gcs_path =  GCS_PATH + QUANTILE_TRANSFORMER_LOCAL_PATH,
                local_path = QUANTILE_TRANSFORMER_LOCAL_PATH)
    """ 
    
    bucket = storage.Client(project=project_id).bucket(bucket_name) 
    blob = bucket.blob(gcs_path) 
    blob.upload_from_filename(local_path)
    
