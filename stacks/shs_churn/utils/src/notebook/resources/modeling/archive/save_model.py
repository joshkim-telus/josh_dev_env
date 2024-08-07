# import global modules
import sys
import os
import gc
import pickle
import numpy as np
import pandas as pd
import xgboost as xgb

from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

from sklearn.model_selection import train_test_split

from typing import List, Dict, Tuple, Optional
# import warnings
# from pandas.errors import SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
# warnings.simplefilter(action='ignore', category=FutureWarning)

def save_model(model, 
            file_bucket:str, 
            service_type: str,
            d_model_config:dict
            ):

    """
    Function to save a model to gcs bucket.

    Args:
        - model (xgb.Booster or xgb.XGBRegressor/XGBClassifier): The pre-trained XGBoost model.
        - file_bucket: A GCS Bucket where training dataset is saved.
        - service_type: Service type name
        - d_model_config: A dictionary containing the metadata information for the model.

    Returns:
        col_list: name of the features in the training dataset
        model_uri: The gcs location where model artifacts are saved
    """

    model_class_name = model.__class__.__name__

    #### Save the Report and Model 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)
    
    features = [f['name'] for f in d_model_config['features']] 

    # save the model in GCS
    models_dict = {}
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    models_dict['create_time'] = create_time
    models_dict['model'] = model
    models_dict['features'] = features

    with open('model_dict.pkl', 'wb') as handle:
        pickle.dump(models_dict, handle)
    handle.close()

    model_path = 'models/'
        
    blob = bucket.blob(model_path)
    if not blob.exists(storage_client):
        blob.upload_from_string('')

    model_name_onbkt = f'{model_path}{service_type}_xgb_models_latest.pkl'
    blob = bucket.blob(model_name_onbkt)
    blob.upload_from_filename('model_dict.pkl')
        
    model_name_onbkt = f'{model_path}{service_type}_xgb_models_{create_time}.pkl'
    blob = bucket.blob(model_name_onbkt)
    blob.upload_from_filename('model_dict.pkl')

    model.uri = f'gs://{file_bucket}/{model_name_onbkt}'

    print(f"....model loaded to GCS done at {str(create_time)}")

    col_list = features

    return (col_list, model.uri)
    
        