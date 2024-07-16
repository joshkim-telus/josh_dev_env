# import global modules
import sys
import os
import gc
import numpy as np
import pandas as pd
import xgboost as xgb
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from typing import List, Dict, Tuple, Optional

def train(file_bucket: str
          , stack_name: str
          , pipeline_path: str
          , service_type: str
          , model_type: str
          , pipeline_type: str
          , d_model_config: dict
          , preprocess_output_csv: str
          , save_file_name: str
         ):
    """
    This function trains the xgb model based on set parameters and returns 'xgb_model'. It takes the following parameters:
    
    Args:
        - file_bucket: A GCS Bucket where training dataset is saved.
        - stack_name: Model stack name
        - pipeline_path: A GCS Pipeline path where related files/artifacts will be saved. 
        - service_type: Service type name
        - model_type: 'acquisition' or 'tier'
        - d_model_config: A dictionary containing the metadata information for the model.
        - preprocess_output_csv: The name of training dataset csv file saved in the gcs bucket. 
        - save_file_name: The name of validation dataset with predictions to be saved in the gcs bucket.

    Returns:
        - pd.DataFrame: The processed dataframe with additional features and mapped target values.
    """
    
    df = pd.read_csv(f'gs://{file_bucket}/{pipeline_type}/{preprocess_output_csv}', index_col=False)
    
    #set up features (list)
    features = [d_f['name'] for d_f in d_model_config['features']]
    
    # target 
    target = d_model_config['target']

    # train val test split
    df_train = df[df['split_type'] == '1-train']
    df_val = df[df['split_type'] == '2-val']
    df_test = df[df['split_type'] == '3-test']
    
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    df_train.to_csv(f'gs://{file_bucket}/backup/{service_type}_train_{create_time}.csv', index=False)
    df_val.to_csv(f'gs://{file_bucket}/backup/{service_type}_val_{create_time}.csv', index=False)
    df_test.to_csv(f'gs://{file_bucket}/backup/{service_type}_test_{create_time}.csv', index=False)

    ban_train = df_train[['ban', 'cust_id']]
    X_train = df_train[features]
    y_train = np.squeeze(df_train[target].values)

    ban_val = df_val[['ban', 'cust_id']]
    X_val = df_val[features]
    y_val = np.squeeze(df_val[target].values)
    
    ban_test = df_test[['ban', 'cust_id']]
    X_test = df_test[features]
    y_test = np.squeeze(df_test[target].values)

    del df_train, df_val, df_test
    gc.collect()

    xgb_model = xgb.XGBClassifier()

#     xgb_model = xgb.XGBClassifier(
#         learning_rate=0.05,
#         n_estimators=250,
#         max_depth=8,
#         min_child_weight=1,
#         gamma=0,
#         subsample=0.8,
#         colsample_bytree=0.8,
#         objective='binary:logistic',
#         nthread=4,
#         scale_pos_weight=1,
#         seed=27
#     )
    
    xgb_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)]
    )
    print('xgb training done')
    
    #predictions on X_val
    y_pred = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)[:, 1]

    df_ban_test = ban_test
    df_test_exp = df_ban_test.join(X_test) 
    df_test_exp[target] = y_test

    df_test_exp.to_csv(f'gs://{file_bucket}/{pipeline_type}_results/{save_file_name}', index=False)
    
    # prepare for evaluate.py
    y_true = y_test
    y_score = y_pred
    y_pred = (y_pred > 0.5).astype(int)

    return df_test_exp, xgb_model, y_true, y_pred, y_score
    
    