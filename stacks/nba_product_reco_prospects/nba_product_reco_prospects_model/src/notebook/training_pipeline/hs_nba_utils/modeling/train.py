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
# import warnings
# from pandas.errors import SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
# warnings.simplefilter(action='ignore', category=FutureWarning)

def train(file_bucket: str
          , service_type: str
          , model_type: str
          , d_model_config: dict
          , train_csv: str
          , save_file_name: str
         ):
    """
    This function trains the xgb model based on set parameters and returns 'xgb_model'. It takes the following parameters:
    
    Args:
        - file_bucket: A GCS Bucket where training dataset is saved.
        - service_type: Service type name
        - model_type: 'acquisition' or 'tier'
        - d_model_config: A dictionary containing the metadata information for the model.
        - train_csv: The name of training dataset csv file saved in the gcs bucket. 
        - save_file_name: The name of validation dataset with predictions to be saved in the gcs bucket.

    Returns:
        - pd.DataFrame: The processed dataframe with additional features and mapped target values.
    """
    
    df = pd.read_csv(f'gs://{file_bucket}/{service_type}/{train_csv}', index_col=False)  
    
    #set up features (list)
    features = [d_f['name'] for d_f in d_model_config['features']]
    
    # target 
    target = d_model_config['target']

    # train val test split
    df_train = df[df['split_type'] == '1-train']
    df_val = df[df['split_type'] == '2-val']
    df_test = df[df['split_type'] == '3-test']
    
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    df_train.to_csv('gs://{}/{}/backup/{}_train_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_val.to_csv('gs://{}/{}/backup/{}_val_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_test.to_csv('gs://{}/{}/backup/{}_test_{}.csv'.format(file_bucket, service_type, service_type, create_time))

    ban_train = df_train[['ban', 'lpds_id']]
    X_train = df_train[features]
    y_train = np.squeeze(df_train['target'].values)

    ban_val = df_val[['ban', 'lpds_id']]
    X_val = df_val[features]
    y_val = np.squeeze(df_val['target'].values)
    
    ban_test = df_test[['ban', 'lpds_id']]
    X_test = df_test[features]
    y_test = np.squeeze(df_test['target'].values)

    del df_train, df_val, df_test
    gc.collect()

#     xgb_model = xgb.XGBClassifier(
#         learning_rate=0.05,
#         n_estimators=250,
#         max_depth=8,
#         min_child_weight=1,
#         gamma=0,
#         subsample=0.8,
#         colsample_bytree=0.8,
#         objective='multi:softproba',
#         num_class=10, 
#         eval_metric='mlogloss', 
#         nthread=4,
#         seed=27,
#         verbose=1
#     )
    
#     xgb_model.fit(X_train, y_train)
#     print('xgb training done')

    xgb_model = xgb.XGBClassifier()
    xgb_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)]
    )
    print('xgb training done')
    
    #predictions on X_val
    y_pred = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)

    df_ban_test = ban_test
    df_test_exp = df_ban_test.join(X_test) 
    df_test_exp[target] = y_test

    # extract target name - index mapping
    d_target_mapping = {
        d_target_info['class_index']: d_target_info['name'] 
        for d_target_info in d_model_config['target_variables'][model_type]
    }
    
    n_targets = len(d_model_config['target_variables'][model_type])
    for i in range(n_targets): 
        df_test_exp[d_target_mapping[i]] = y_pred[:, i]
    
    df_test_exp.to_csv(f'gs://{file_bucket}/{service_type}/{save_file_name}', index=False)
    
    return df_test_exp, xgb_model


        