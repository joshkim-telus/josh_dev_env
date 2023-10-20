import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                        Metrics, component)
from typing import NamedTuple

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="churn_12_months_xgb_train_model.yaml",
)
def train_and_save_model(file_bucket: str
                        , service_type: str
                        , project_id: str
                        , dataset_id: str
                        , metrics: Output[Metrics]
                        , metricsc: Output[ClassificationMetrics]
                        , model: Output[Model]
                        , token: str
                        )-> NamedTuple("output", [("col_list", list), ("model_uri", str)]):

    import gc
    import time
    import pandas as pd
    import numpy as np
    import pickle
    import xgboost as xgb
    from datetime import datetime
    from sklearn.metrics import roc_auc_score
    from sklearn.preprocessing import normalize
    from sklearn.model_selection import train_test_split
    from google.cloud import storage
    from google.cloud import bigquery

    def get_lift(prob, y_test, q):
        result = pd.DataFrame(columns=['Prob', 'Churn'])
        result['Prob'] = prob
        result['Churn'] = y_test
        # result['Decile'] = pd.qcut(1-result['Prob'], 10, labels = False)
        result['Decile'] = pd.qcut(result['Prob'], q, labels=[i for i in range(q, 0, -1)])
        add = pd.DataFrame(result.groupby('Decile')['Churn'].mean()).reset_index()
        add.columns = ['Decile', 'avg_real_churn_rate']
        result = result.merge(add, on='Decile', how='left')
        result.sort_values('Decile', ascending=True, inplace=True)
        lg = pd.DataFrame(result.groupby('Decile')['Prob'].mean()).reset_index()
        lg.columns = ['Decile', 'avg_model_pred_churn_rate']
        lg.sort_values('Decile', ascending=False, inplace=True)
        lg['avg_churn_rate_total'] = result['Churn'].mean()
        lg['total_churn'] = result['Churn'].sum()
        lg = lg.merge(add, on='Decile', how='left')
        lg['lift'] = lg['avg_real_churn_rate'] / lg['avg_churn_rate_total']

        return lg

    df_train = pd.read_csv('gs://{}/{}/{}_train.csv'.format(file_bucket, service_type, service_type), index_col=False)  
    df_test = pd.read_csv('gs://{}/{}/{}_validation.csv'.format(file_bucket, service_type, service_type), index_col=False)

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()

    #set up features (list)
    cols_1 = df_train.columns.values
    cols_2 = df_test.columns.values
    cols = set(cols_1).intersection(set(cols_2))
    features = [f for f in cols if f not in ['ban', 'target', 'Unnamed: 0']]

    #train test split
    df_train, df_val = train_test_split(df_train, shuffle=True, test_size=0.25, random_state=42,
                                        stratify=df_train['target']
                                        )
    
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_train.to_csv('gs://{}/{}/backup/{}_train_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_val.to_csv('gs://{}/{}/backup/{}_val_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_test.to_csv('gs://{}/{}/backup/{}_test_{}.csv'.format(file_bucket, service_type, service_type, create_time))

    ban_train = df_train['ban']
    X_train = df_train[features]
    y_train = np.squeeze(df_train['target'].values)

    ban_val = df_val['ban']
    X_val = df_val[features]
    y_val = np.squeeze(df_val['target'].values)

    ban_test = df_test['ban']
    X_test = df_test[features]
    y_test = np.squeeze(df_test['target'].values)

    del df_train, df_val, df_test
    gc.collect()

    # build model and fit in training data
    # xgb_model = xgb.XGBClassifier(
    #     learning_rate=0.1,
    #     n_estimators=100,
    #     max_depth=8,
    #     min_child_weight=1,
    #     gamma=0,
    #     subsample=0.8,
    #     colsample_bytree=0.8,
    #     objective='binary:logistic',
    #     nthread=4,
    #     scale_pos_weight=1
    #     # seed=27
    # )

    xgb_model = xgb.XGBClassifier(
        learning_rate=0.02,
        n_estimators=1000,
        max_depth=10,
        min_child_weight=1,
        gamma=0,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='binary:logistic',
        nthread=4,
        scale_pos_weight=1,
        seed=27
    )
    
    xgb_model.fit(X_train, y_train)
    print('xgb training done')

    #predictions on X_val
    y_pred = xgb_model.predict_proba(X_val, ntree_limit=xgb_model.best_iteration)[:, 1]
    y_pred_label = (y_pred > 0.5).astype(int)
    auc = roc_auc_score(y_val, y_pred_label)
    metrics.log_metric("AUC", auc)

    pred_prb = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)[:, 1]
    lg = get_lift(pred_prb, y_test, 10)

    # save the model in GCS
    models_dict = {}
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    models_dict['create_time'] = create_time
    models_dict['model'] = xgb_model
    models_dict['features'] = features
    lg.to_csv('gs://{}/{}/lift_on_scoring_data_{}.csv'.format(file_bucket, service_type, create_time, index=False))

    with open('model_dict.pkl', 'wb') as handle:
        pickle.dump(models_dict, handle)
    handle.close()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)

    MODEL_PATH = '{}/{}_xgb_models/'.format(service_type, service_type)
    blob = bucket.blob(MODEL_PATH)
    if not blob.exists(storage_client):
        blob.upload_from_string('')

    model_name_onbkt = '{}{}_models_xgb_{}'.format(MODEL_PATH, service_type, models_dict['create_time'])
    blob = bucket.blob(model_name_onbkt)
    blob.upload_from_filename('model_dict.pkl')
    
    model.uri = f'gs://{file_bucket}/{model_name_onbkt}'
    
    print(f"....model loaded to GCS done at {str(create_time)}")

    col_list = features

    return (col_list, model.uri)

    
