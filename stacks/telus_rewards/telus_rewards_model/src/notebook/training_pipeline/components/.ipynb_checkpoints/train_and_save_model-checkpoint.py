from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                        Metrics, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="telus_rewards_xgb_train_model.yaml",
)
def train_and_save_model(
            file_bucket: str,
            service_type: str,
            score_date_dash: str,
            score_date_val_dash: str,
            project_id: str,
            dataset_id: str,
            metrics: Output[Metrics],
            metricsc: Output[ClassificationMetrics]
):

    import gc
    import time
    import pandas as pd
    import numpy as np
    import pickle
    from google.cloud import storage
    from google.cloud import bigquery
    from sklearn.model_selection import train_test_split

    def get_lift(prob, y_test, q):
        result = pd.DataFrame(columns=['Prob', 'Redemption'])
        result['Prob'] = prob
        result['Redemption'] = y_test
        result['Decile'] = pd.qcut(result['Prob'], q, labels=[i for i in range(q, 0, -1)])
        add = pd.DataFrame(result.groupby('Decile')['Redemption'].mean()).reset_index()
        add.columns = ['Decile', 'avg_real_redemption_rate']
        result = result.merge(add, on='Decile', how='left')
        result.sort_values('Decile', ascending=True, inplace=True)
        lg = pd.DataFrame(result.groupby('Decile')['Prob'].mean()).reset_index()
        lg.columns = ['Decile', 'avg_model_pred_redemption_rate']
        lg.sort_values('Decile', ascending=False, inplace=True)
        lg['avg_redemption_rate_total'] = result['Redemption'].mean()
        lg = lg.merge(add, on='Decile', how='left')
        lg['lift'] = lg['avg_real_redemption_rate'] / lg['avg_redemption_rate_total']

        return lg    
    
    df_train = pd.read_csv('gs://{}/{}_train.csv.gz'.format(file_bucket, service_type),
                           compression='gzip')  
    df_test = pd.read_csv('gs://{}/{}_validation.csv.gz'.format(file_bucket, service_type),  
                          compression='gzip')

    #set up df_train
    client = bigquery.Client(project=project_id)
    sql_train = ''' SELECT * FROM `{}.{}.bq_telus_rwrd_redemption_targets` '''.format(project_id, dataset_id) 
    df_target_train = client.query(sql_train).to_dataframe()
    # df_target_train = df_target_train.loc[
    #     df_target_train['YEAR_MONTH'] == '-'.join(score_date_dash.split('-')[:2])]  # score_date_dash = '2022-08-31'
    df_target_train = df_target_train.loc[df_target_train['YEAR_MONTH'] == '2022-H2']  # score_date_dash = '2022-08-31'
    df_target_train['ban'] = df_target_train['ban'].astype('int64')
    df_target_train = df_target_train.groupby('ban').tail(1)
    df_train = df_train.merge(df_target_train[['ban', 'target_ind']], on='ban', how='left')
    df_train.rename(columns={'target_ind': 'target'}, inplace=True)
    # df_train.dropna(subset=['target'], inplace=True)
    df_train.fillna(0, inplace=True)
    df_train['target'] = df_train['target'].astype(int)
    print(df_train.shape)

    #set up df_test
    sql_test = ''' SELECT * FROM `{}.{}.bq_telus_rwrd_redemption_targets` '''.format(project_id, dataset_id) 
    df_target_test = client.query(sql_test).to_dataframe()
    # df_target_test = df_target_test.loc[
    #     df_target_test['YEAR_MONTH'] == '-'.join(score_date_val_dash.split('-')[:2])]  # score_date_dash = '2022-09-30'
    df_target_test = df_target_test.loc[df_target_test['YEAR_MONTH'] == '2023-H1']  # score_date_dash = '2022-08-31'
    df_target_test['ban'] = df_target_test['ban'].astype('int64')
    df_target_test = df_target_test.groupby('ban').tail(1)
    df_test = df_test.merge(df_target_test[['ban', 'target_ind']], on='ban', how='left')
    df_test.rename(columns={'target_ind': 'target'}, inplace=True)
    # df_test.dropna(subset=['target'], inplace=True)
    df_test.fillna(0, inplace=True) 
    df_test['target'] = df_test['target'].astype(int)
    print(df_test.shape)

    #set up features (list)
    cols_1 = df_train.columns.values
    cols_2 = df_test.columns.values
    cols = set(cols_1).intersection(set(cols_2))
    features = [f for f in cols if f not in ['ban', 'target']]

    #train test split
    df_train, df_val = train_test_split(df_train, shuffle=True, test_size=0.3, random_state=42,
                                        stratify=df_train['target']
                                        )

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
    import xgboost as xgb
    from sklearn.metrics import roc_auc_score

    xgb_model = xgb.XGBClassifier(
        learning_rate=0.01,
        n_estimators=100,
        max_depth=8,
        min_child_weight=1,
        gamma=0,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='binary:logistic',
        nthread=4,
        scale_pos_weight=1
        # seed=27
    )

    xgb_model.fit(X_train, y_train)
    print('xgb training done')

    from sklearn.preprocessing import normalize

    #predictions on X_val
    y_pred = xgb_model.predict_proba(X_val, ntree_limit=xgb_model.best_iteration)[:, 1]
    y_pred_label = (y_pred > 0.5).astype(int)
    auc = roc_auc_score(y_val, y_pred_label)
    metrics.log_metric("AUC", auc)

    pred_prb = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)[:, 1]
    lg = get_lift(pred_prb, y_test, 10)

    # save the model in GCS
    from datetime import datetime
    models_dict = {}
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    models_dict['create_time'] = create_time
    models_dict['model'] = xgb_model
    models_dict['features'] = features
    lg.to_csv('gs://{}/lift_on_scoring_data_{}.csv'.format(file_bucket, create_time, index=False))

    with open('model_dict.pkl', 'wb') as handle:
        pickle.dump(models_dict, handle)
    handle.close()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)

    MODEL_PATH = '{}_xgb_models/'.format(service_type)
    blob = bucket.blob(MODEL_PATH)
    if not blob.exists(storage_client):
        blob.upload_from_string('')

    model_name_onbkt = '{}{}_models_xgb_{}'.format(MODEL_PATH, service_type, models_dict['create_time'])
    blob = bucket.blob(model_name_onbkt)
    blob.upload_from_filename('model_dict.pkl')

    print(f"....model loaded to GCS done at {str(create_time)}")

    from sklearn.preprocessing import normalize

    #predictions on X_test
    pred_prb = xgb_model.predict_proba(X_test, ntree_limit=xgb_model.best_iteration)[:, 1]
    pred_prb = np.array(normalize([pred_prb]))[0]

    #join ban_test, X_test, y_test and pred_prb and print to csv
    #CHECK THE SIZE OF EACH COMPONENT BEFORE JOINING
    q=10
    df_ban_test = ban_test.to_frame()
    df_test_exp = df_ban_test.join(X_test) 
    df_test_exp['y_test'] = y_test
    df_test_exp['y_pred_proba'] = pred_prb
    df_test_exp['y_pred'] = (df_test_exp['y_pred_proba'] > 0.5).astype(int)
    df_test_exp['decile'] = pd.qcut(df_test_exp['y_pred_proba'], q, labels=[i for i in range(q, 0, -1)])

    lg = get_lift(pred_prb, y_test, q)

    df_test_exp.to_csv('gs://{}/df_test_exp.csv'.format(file_bucket, index=True))
    print("....df_test_exp done")

    lg.to_csv('gs://{}/lift_on_scoring_data.csv'.format(file_bucket, index=False))
    print("....lift_to_csv done")

    time.sleep(120)
