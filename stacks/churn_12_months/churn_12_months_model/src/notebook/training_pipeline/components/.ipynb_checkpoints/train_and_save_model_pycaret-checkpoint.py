import kfp
from kfp import dsl
from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)
from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                        Metrics, component)
from typing import NamedTuple

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="churn_12_months_pycaret_train_model.yaml",
)
def train_and_save_model_pycaret(file_bucket: str
                        , service_type: str
                        , project_id: str
                        , dataset_id: str
                        , metrics: Output[Metrics]
                        , metricsc: Output[ClassificationMetrics]
                        , model: Output[Model]
                        , token: str
                        )-> NamedTuple("output", [("col_list", list), ("model_uri", str)]):

    #### Import Libraries ####

    import gc
    import time
    import pandas as pd
    import numpy as np
    import pickle
    import xgboost as xgb
    import seaborn as sns
    import logging 
    from datetime import datetime
    from sklearn.metrics import roc_auc_score
    from sklearn.preprocessing import normalize
    from sklearn.model_selection import train_test_split
    from google.cloud import storage
    from google.cloud import bigquery

    from pycaret.classification import setup,create_model,tune_model, predict_model,get_config,compare_models,save_model,tune_model, models
    from sklearn.metrics import accuracy_score, roc_auc_score, precision_recall_curve, mean_squared_error, f1_score, precision_score, recall_score, confusion_matrix, roc_curve
    from pycaret.datasets import get_data
    
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

    df_train = pd.read_csv('gs://{}/{}/{}_train.csv'.format(file_bucket, service_type, service_type))
    df_test = pd.read_csv('gs://{}/{}/{}_validation.csv'.format(file_bucket, service_type, service_type))

    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    job_config = bigquery.QueryJobConfig()

#     #### For prod 
#     client = bigquery.Client(project=project_id)
#     job_config = bigquery.QueryJobConfig()

    #### Define Variables
    
    # Define target variable
    target = 'target'
    drop_cols = ['ban', 'target']
    cat_feat = []

    # define X and y
    X = df_train.drop(columns=drop_cols) 
    y = df_train[target]
    
    X_test = df_test.drop(columns=drop_cols) 
    y_test = df_test[target]

    # Split the data into training and testing sets with a 70-30 split
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.3, random_state=42)

    df_train = X_train.join(y_train)
    df_val = X_val.join(y_val)
    df_test = X_test.join(y_test)
    
    # save backups
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_train.to_csv('gs://{}/{}/backup/{}_train_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_val.to_csv('gs://{}/{}/backup/{}_val_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    df_test.to_csv('gs://{}/{}/backup/{}_test_{}.csv'.format(file_bucket, service_type, service_type, create_time))
    
    #set up features (list)
    cols_1 = df_train.columns.values
    cols_2 = df_test.columns.values
    cols = set(cols_1).intersection(set(cols_2))
    features = [f for f in cols if f not in ['ban', 'target']]

    # assign numeric and categorical features
    numeric_features = [col for col in df_train.columns if col not in drop_cols+cat_feat]
    categorical_features = [col for col in df_train.columns if col in cat_feat]

    #### Pycaret Setup initialize
    classification_setup = setup(data=df_train, 
                             target=target,
                             normalize=True,
                             normalize_method='zscore',
                             log_experiment=False,
                             fold=5,
                             fold_shuffle=True,
                             session_id=123,
                             numeric_features=numeric_features,
                             categorical_features=categorical_features, 
                             fix_imbalance=True, 
                             remove_multicollinearity=True, 
                             multicollinearity_threshold=0.95, 
                             silent=True)
    
    ##### experiment with xgboost
    top_models = compare_models(include = ['lightgbm'], errors='raise', n_select=1)

    # assign best_model to models for code simplicity
    if type(top_models) == "list": 
        models = top_models.copy() 
    else: 
        models = [top_models].copy()

    # define dictionaries to contain results
    eval_results = {}
    model_dict = {}

    for i in range(len(models)):

        # print model name
        model_name = models[i].__class__.__name__
        print(model_name)

        # Get predictions on test set for model
        predictions = predict_model(models[i], data=df_test, raw_score=True)

        # Actual vs predicted
        y_true = predictions[target]
        y_pred = predictions["Label"]
        y_score = predictions["Score_1"]

        # calculate Accuracy, AUC, Recall, Precision, F1 
        accuracy = accuracy_score(y_true, y_pred)
        auc = roc_auc_score(y_true, y_score)
        recall = recall_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred)

        # use rmse as the key indicator for best performance
        eval_results[model_name] = f1
        model_dict[model_name] = models[i]

    # Find the model with the highest f1 score
    top_model = max(eval_results, key=eval_results.get)

    # Print the result
    print("The top performing model on the test dataset:", top_model, ", f1 score:", eval_results[top_model])

    #### Model Tuning ###
    model_base = create_model(model_dict[top_model])
    tuned_model, tuner = tune_model(model_base, optimize='F1', return_tuner = True, n_iter = 25)
    # model_reports_tuned, model_to_report_map_tuned = evaluate_and_save_models(models=tuned_model, 
    #                                      bucket_name=bucket_name,
    #                                      save_path=save_path, 
    #                                      test_df=test_df,
    #                                      actual_label_str='target',
    #                                      columns = get_config('X_train').columns,
    #                                      save_columns=True,
    #                                      show_report=False)
    
    #### Final Evaluation ####
    # print model name
    model_name = tuned_model.__class__.__name__
    print(f'model_name: {model_name}')

    # Get predictions on test set for model
    predictions = predict_model(tuned_model, data=df_test, raw_score=True, round=10)

    # Actual vs predicted
    y_true = predictions[target]
    y_pred = predictions["Label"]
    y_score = predictions["Score_1"]
    
    # get lift
    lg = get_lift(y_score, y_true, 10)

    # save the lift calc in GCS
    models_dict = {}
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    models_dict['create_time'] = create_time
    models_dict['model'] = tuned_model
    models_dict['features'] = features
    lg.to_csv('gs://{}/{}/lift_on_scoring_data_{}.csv'.format(file_bucket, service_type, create_time, index=False))

    # calculate Accuracy, AUC, Recall, Precision, F1 
    accuracy = accuracy_score(y_true, y_pred)
    auc = roc_auc_score(y_true, y_score)
    recall = recall_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    print(f'accuracy: {accuracy}')
    print(f'auc: {auc}')
    print(f'recall: {recall}')
    print(f'precision: {precision}')
    print(f'f1: {f1}')

    with open('model_dict.pkl', 'wb') as handle:
        pickle.dump(models_dict, handle)
    handle.close()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)

    model_path = '{}/{}_models/'.format(service_type, service_type)
    blob = bucket.blob(model_path)
    if not blob.exists(storage_client):
        blob.upload_from_string('')

    model_name_onbkt = '{}{}_models_{}'.format(model_path, service_type, models_dict['create_time'])
    blob = bucket.blob(model_name_onbkt)
    blob.upload_from_filename('model_dict.pkl')
    
    model.uri = f'gs://{file_bucket}/{model_name_onbkt}'
    
    print(f"....model loaded to GCS done at {str(create_time)}")

    col_list = features

    return (col_list, model.uri)

    