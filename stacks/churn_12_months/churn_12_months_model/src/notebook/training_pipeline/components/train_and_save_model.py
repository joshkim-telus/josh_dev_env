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

    #### Import Libraries ####

    telus_purple = '#4B286D'
    telus_green = '#66CC00'
    telus_grey = '#F4F4F7'
    
    import os 
    import gc
    import time
    import pickle
    import joblib
    import logging 
    import pandas as pd
    import numpy as np
    import xgboost as xgb
    import seaborn as sns
    
    import matplotlib.pyplot as plt
    import plotly.graph_objs as go
    import plotly.express as px

    from plotly.subplots import make_subplots
    from datetime import datetime
    from sklearn.metrics import roc_auc_score
    from sklearn.preprocessing import normalize
    from sklearn.model_selection import train_test_split
    from google.cloud import storage
    from google.cloud import bigquery

    from pycaret.classification import setup,create_model,tune_model, predict_model,get_config,compare_models,save_model,tune_model, models
    from sklearn.metrics import accuracy_score, roc_auc_score, precision_recall_curve, mean_squared_error, f1_score, precision_score, recall_score, confusion_matrix, roc_curve, classification_report
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
    
    def create_folder_if_not_exists(path):
        """
        Create a new folder based on a path if that path does not currently exist.
        """
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Folder created: {path}")
        else:
            print(f"Folder already exists: {path}")

    def ploty_model_metrics(actual, predicted, plot=False):
        f1_score_ = f1_score(actual, predicted)
        recall_score_ = recall_score(actual, predicted)
        acc_score_ = accuracy_score(actual, predicted)
        pr_score_ = precision_score(actual, predicted)

        metrics_df = pd.DataFrame(data=[[acc_score_, pr_score_,recall_score_, f1_score_,]],
                                  columns=['Accuracy', 'Precision', 'Recall', 'F1_score'])

        trace = go.Bar(x = (metrics_df.T[0].values), 
                        y = list(metrics_df.columns), 
                        text = np.round_(metrics_df.T[0].values,4),
                        textposition = 'auto',
                        orientation = 'h', 
                        opacity = 0.8,
                        marker=dict(
                                    color=[telus_purple] * 4,
                                    line=dict(color='#000000',width=1.5)
                                )
                       )
        fig = go.Figure()
        fig.add_trace(trace)
        fig.update_layout(title='Model Metrics')

        if plot:
            fig.show()
        return  metrics_df, fig

    def plotly_confusion_matrix(actual, 
                                predicted, 
                                axis_labels='',
                                plot=False):
        cm=confusion_matrix(actual, predicted)

        if axis_labels=='':
            x = [str(x) for x in range(pd.Series(actual).nunique())]
            #list(np.arange(0, actual.nunique()))
            y = x
        else:
            y = axis_labels
            x = axis_labels

        fig = px.imshow(cm, 
                    text_auto=True,
                    aspect='auto',
                    color_continuous_scale = 'Blues',
                    labels = dict(x = "Predicted Labels",
                                  y= "Actual Labels"),
                    x = x,
                    y = y
                    )
        if plot:
            fig.show()

        return cm, fig

    def plotly_output_hist(actual,
                           prediction_probs,
                           plot=False
                          ):
        hist_ = px.histogram(x = prediction_probs,
                             color = actual,
                             nbins=100,
                             labels=dict(color='True Labels',
                                         x = 'Prediction Probability'
                                        )
                            )
        if plot:
            hist_.show()


        return hist_


    def plotly_precision_recall(actual,
                                predictions_prob,
                                plot=False
                               ):
        prec, recall, threshold = precision_recall_curve(actual, predictions_prob)

        trace = go.Scatter(
                            x=recall,
                            y=prec,
                            mode='lines',
                            line=dict(color=telus_purple),
                            fill='tozeroy',
                            name='Precision-Recall curve'
                        )
        layout = go.Layout(
                            title='Precision-Recall Curve',
                            xaxis=dict(title='Recall'),
                            yaxis=dict(title='Precision')
                        )
        fig = go.Figure(data=[trace], layout=layout)

        if plot:
            fig.show()

        return fig

    def plotly_roc(actual,
                    predictions_prob,
                    plot=False
                   ):
        auc_score = roc_auc_score(actual, predictions_prob)
        fpr, tpr, thresholds  = roc_curve(actual, predictions_prob)

        df = pd.DataFrame({
                            'False Positive Rate': fpr,
                            'True Positive Rate': tpr
                          }, 
                            index=thresholds)
        df.index.name = "Thresholds"
        df.columns.name = "Rate"
        df = df.loc[df.index <= 1]
        fig_tpr_fpr = 0

        fig_tpr_fpr= px.line(
                            df, 
                            title='TPR and FPR at every threshold',
                        )

        # ROC Curve with AUC
        trace = go.Scatter(
                    x=fpr,
                    y=tpr,
                    mode='lines',
                    line=dict(color=telus_purple),
                    fill='tozeroy',
                    name='Precision-Recall curve'
                )
        layout = go.Layout(
                            title=f'ROC Curve (AUC={auc_score:.4f})',
                            xaxis=dict(title='False Positive Rate'),
                            yaxis=dict(title='True Positive Rate')
                        )
        fig_roc = go.Figure(data=[trace], layout=layout)

        fig_roc.add_shape(
            type='line', line=dict(dash='dash'),
            x0=0, x1=1, y0=0, y1=1
        )
        fig_roc.update_xaxes(constrain='domain')

        if plot:
            fig_tpr_fpr.show()
            fig_roc.show()


        return fig_tpr_fpr, fig_roc, df, auc_score

    def plotly_lift_curve(actual,
                          predictions_prob,
                          step=0.01,
                          plot=False
                         ):
        #Define an auxiliar dataframe to plot the curve
        aux_lift = pd.DataFrame()
        #Create a real and predicted column for our new DataFrame and assign values
        aux_lift['real'] = actual
        aux_lift['predicted'] = predictions_prob
        #Order the values for the predicted probability column:
        aux_lift.sort_values('predicted',ascending=False,inplace=True)

        #Create the values that will go into the X axis of our plot
        x_val = np.arange(step,1+step,step)
        #Calculate the ratio of ones in our data
        ratio_ones = aux_lift['real'].sum() / len(aux_lift)
        #Create an empty vector with the values that will go on the Y axis our our plot
        y_v = []

        #Calculate for each x value its correspondent y value
        for x in x_val:
            num_data = int(np.ceil(x*len(aux_lift))) #The ceil function returns the closest integer bigger than our number 
            data_here = aux_lift.iloc[:num_data,:]   # ie. np.ceil(1.4) = 2
            ratio_ones_here = data_here['real'].sum()/len(data_here)
            y_v.append(ratio_ones_here / ratio_ones)



        # Lift Curve 
        trace = go.Scatter(
                    x=x_val,
                    y=y_v,
                    mode='lines',
                    line=dict(color=telus_purple),

                    name='Lift Curve'
                )
        layout = go.Layout(
                            title=f'Lift Curve',
                            xaxis=dict(title='Proportion of Sample'),
                            yaxis=dict(title='Lift')
                        )
        fig_lift = go.Figure(data=[trace], layout=layout)

        fig_lift.add_shape(
            type='line', line=dict(dash='dash'),
            x0=0, x1=1, y0=1, y1=1
        )
        fig_lift.update_xaxes(constrain='domain')

        if plot:
            fig_lift.show()

        return fig_lift

    def plotly_feature_importance(model,
                                  columns,
                                  plot=False):
        coefficients  = pd.DataFrame(model.feature_importances_)
        column_data   = pd.DataFrame(columns)
        coef_sumry    = (pd.merge(coefficients,column_data,left_index= True,
                                  right_index= True, how = "left"))

        coef_sumry.columns = ["coefficients","features"]
        coef_sumry = coef_sumry.sort_values(by = "coefficients",ascending = False)

        fig_feats = 0
        trace= go.Bar(y = coef_sumry["features"].head(15).iloc[::-1],
                      x = coef_sumry["coefficients"].head(15).iloc[::-1],
                      name = "coefficients",
                      marker = dict(color = coef_sumry["coefficients"],
                                    colorscale = "Viridis",
                                    line = dict(width = .6,color = "black")
                                   ),
                      orientation='h'
                     )
        layout = go.Layout(
                            title='Feature Importance',
                            yaxis=dict(title='Features')

                        )
        fig_feats = go.Figure(data=[trace], layout=layout)

        if plot:
            fig_feats.update_yaxes(automargin=True)
            fig_feats.show()
        return coef_sumry, fig_feats

    def plotly_model_report(model,
                            actual,
                            predicted,
                            predictions_prob,
                            bucket_name,
                            show_report = True,
                            columns=[],
                            save_path = ''
                           ):
        print(model.__class__.__name__)

        metrics_df, fig_metrics = ploty_model_metrics(actual, 
                                                  predicted, 
                                                  plot=False)
        cm, fig_cm = plotly_confusion_matrix(actual, 
                                            predicted, 
                                            axis_labels='',
                                            plot=False)
        fig_hist = plotly_output_hist(actual, 
                                      prediction_probs=predictions_prob,
                                      plot=False)
        fig_pr = plotly_precision_recall(actual,
                                    predictions_prob,
                                    plot=False
                                   )
        fig_tpr_fpr, fig_roc, _, auc_score = plotly_roc(actual,
                                    predictions_prob,
                                    plot=False
                                   )
        try:
            coefs_df, fig_feats = plotly_feature_importance(model=model, 
                                                            columns = columns,
                                                              plot=False)
            coefs_df=coefs_df.to_dict()
        except:
            coefs_df = 0
            pass
        fig_lift = plotly_lift_curve(actual,
                              predictions_prob,
                              step=0.01,
                              plot=False
                             )
        # Figure out how to put this into report on Monday -> Not Urgent
        cr=classification_report(actual,predicted, output_dict=True)

        # Generate dataframe with summary of results in one row

        results_cols = ['date', 'model_name', 'estimator_type', 
                        'confusion_matrix','classification_report', 
                        'auc_score', 'feature_importances']
        results_list = [datetime.now().strftime("%Y-%m-%d"), model.__class__.__name__,  model._estimator_type,
                        cm, cr,
                        auc_score, coefs_df
                       ]

        results_df_combined = pd.concat([pd.DataFrame([results_list], columns=results_cols),
                                         metrics_df],
                                       axis=1)

        # Generate Plotly page report

        report_fig = make_subplots(rows=4, 
                                cols=2, 
                                print_grid=False, 
                                specs=[[{}, {}], 
                                     [{}, {}],
                                     [{}, {}],
                                     [{}, {}],
                                     ],
                                subplot_titles=('Confusion Matrix',
                                            'Model Metrics',
                                            'Probability Output Histogram',
                                            'Precision - Recall curve',
                                            'TPR & FPR Vs. Threshold',
                                            f'ROC Curve: AUC Score {np.round(auc_score, 3)}',                                        
                                            'Feature importance',
                                            'Lift Curve'
                                            )
                                )        

        report_fig.append_trace(fig_cm.data[0],1,1)
        report_fig.update_coloraxes(showscale=False)
        report_fig.append_trace(fig_metrics.data[0],1,2)

        report_fig.append_trace(fig_hist.data[0],2,1)
        report_fig.append_trace(fig_hist.data[1],2,1)
        report_fig.append_trace(fig_pr.data[0],2,2)

        report_fig.append_trace(fig_tpr_fpr.data[0],3,1)
        report_fig.append_trace(fig_tpr_fpr.data[1],3,1)
        report_fig.append_trace(fig_roc.data[0],3,2)
        try:
            report_fig.append_trace(fig_feats.data[0],4,1)
        except:
            pass
        report_fig.append_trace(fig_lift.data[0],4,2)    
        title_str = f"{model.__class__.__name__} : Model performance report"
        report_fig['layout'].update(title = f'<b>{title_str}</b><br>',
                        autosize = True, height = 1500,width = 1200,
                        plot_bgcolor = 'rgba(240,240,240, 0.95)',
                        paper_bgcolor = 'rgba(240,240,240, 0.95)',
                        margin = dict(b = 195))

        report_fig["layout"]["xaxis1"].update(dict(title = "Predicted Labels"))
        report_fig["layout"]["yaxis1"].update(dict(title = "Actual Labels"))

        report_fig["layout"]["xaxis3"].update(dict(title = "Prediction Probabilities"))
        report_fig["layout"]["yaxis3"].update(dict(title = "Count"))

        report_fig["layout"]["xaxis4"].update(dict(title = "Recall"))
        report_fig["layout"]["yaxis4"].update(dict(title = "Precision"))

        report_fig["layout"]["xaxis5"].update(dict(title = "Thresholds"))
        report_fig["layout"]["yaxis5"].update(dict(title = "Rate"))

        report_fig["layout"]["xaxis6"].update(dict(title = "False Positive Rate"))
        report_fig["layout"]["yaxis6"].update(dict(title = "True Positive Rate"))

        report_fig["layout"]["yaxis7"].update(dict(title = "Features"))

        report_fig["layout"]["xaxis8"].update(dict(title = "Proportion of Sample"))
        report_fig["layout"]["yaxis8"].update(dict(title = "Lift"))             

        if show_report:
            report_fig.show()       

        #Save html report
        todays_date = datetime.now().strftime("%Y-%m-%d")
        report_fig.write_html(f"{save_path}{model.__class__.__name__}_{todays_date}.html")
        bucket = storage.Client().bucket(bucket_name)
        filename = f"{model.__class__.__name__}_{todays_date}.html"
        blob = bucket.blob(f"{save_path}{filename}")
        blob.upload_from_filename(f"{save_path}{model.__class__.__name__}_{todays_date}.html")
        print(f"{filename} sucessfully uploaded to GCS bucket!")
        
        return results_df_combined, report_fig


    def save_reports_to_gcs(models, y_true, y_pred, y_score, file_bucket, save_path, columns, show_report=False):

        # define_the_bucket
        bucket = storage.Client().bucket(file_bucket)
        date=datetime.now().strftime("%Y-%m-%d")
        model_test_set_reports = []
        model_to_report_map = {}

        # If single model passed through
        if type(models) != list:
            models = [models]

        create_folder_if_not_exists(save_path)

        # Add code to set model to a list if only 1 model passed
        for i in range(len(models)):

            print(models[i])

            # Pass data to generate plotly_report
            report_df,report_fig = plotly_model_report(model=models[i],
                                            actual=y_true,
                                            predicted=y_pred,
                                            predictions_prob=y_score,
                                            bucket_name  = file_bucket,
                                            show_report = show_report,
                                            columns = columns,
                                            save_path = save_path
                                           )

            todays_date = datetime.now().strftime("%Y-%m-%d")
            model_to_report_map[models[i].__class__.__name__ ]=report_fig

            # report_fig.write_html(f"{save_path}{todays_date}_{model.__class__.__name__}.html")
            model_test_set_reports.append(report_df)

        model_test_set_reports_concat = pd.concat(model_test_set_reports)

        return model_test_set_reports_concat, model_to_report_map

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
    
    y_true = y_test
    y_pred = (pred_prb > 0.5).astype(int)
    y_score = pred_prb 

    model_reports, model_to_report_map = save_reports_to_gcs(models = xgb_model
                                                            , y_true = y_true
                                                            , y_pred = y_pred 
                                                            , y_score = y_score
                                                            , file_bucket = file_bucket
                                                            , save_path = 'churn_12_months/reports/'
                                                            , columns = features
                                                            , show_report=False
                                                            )
    
    model_class_name = xgb_model.__class__.__name__
    final_model_report = model_to_report_map[model_class_name]    
    
    #### Save the Report and Model 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)
    
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

    