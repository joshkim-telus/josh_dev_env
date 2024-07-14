# import global modules

import os
import gc
import time
import pandas as pd
import numpy as np

from typing import Any, Dict
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

from typing import List, Dict, Tuple, Optional

def evaluate(df_result: pd.DataFrame,
             file_bucket: str,
             stack_name: str,
             pipeline_path: str,
             pipeline_type: str, 
             service_type: str,
             model_type: str,
             d_model_config: dict,
             stats_file_name: str,
             model: Any,
             y_true,
             y_pred,
             y_score
             ):
    """
    This function evaluates the model based on the predictions it made on validation set. It takes the following parameters:
    
    Args:
        - df_result: Returned dataset from train() function
        - file_bucket: A GCS Bucket where training dataset is saved.
        - stack_name: Model stack name
        - pipeline_path: A GCS Pipeline path where related files/artifacts will be saved. 
        - service_type: Service type name
        - model_type: 'churn', 'cross-sell', or 'upsell'
        - d_model_config: A dictionary containing the metadata information for the model.
        - stats_file_name: The name of the file that contains model stats including capture rate. 
        - model: Trained model 
        - y_true: np array of actual y values
        - y_pred: np array of predicted y values (1 or 0)
        - y_score: np array of predicted probabilities (between 0 and 1)

    Returns:
        - pd.DataFrame: The processed dataframe with model stats (lift).
    """
    
    #### Import Libraries ####

    telus_purple = '#4B286D'
    telus_green = '#66CC00'
    telus_grey = '#F4F4F7'
    
    import plotly.graph_objs as go
    import plotly.express as px

    from plotly.subplots import make_subplots
    from sklearn.preprocessing import normalize

    from pycaret.classification import setup,create_model,tune_model, predict_model,get_config,compare_models,save_model,tune_model, models
    from sklearn.metrics import accuracy_score, roc_auc_score, precision_recall_curve, mean_squared_error, f1_score, precision_score, recall_score, confusion_matrix, roc_curve, classification_report

    def get_lift(prob, y_test, q):
        result = pd.DataFrame(columns=['Prob', model_type])
        result['Prob'] = prob
        result[model_type] = y_test
        # result['Decile'] = pd.qcut(1-result['Prob'], 10, labels = False)
        result['Decile'] = pd.qcut(result['Prob'], q, labels=[i for i in range(q, 0, -1)])
        add = pd.DataFrame(result.groupby('Decile')[model_type].mean()).reset_index()
        add.columns = ['Decile', f'avg_real_{model_type}_rate']
        result = result.merge(add, on='Decile', how='left')
        result.sort_values('Decile', ascending=True, inplace=True)
        lg = pd.DataFrame(result.groupby('Decile')['Prob'].mean()).reset_index()
        lg.columns = ['Decile', f'avg_model_pred_{model_type}_rate']
        lg.sort_values('Decile', ascending=False, inplace=True)
        lg[f'avg_{model_type}_rate_total'] = result[model_type].mean()
        lg[f'total_{model_type}'] = result[model_type].sum()
        lg = lg.merge(add, on='Decile', how='left')
        lg['lift'] = lg[f'avg_real_{model_type}_rate'] / lg[f'avg_{model_type}_rate_total']

        return lg
    
    #########################
    
    # get lift
    lg = get_lift(y_score, y_true, 10)
    
    ##########################

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
    
    # get lift
    lg = get_lift(y_score, y_true, 10)
    
    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lg.to_csv(f'gs://{file_bucket}/{pipeline_type}_eval/{stack_name}_lift_{create_time}.csv', index=False)
    
    #set up features (list)
    features = [d_f['name'] for d_f in d_model_config['features']]
    
    model_reports, model_to_report_map = save_reports_to_gcs(models = model
                                                            , y_true = y_true
                                                            , y_pred = y_pred
                                                            , y_score = y_score
                                                            , file_bucket = file_bucket
                                                            , save_path = f'{stack_name}/reports/'
                                                            , columns = features
                                                            , show_report=False
                                                            )
    
    model_class_name = model.__class__.__name__
    final_model_report = model_to_report_map[model_class_name]
    
    return lg
    
    