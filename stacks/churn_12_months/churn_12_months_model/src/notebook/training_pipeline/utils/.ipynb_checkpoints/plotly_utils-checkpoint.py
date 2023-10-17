import plotly.graph_objs as go
from plotly.subplots import make_subplots

telus_purple = '#4B286D'
telus_green = '#66CC00'
telus_grey = '#F4F4F7'
from google.cloud import storage
import matplotlib.pyplot as plt
from sklearn.metrics import *
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
from pycaret.classification import setup,create_model,tune_model, predict_model,get_config,compare_models,save_model,tune_model, models
import joblib
import os 
import pickle

def create_folder_if_not_exists(path):
    """
    Create a new folder based on a path if that path does not currently exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Folder created: {path}")
    else:
        print(f"Folder already exists: {path}")

def ploty_model_metrics(actual, predicted,  plot=False):
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
        x = [str(x) for x in range(actual.nunique())]
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
    report_fig.write_html(f"{save_path}{todays_date}_{model.__class__.__name__}.html")
    bucket = storage.Client().bucket(bucket_name)
    filename = f"{todays_date}_{model.__class__.__name__}.html"
    blob = bucket.blob(f"{save_path}{filename}")
    blob.upload_from_filename(f"{save_path}{todays_date}_{model.__class__.__name__}.html")
    print(f"{filename} sucessfully uploaded to GCS bucket!")
    # Return dataframe with metrics
    return results_df_combined,report_fig

    

def evaluate_and_save_models(models,bucket_name, save_path, test_df, actual_label_str, columns, save_columns=False, show_report=False):
    # define_the_bucket
    bucket = storage.Client().bucket(bucket_name)
    date=datetime.now().strftime("%Y-%m-%d")
    model_test_set_reports = []
    model_to_report_map = {}
    
    # If single model passed through
    if type(models) != list:
        models = [models]
        
    create_folder_if_not_exists(save_path)
    
    if save_columns:
        with open(f"{save_path}{date}_columns.pkl" , "wb") as f:
            pickle.dump(columns, f)
        print(f"Columns saved as {save_path}{date}_columns.pkl !")
        filename = f"{date}_columns.pkl"
        blob = bucket.blob(f"{save_path}{filename}")
        blob.upload_from_filename(f"{save_path}{date}_columns.pkl")
        print(f"{save_path}/{date}_columns.pkl sucessfully uploaded to GCS bucket!")
    
     # Add code to set model to a list if only 1 model passed
    for i in range(len(models)):
        print(models[i])
        # Save model
        
        # Add code to create new folder if it does not exist
        model_file_name = '{save_path}{model_type}_{date}'.format(save_path = save_path,                                                                     model_type=models[i].__class__.__name__,                                                                    date=datetime.now().strftime("%Y-%m-%d"))
        save_model(models[i],model_file_name )
        filename = '{model_type}_{date}.pkl'.format(model_type=models[i].__class__.__name__,                                        date=datetime.now().strftime("%Y-%m-%d"))
        blob = bucket.blob(f"{save_path}{filename}")
        blob.upload_from_filename(f"{model_file_name}.pkl")
        print(f"{filename} sucessfully uploaded to GCS bucket!")
        # joblib.dump(models[i], '{save_path}{model_type}_{date}.joblib'.format(
        #                                                                     save_path = save_path,
        #                                                                     model_type=models[i].__class__.__name__,
        #                                                                     date=datetime.now().strftime("%Y-%m-%d")))

        # Get predictions on test set for model
        predictions = predict_model(models[i], data=test_df)
        # Normalize prediction probabilities 
        predictions['Score_Normalized']=predictions['Score']
        predictions.loc[predictions['Label'] == 0,'Score_Normalized'] = 1 - predictions['Score']
        predictions_prob = predictions["Score_Normalized"].astype(float)

        actual = predictions[actual_label_str].astype(int)
        predicted = predictions["Label"].astype(int)
        
        # Pass data to generate plotly_report
        report_df,report_fig = plotly_model_report(model=models[i],
                                        actual=actual,
                                        predicted=predicted,
                                        predictions_prob=predictions_prob,
                                        bucket_name  = bucket_name  ,
                                        show_report = show_report,
                                        columns = columns,
                                        save_path = save_path      
                                       )
        todays_date = datetime.now().strftime("%Y-%m-%d")
        model_to_report_map[models[i].__class__.__name__ ]=report_fig

    # report_fig.write_html(f"{save_path}{todays_date}_{model.__class__.__name__}.html")
        model_test_set_reports.append(report_df)
        
    model_test_set_reports_concat = pd.concat(model_test_set_reports)
    model_test_set_reports_concat.to_csv(f"{save_path}{date}_model_reports.csv", index=False)
    
    filename = f"{date}_model_reports.csv"
    blob = bucket.blob(f"{save_path}{filename}")
    blob.upload_from_filename(f"{save_path}{date}_model_reports.csv")
    print(f"{filename} sucessfully uploaded to GCS bucket!")
    return model_test_set_reports_concat,model_to_report_map