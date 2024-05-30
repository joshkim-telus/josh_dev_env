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

def evaluate(df_result: pd.DataFrame, 
             file_bucket: str, 
             stack_name: str, 
             pipeline_path: str,
             service_type: str, 
             model_type: str, 
             d_model_config: dict, 
             stats_file_name: str
             ):
    """
    This function evaluates the prospects NBA model based on the predictions it made on validation set. It takes the following parameters:
    
    Args:
        - df_result: Returned dataset from train() function
        - file_bucket: A GCS Bucket where training dataset is saved.
        - stack_name: Model stack name
        - pipeline_path: A GCS Pipeline path where related files/artifacts will be saved. 
        - service_type: Service type name
        - model_type: 'acquisition' or 'tier'
        - d_model_config: A dictionary containing the metadata information for the model.
        - stats_file_name: The name of the file that contains model stats including capture rate. 

    Returns:
        - pd.DataFrame: The processed dataframe with model stats.
    """

    def extract_stats(
        file_bucket: str, 
        stack_name: str, 
        service_type: str, 
        stats_file_name: str, 
        n: int, 
        predictions_ranked: np.array, 
        true_values: np.array,
        d_target_mapping: dict
    ):
        """
        Extracts statistics and metrics for evaluating predictions ranked by their probability scores.

        Parameters:
        n (int): The number of predictions to consider in the top N.
        predictions_ranked (np.array): An array of ranked predictions.
        true_values (np.array): An array of true values corresponding to the predictions.

        Returns:
        pd.DataFrame: A DataFrame containing statistics and metrics for evaluating the predictions.
        """

        # true_predctions - check if prediction is in top n
        l_results = [
            1 if true_value in prediction[:n] else 0
            for prediction, true_value in zip(predictions_ranked, true_values)
        ]

        # build results dataframe
        df_results = pd.DataFrame(true_values)
        df_results = df_results.rename(columns = {df_results.columns[0]: 'label'})
        df_results[f'is_prediction_in_top_{n}'] = l_results

        # aggregate by label
        df_stats = df_results.groupby('label').agg({
            'label': 'count',
            f'is_prediction_in_top_{n}': 'sum'
        }).rename(
            columns = {
                'label': 'n_acquisitions'
            }
        )

        # capture rate
        capture_rate = df_stats[f'is_prediction_in_top_{n}'] / df_stats['n_acquisitions']
        df_stats[f'capture_rate_top_{n}'] = round(capture_rate * 100, 2)

        # add product names
        df_stats['product'] = ''
        for name, idx in d_target_mapping.items():
            df_stats.at[idx, 'product'] = name

        # calculate the weighted average and append to df
        w_avg = (df_stats[f'capture_rate_top_{n}'] * df_stats['n_acquisitions']).sum() / df_stats['n_acquisitions'].sum()
        total_correct_predictions = df_stats[f'is_prediction_in_top_{n}'].sum()
        df_w_avg = pd.DataFrame({
            'n_acquisitions': [df_stats['n_acquisitions'].sum()],
            f'is_prediction_in_top_{n}': [total_correct_predictions],
            f'capture_rate_top_{n}': [round(w_avg, 2)],
            'product': [f'weighted_avg']    
        })
        df_stats = pd.concat([df_stats, df_w_avg])

        df_stats.to_csv(f'gs://{file_bucket}/{stack_name}/{pipeline_path}/{stats_file_name}', index=False)
        
        return df_stats
    
    # extract target name - index mapping
    d_target_mapping = {
        d_target_info['name']: d_target_info['class_index']
        for d_target_info in d_model_config['target_variables'][model_type]
    }
    
    # creta list with same order of label indexes
    l_pred_ordered = [label for label in d_target_mapping.keys()]
    probabilities =  df_result[l_pred_ordered].to_numpy()
    results_ranked = np.argsort(-probabilities, axis=1)
    df_stats = extract_stats(file_bucket, stack_name, service_type, stats_file_name, 3, results_ranked, df_result['target'], d_target_mapping)
    
    return df_stats
    