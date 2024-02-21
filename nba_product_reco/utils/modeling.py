import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
import warnings
# from pandas.errors import SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


def _extract_AB_BC_flag(province: str):
    """
    Generate AB BC flag 
    """
    if pd.isnull(province): return -1
    if province in ('AB', 'BC'): return 1
    
    return 0


def _extract_account_risk_value(value: str):
    """ 
    Generate account risk values
    """
    if pd.isnull(value): return 0
    if 'Low ' in value: return 1
    if 'Medium' in value: return 2
    if 'High' in value: return 3
    
    return 0
    

def _extract_fsa(pstl_cd_list: List[str]):
    """
    Extract FSA from postal code
    """
    # return first fsa that check conditions
    for pstl_cd in pstl_cd_list:
        if pd.notnull(pstl_cd) and \
            isinstance(pstl_cd, str) and \
                len(pstl_cd) == 6:

            return pstl_cd[:3]
            
    return None


def process_features(
    df: pd.DataFrame, 
    d_features_metadata: dict, 
    target_name: str,
    d_target_mapping: dict
) -> pd.DataFrame:
    """
    Process features in the given DataFrame and return the modified DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing the features to be processed.

    Returns:
    pd.DataFrame: The modified DataFrame with processed features.
    """
    
    
    # create AB_BC binary feature
    df['AB_BC'] = df.apply(
        lambda row: _extract_AB_BC_flag(row['cust_prov_state_cd']), axis = 1
    )
    
    # create acct risk value
    df['acct_cr_risk_value'] = df.apply(
        lambda row: _extract_account_risk_value(row['acct_cr_risk_txt']), axis = 1
    )
    
    # create ebill value
    df['acct_ebill_value'] = df.apply(
        lambda row: 1 if row['acct_ebill_ind'] == 'Y' else 0, axis = 1
    )

    # # extract FSA
    # df['fsa'] = df.apply(
    #     lambda row: _extract_fsa([row['winning_pstl_cd'], row['bill_pstl_cd']]), axis = 1
    # )
        
    # extract features name
    l_features = [d_f['name'] for d_f in d_features_metadata['features']]
    df_features = df[l_features + [target_name]]
    df_features = df_features.fillna(0)
    
#     # convert features to type
#     for d_f in d_features_metadata['features']:
#         df_features[d_f['name']] = df_features[d_f['name']].astype(d_f['type'])

    # map arget values
    df_features['target'] = df_features[target_name].map(d_target_mapping)
    df_features = df_features.drop(columns=target_name)
    
    return df_features


def extract_features_importance(features_name, features_score):
    return {
        feature_name: str(feature_score)
        for feature_name, feature_score in zip(features_name, features_score)
    }


def extract_stats(
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
    df_w_avg = pd.DataFrame({
        'n_acquisitions': [df_stats['n_acquisitions'].sum()],
        f'is_prediction_in_top_{n}': [df_stats[f'is_prediction_in_top_{n}'].sum()],
        f'capture_rate_top_{n}': [round(w_avg, 2)],
        'product': [f'weighted_avg']    
    })
    df_stats = pd.concat([df_stats, df_w_avg])
    
    return df_stats

    
# def extract_stats(
#     n: int, 
#     predictions_ranked: np.array, 
#     true_values: np.array,   
# ):
#     """
#     Extracts statistics and metrics for evaluating predictions ranked by their probability scores.

#     Parameters:
#     n (int): The number of predictions to consider in the top N.
#     predictions_ranked (np.array): An array of ranked predictions.
#     true_values (np.array): An array of true values corresponding to the predictions.

#     Returns:
#     pd.DataFrame: A DataFrame containing statistics and metrics for evaluating the predictions.

#     """

#     # total predictions - count total predictions in top n by product
#     d_total_predictions_in_top_n_by_label = {}
#     for label in true_values.unique():
#         d_total_predictions_in_top_n_by_label[label] = 0
#         for prediction in predictions_ranked:
#             if label in prediction[:n]:
#                 d_total_predictions_in_top_n_by_label[label] += 1
                
#     d_total_predictions_by_label = pd.DataFrame.from_dict(
#         d_total_predictions_in_top_n_by_label, orient='index', columns=[f'total_predictions_in_top_{n}']
#     )

#     # true_predctions - check if prediction is in top n
#     l_results = [
#         1 if true_value in prediction[:n] else 0
#         for prediction, true_value in zip(predictions_ranked, true_values)
#     ]
    
#     # build results dataframe
#     df_results = pd.DataFrame(true_values)
#     df_results = df_results.rename(columns = {df_results.columns[0]: 'label'})
#     df_results[f'is_prediction_in_top_{n}'] = l_results

#     # aggregate by label
#     df_stats = df_results.groupby('label').agg({
#         'label': 'count',
#         f'is_prediction_in_top_{n}': 'sum',
#     }).rename(
#         columns = {
#             'label': 'n_acquisitions',
#             f'is_prediction_in_top_{n}': f'true_predictions_in_top_{n}'
#         }
#     )
#     df_stats = df_stats.merge(d_total_predictions_by_label, left_index=True, right_index=True)

#     # calculate true and false positive sand negative
#     df_stats['total_universe'] = len(true_values)
#     df_stats['total_label_positives'] = df_stats['n_acquisitions']
#     df_stats['total_label_negatives'] = df_stats['total_universe'] - df_stats['total_label_positives']
    
#     df_stats['true_positives'] = df_stats[f'true_predictions_in_top_{n}']
#     df_stats['false_positives'] = df_stats[f'total_predictions_in_top_{n}'] - df_stats['true_positives']
#     df_stats['false_negatives'] = df_stats['total_label_positives'] - df_stats['true_positives']
#     df_stats['true_negatives'] = df_stats['total_universe'] - df_stats['true_positives'] - df_stats['false_positives'] - df_stats['false_negatives']
    
#     # accuracy, precision, recall
#     accuracy = (
#         df_stats['true_positives'] + df_stats['true_negatives']
#     ) / (
#         df_stats['true_positives'] + df_stats['false_positives'] + df_stats['true_negatives'] + df_stats['false_negatives']
#     )

#     # precision
#     precision = (
#         df_stats['true_positives']
#     ) / (
#         df_stats['true_positives'] + df_stats['false_positives']
#     )

#     # recall
#     recall = (
#         df_stats['true_positives']
#     ) / (
#         df_stats['true_positives'] + df_stats['false_negatives']
#     )

#     # insert values to df
#     df_stats['accuracy'] = round(accuracy * 100, 2)
#     df_stats['precision'] = round(precision * 100, 2)
#     df_stats['recall'] = round(recall * 100, 2)

#     return df_stats

