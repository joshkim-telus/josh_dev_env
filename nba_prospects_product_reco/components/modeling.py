import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
# import warnings
# from pandas.errors import SettingWithCopyWarning
# warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
# warnings.simplefilter(action='ignore', category=FutureWarning)


REVENUE_BAND_MAPPING = {
    'A': 1, 'B': 2, 'C': 2, 'D': 3, 'E': 4,
    'F': 5, 'H': 6, 'N': 7, 'S': 8
}

CREDIT_CLASS_MAPPING = {
    'B': 1, 'D': 2, 'C': 3, 'X': 4, 'L': 5, 'V': 6
}

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


def _payment_method(value: str):
    """
    Extract payment methos
    """
    if pd.isnull(value): return 0
    if 'R ' in value: return 1
    if 'C' in value: return 2
    if 'D' in value: return 3
            
    return None
        
    
def process_hs_features(
    df_input: pd.DataFrame, 
    d_model_metadata: dict, 
    training_mode: bool = False,
    target_name: str = None # mandatory in training mode
) -> pd.DataFrame:
    """
    This function processes the features of a given DataFrame based on the provided model metadata. It takes the following parameters:
    
    Args:
        - df_input: A pandas DataFrame containing the input data.
        - d_model_metadata: A dictionary containing the metadata information for the model.
        - training_mode: A boolean indicating whether the function is being used for training or inference. Default is False.
        - target_name: A string indicating the name of the target variable. This parameter is mandatory in training mode.

    Returns:
        - pd.DataFrame: The processed dataframe with additional features and mapped target values.
    """

    df = df_input.copy() 
    
    # create AB_BC binary feature
    if 'cust_prov_state_cd' in df.columns:
        df['cust_prov_state_cd'] = df.apply(
            lambda row: _extract_AB_BC_flag(row['cust_prov_state_cd']), axis = 1
        )

    # create acct risk value
    if 'acct_cr_risk_txt' in df.columns:
        df['acct_cr_risk_txt'] = df.apply(
            lambda row: _extract_account_risk_value(row['acct_cr_risk_txt']), axis = 1
        )

    # create ebill value
    if 'acct_ebill_ind' in df.columns:
        df['acct_ebill_ind'] = df.apply(
            lambda row: 1 if row['acct_ebill_ind'] == 'Y' else 0, axis = 1
        )

    # extract FSA
    if 'fsa' in df.columns:
        df['fsa'] = df.apply(
            lambda row: _extract_fsa([row['winning_pstl_cd'], row['bill_pstl_cd']]), axis = 1
        )

    # extract language
    if 'cust_pref_lang_txt' in df.columns:
        df['cust_pref_lang_txt'] = df.apply(
            lambda row: 1 if row['cust_pref_lang_txt'] == 'English' else 0, axis = 1
        )

    # diff in days of ref dt
    if 'date_to_days_features' in d_model_metadata.keys():
        for f in d_model_metadata['date_to_days_features']:
            df[f['name']] = (
                pd.to_datetime(df[f['name']], errors='coerce') - pd.to_datetime(df['part_dt'], errors='coerce')
            ).dt.days
    
    # extract features name
    l_features = [
        d_f['name'] for d_f in d_model_metadata['features'] if d_f['name'] in df.columns
    ]

    df_features = df[l_features + [target_name]] if training_mode else df[l_features]
    df_features = df_features.fillna(0)

    # convert features to type
    for d_f in d_model_metadata['features']:
        if d_f['name'] in df_features.columns:
            df_features[d_f['name']] = df_features[d_f['name']].astype(d_f['type'])

    if training_mode:
        # extract target name - index mapping
        d_target_mapping = {
            d_target_info['name']: d_target_info['class_index']
            for d_target_info in d_model_metadata['target_variables']
        }
        # map target values
        df_features['target'] = df_features[target_name].map(d_target_mapping)
        df_features = df_features.drop(columns=target_name)

    else:
        l_customer_ids = d_model_metadata['customer_ids']
        df_features[l_customer_ids] = df[l_customer_ids]
        df_features = df_features[l_customer_ids + l_features]
        df_features['part_dt'] = df['part_dt']

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
    total_correct_predictions = df_stats[f'is_prediction_in_top_{n}'].sum()
    df_w_avg = pd.DataFrame({
        'n_acquisitions': [df_stats['n_acquisitions'].sum()],
        f'is_prediction_in_top_{n}': [total_correct_predictions],
        f'capture_rate_top_{n}': [round(w_avg, 2)],
        'product': [f'weighted_avg']    
    })
    df_stats = pd.concat([df_stats, df_w_avg])

    return df_stats

