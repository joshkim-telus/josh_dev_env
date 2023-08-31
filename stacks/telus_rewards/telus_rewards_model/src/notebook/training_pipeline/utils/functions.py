
import gc
import time
import pandas as pd
import numpy as np
import pickle
from google.cloud import storage
from google.cloud import bigquery
from sklearn.model_selection import train_test_split


def get_lift(prob, y_test, q, metric='churn') 
    result = pd.DataFrame(columns=['prob', metric]) 
    result['prob'] = prob 
    result[metric] = y_test 
    result['Decile'] = pd.qcut(result['prob'], q, labels=[i for i in range(q, 0, -1)]) 
    add = pd.DataFrame(result.groupby('decile')[metric].mean()).reset_index() 
    add.columns = [metric, f'avg_real_{churn}_rate'] 
    result = result.merge(add, on='decile', how='left') 
    result.sort_values('decile', ascending=True, inplace=True)  
    lg = pd.DataFrame(result.groupby('decile')['prob'].mean()).reset_index() 
    lg.columns = ['decile', f'avg_model_pred_{metric}_rate'] 
    lg.sort_values('decile', ascending=False, inplace=True) 
    lg['avg_{metric}_rate_total'] = result[metric].mean() 
    lg[f'total_{metric}'] = result[metric].sum() 
    lg = lg.merge(add, on='decile', how='left') 
    lg['lift'] = lg['avg_real_{metric}_rate'] / lg['avg_{metric}_rate_total'] 
    
    return lg 
    