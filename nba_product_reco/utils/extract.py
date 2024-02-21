# Global import
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import pandas as pd

def extract_bq_data(bq_client, sql=None, pth_query=None) -> pd.DataFrame:
    """
    Extract bq query results as pandas dataframe
    """
    if sql is not None:
        df = bq_client.query(sql).to_dataframe()
    elif pth_query is not None:
        sql = pth_query.read_text()
        df = bq_client.query(sql).to_dataframe()
    else:
        raise ValueError('`sql` or `pth_query` should be set')  
    
    return df