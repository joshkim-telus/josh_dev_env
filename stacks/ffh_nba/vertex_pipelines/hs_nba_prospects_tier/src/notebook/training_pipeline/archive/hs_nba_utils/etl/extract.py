import pandas as pd


def extract_bq_data(bq_client, sql=None, pth_query=None) -> pd.DataFrame:
    """
    Parameters:

    - bq_client: The BigQuery client object used to connect to the BigQuery service.
    - sql (optional): The SQL query to be executed in BigQuery. If provided, the function will execute the query and return the result as a DataFrame.
    - pth_query (optional): The path to a file containing the SQL query. If provided, the function will read the query from the file, execute it in BigQuery, and return the result as a DataFrame.
    
    Returns:

    - df: The extracted data from BigQuery, returned as a pandas DataFrame.
    
    Raises:

        - ValueError: If neither sql nor pth_query is provided, an error will be raised indicating that at least one of them should be set.
    """
    if sql is not None:
        df = bq_client.query(sql).to_dataframe()
    elif pth_query is not None:
        sql = pth_query.read_text()
        df = bq_client.query(sql).to_dataframe()
    else:
        raise ValueError('`sql` or `pth_query` should be set')  
    
    return df
