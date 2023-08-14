
from google.cloud import bigquery
import pandas as pd 
import numpy as np 

dtype_bq_mapping = { 
    np.dtype('int64') = 'INTEGER', 
    np.dtype('float64') = 'FLOAT', 
    np.dtype('float32') = 'FLOAT', 
    np.dtype('object') = 'STRING', 
    np.dtype('bool') = 'BOOLEAN', 
    np.dtype('datetime64[ns]') = 'DATE', 
    pd.Int64Dtype(): 'INTEGER' 
} 

def export_dataframe_to_bq(df, client, table_id='', schema_list=[], generate_schema=True, write='overwrite'): 
    
    """
    inputs: 
    df: dataframe that you want to export to BQ 
    table_id: string with dataset and table name. i.e 'ttv_churn_dataset.bq_tv_churn_score' 
    schema_list: List of the SchemaFields if provided, otherwise the function can generate it for you 
    generate_schema: True (if True, the function will provide schema for you) or False (provide your own list) 
    write: if 'overwrite', the function will overwrite the existing table
           if 'append', it will append to the existing table 
    """
    
    if write == 'overwrite': 
        write_type = 'WRITE_TRUNCATE' 
    else: 
        write_type = 'WRITE_APPEND' 
        
    if ((generate_schema == False) & (len(schema_list) == 0)): 
        print('Error: provide schema list, otherwise set generate_schema parameter to True') 
        return 
    
    if table_id == '': 
        print('Error: Provide table_id') 
    else: 
        if generate_schema == True: 
            schema_list = [] 
            for column in df.columns: 
                schema_list.append(bigquery.SchemaField(column, dtype_bq_mapping[df.dtypes[column]], mode='NULLABLE')) 
        print(schema_list) 
        
        # Sending to bigquery 
        
        job_config = bigquery.LoadJobConfig(schema=schema_list, write_disposition=write_type) 
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config) 
        job.result() 
        table = client.get_table(table_id) # Make an API request 
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)) 
        

def sql_to_dataframe(sql, token: str): 
    from google.cloud import bigquery
    from google.oauth2 import credentials

    CREDENTIALS = google.oauth2.credentials.Credentials(token) # get credentials from token
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    df_target = client.query(sql).to_dataframe()

    return df_target 
    
    