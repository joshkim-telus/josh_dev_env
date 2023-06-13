

from google.cloud import bigquery
import pandas as pd
import numpy as np 
from google.cloud import storage

dtype_bq_mapping = {
    np.dtype('int64') : "INTEGER",
    np.dtype('float64') : "FLOAT",
    np.dtype('float32') : "FLOAT",
    np.dtype('object') : "STRING",
    np.dtype('bool') : "BOOLEAN",
    np.dtype('datetime64[ns]') : "DATE",
    pd.Int64Dtype() : "INTEGER"
    
}


def download_data_from_gcs(project_id, bucket_name, gcs_path, local_path):
    
    """
    
    Example:
    Example Uploading to BI-Layer GCS Bucket:
    PROJECT_ID = 'bi-srv-aaaie-pr-c0c268' 
    BUCKET_NAME = 'customer_personas_vertex_ai_pipelines'
    GCS_PATH = 'speed_tiers_models/'
    QUANTILE_TRANSFORMER_LOCAL_PATH = 'quantile_transformer.joblib'

    download_data(
                  project_id = PROJECT_ID,
                  bucket_name = BUCKET_NAME,
                  gcs_path =  GCS_PATH + QUANTILE_TRANSFORMER_LOCAL_PATH,
                  local_path = QUANTILE_TRANSFORMER_LOCAL_PATH)
    quantile_transformer = joblib.load(QUANTILE_TRANSFORMER_LOCAL_PATH)

    """
    

    bucket = storage.Client(project=project_id).bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(local_path)



def upload_data_to_gcs(project_id, bucket_name, gcs_path, local_path):
    
    """

    Example Uploading to BI-Layer GCS Bucket:
    PROJECT_ID = 'bi-srv-aaaie-pr-c0c268' 
    BUCKET_NAME = 'customer_personas_vertex_ai_pipelines'
    GCS_PATH = 'speed_tiers_models/'
    QUANTILE_TRANSFORMER_LOCAL_PATH = 'quantile_transformer.joblib'

    joblib.dump(quantile_transformer, QUANTILE_TRANSFORMER_LOCAL_PATH)

    upload_data(
                project_id = PROJECT_ID,
                bucket_name = BUCKET_NAME,
                gcs_path =  GCS_PATH + QUANTILE_TRANSFORMER_LOCAL_PATH,
                local_path = QUANTILE_TRANSFORMER_LOCAL_PATH)

    """ 

    bucket = storage.Client(project=project_id).bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

def export_dataframe_to_bq(df, client, table_id='', schema_list=[], generate_schema=True, write='overwrite'):
    """
    Inputs:
    df: dataframe that you want to export to BQ
    table_id: string with dataset and table name. Ie: 'customer_personas_reports.firefly_campaign_output' 
    schema_list: List of the SchemaFields if provided, otherwise the function can generate it for you.
    generate_schema: True (Function will generate schema for you). False: Provide own schema list
    
    Ie. table_id = project_id.dataset_id.table_name
    write = 'overwrite' will overwrite the existing table in BQ
    """
    if write == 'overwrite':
        write_type = 'WRITE_TRUNCATE'
    else:
        write_type = 'WRITE_APPEND'
    
    if ((generate_schema == False) & (len(schema_list) == 0)):
        print('Error: Provide Schema List, otherwise set generate_schema to True')
        return 
    if table_id=='':
        print('Error: Provide table_id')
    else:
        if generate_schema==True:
            schema_list=[]
            for column in df.columns:
                schema_list.append(bigquery.SchemaField(column, dtype_bq_mapping[df.dtypes[column]], mode='NULLABLE'))
        print(schema_list)
        
        #Sending to bigquery

        job_config = bigquery.LoadJobConfig(schema=schema_list, write_disposition=write_type)
        job=client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
            

def download_data(bucket_name, gcs_path, local_path):
    """
    Function to download file from GCS Bucket to local VM
    
    Example to download a file from 'customer_personas_vertex_ai_pipelines/modules/hello.py' to local vm
    File will be saved as hello.py
    
    bucket_name = 'customer_personas_vertex_ai_pipelines'
    gcs_path = 'modules/hello.py'
    local_path = 'hello.py'
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(local_path)


def upload_data(bucket_name, gcs_path, local_path):
    """
    Function to upload files from local VM to GCS Bucket
    Example to upload a file named hello.py from local vm to 'customer_personas_vertex_ai_pipelines/modules/' as hello.py

    bucket_name = 'customer_personas_vertex_ai_pipelines'
    gcs_path = 'modules/hello.py'
    local_path = 'hello.py'
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)