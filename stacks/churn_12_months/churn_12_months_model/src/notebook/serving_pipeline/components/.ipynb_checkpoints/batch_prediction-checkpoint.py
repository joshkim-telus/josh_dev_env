from kfp.v2.dsl import (Artifact, Dataset, Input, InputPath, Model, Output, OutputPath, ClassificationMetrics,
                        Metrics, component, HTML)
@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="xgb_batch_prediction.yaml",
)
def batch_prediction(
        project_id: str,
        dataset_id: str,
        table_id: str, 
        file_bucket: str,
        save_data_path: str, 
        service_type: str,
        score_table: str,
        score_date_dash: str,
        temp_table: str,
        metrics: Output[Metrics],
        metricsc: Output[ClassificationMetrics],
        model_uri: str, 
):
    import time
    import pandas as pd
    import numpy as np
    import pickle
    from datetime import date
    from dateutil.relativedelta import relativedelta
    from google.cloud import bigquery
    from google.cloud import storage
    
    MODEL_ID = '5090'
    
    def if_tbl_exists(bq_client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            bq_client.get_table(table_ref)
            return True
        except NotFound:
            return False

    def upsert_table(project_id, dataset_id, table_id, sql, result):
        new_values = ',\n'.join(result.apply(lambda row: row_format(row), axis=1))
        new_sql = sql.format(proj_id=project_id, dataset_id=dataset_id, table_id=table_id,
                             new_values=new_values)
        bq_client = bigquery.Client(project=project_id)
        code = bq_client.query(new_sql)
        time.sleep(5)

    def row_format(row):
        values = row.values
        new_values = ""
        v = str(values[0]) if not pd.isnull(values[0]) else 'NULL'
        if 'str' in str(type(values[0])):
            new_values += f"'{v}'"
        else:
            new_values += f"{v}"

        for i in range(1, len(values)):
            v = str(values[i]) if not pd.isnull(values[i]) else 'NULL'
            if 'str' in str(type(values[i])):
                new_values += f",'{v}'"
            else:
                new_values += f",{v}"
        return '(' + new_values + ')'

    def generate_sql_file(ll):
        s = 'MERGE INTO `{proj_id}.{dataset_id}.{table_id}` a'
        s += " USING UNNEST("
        s += "[struct<"
        for i in range(len(ll) - 1):
            v = ll[i]
            s += "{} {},".format(v[0], v[1])
        s += "{} {}".format(ll[-1][0], ll[-1][1])
        s += ">{new_values}]"
        s += ") b"
        s += " ON a.ban = b.ban and a.score_date = b.score_date"
        s += " WHEN MATCHED THEN"
        s += " UPDATE SET "
        s += "a.{}=b.{},".format(ll[0][0], ll[0][0])
        for i in range(1, len(ll) - 1):
            v = ll[i]
            s += "a.{}=b.{},".format(v[0], v[0])
        s += "a.{}=b.{}".format(ll[-1][0], ll[-1][0])
        s += " WHEN NOT MATCHED THEN"
        s += " INSERT("
        for i in range(len(ll) - 1):
            v = ll[i]
            s += "{},".format(v[0])
        s += "{})".format(ll[-1][0])
        s += " VALUES("
        for i in range(len(ll) - 1):
            s += "b.{},".format(ll[i][0])
        s += "b.{}".format(ll[-1][0])
        s += ")"

        return s

    def right(s, amount):
        return s[-amount:]

    # MODEL_PATH = '{}_xgb_models/'.format(service_type)
    MODEL_PATH = right(model_uri, (len(model_uri) - 6 - len(file_bucket)))
    df_score = pd.read_csv(save_data_path, compression='gzip')
    print(f'save_data_path: {save_data_path}')
    df_score.dropna(subset=['ban'], inplace=True)
    df_score.reset_index(drop=True, inplace=True)
    print('......scoring data loaded:{}'.format(df_score.shape))
    time.sleep(10)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)
    # blobs = storage_client.list_blobs(file_bucket, prefix='{}{}_models_xgb_'.format(MODEL_PATH, service_type))
    blobs = storage_client.list_blobs(file_bucket, prefix=MODEL_PATH)

    model_lists = []
    for blob in blobs:
        model_lists.append(blob.name)

    blob = bucket.blob(model_lists[-1])
    blob_in = blob.download_as_string()
    model_dict = pickle.loads(blob_in)
    model_xgb = model_dict['model']
    features = model_dict['features']
    print('...... model loaded')
    time.sleep(10)

    ll = [('ban', 'string'), ('score_date', 'string'), ('model_id', 'string'), ('score', 'float64')]
    sql = generate_sql_file(ll)

    df_score['ban'] = df_score['ban'].astype(int)
    print('.... scoring for {} promo expiry bans base'.format(len(df_score)))

    # get full score to cave into bucket
    pred_prob = model_xgb.predict_proba(df_score[features], ntree_limit=model_xgb.best_iteration)[:, 1]
    result = pd.DataFrame(columns=['ban', 'score_date', 'model_id', 'score'])
    result['score'] = list(pred_prob)
    result['score'] = result['score'].fillna(0.0).astype('float64')
    result['ban'] = list(df_score['ban'])
    result['ban'] = result['ban'].astype('str')
    result['score_date'] = score_date_dash
    result['model_id'] = MODEL_ID

    result.to_csv('gs://{}/ucar/{}_prediction.csv.gz'.format(file_bucket, service_type), compression='gzip',
                  index=False)
        
    # define df_final
    df_final = df_score[features]
    
    # define dtype_bq_mapping
    dtype_bq_mapping = {np.dtype('int64'): 'INTEGER', 
    np.dtype('float64'):  'FLOAT', 
    np.dtype('float32'):  'FLOAT', 
    np.dtype('object'):  'STRING', 
    np.dtype('bool'):  'BOOLEAN', 
    np.dtype('datetime64[ns]'):  'DATE', 
    pd.Int64Dtype(): 'INTEGER'} 
    
    # export df_final to bigquery 
    schema_list = [] 
    for column in df_final.columns: 
        schema_list.append(bigquery.SchemaField(column, dtype_bq_mapping[df_final.dtypes[column]], mode='NULLABLE')) 
    print(schema_list) 
    
    dest_table = f'{dataset_id}.{table_id}'

    # Sending to bigquery 
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(schema=schema_list, write_disposition='WRITE_TRUNCATE') 
    job = client.load_table_from_dataframe(df_final, dest_table, job_config=job_config) 
    job.result() 
    table = client.get_table(dest_table) # Make an API request 
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)) 
    
    time.sleep(60)

    table_ref = f'{project_id}.{dataset_id}.{score_table}'
    client = bigquery.Client(project=project_id)
    table = client.get_table(table_ref)
    schema = table.schema

    ll = []
    for item in schema:
        col = item.name
        d_type = item.field_type
        if 'float' in str(d_type).lower():
            d_type = 'FLOAT64'
        ll.append((col, d_type))

        if 'integer' in str(d_type).lower():
            result[col] = result[col].fillna(0).astype(int)
        if 'float' in str(d_type).lower():
            result[col] = result[col].fillna(0.0).astype(float)
        if 'string' in str(d_type).lower():
            result[col] = result[col].fillna('').astype(str)

    table_ref = '{}.{}.{}'.format(project_id, dataset_id, temp_table)
    client = bigquery.Client(project=project_id)
    if if_tbl_exists(client, table_ref):
        client.delete_table(table_ref)

    client.create_table(table_ref)
    config = bigquery.LoadJobConfig(schema=schema)
    config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    bq_table_instance = client.load_table_from_dataframe(result, table_ref, job_config=config)
    time.sleep(5)

    drop_sql = f"""delete from `{project_id}.{dataset_id}.{score_table}` where score_date = '{score_date_dash}'"""  # .format(project_id, dataset_id, score_date_dash)
    client.query(drop_sql)
    #
    load_sql = f"""insert into `{project_id}.{dataset_id}.{score_table}`
                  select * from `{project_id}.{dataset_id}.{temp_table}`"""
    client.query(load_sql)

    