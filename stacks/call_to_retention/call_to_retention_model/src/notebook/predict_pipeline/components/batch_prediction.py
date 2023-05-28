from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/wb-platform/pipelines/kubeflow-pycaret:latest",
    output_component_file="xgb_batch_prediction.yaml",
)
def batch_prediction(
        project_id: str,
        dataset_id: str,
        file_bucket: str,
        service_type: str,
        score_table: str,
        score_date_dash: str,
        metrics: Output[Metrics],
        metricsc: Output[ClassificationMetrics],
):
    import time
    import pandas as pd
    import numpy as np
    import pickle
    from datetime import date
    from dateutil.relativedelta import relativedelta
    from google.cloud import bigquery
    from google.cloud import storage
    
    MODEL_ID = '5060'
    
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

    MODEL_PATH = '{}_xgb_models/'.format(service_type)
    df_score = pd.read_csv('gs://{}/{}_score.csv.gz'.format(file_bucket, service_type), compression='gzip')
    df_score.dropna(subset=['ban'], inplace=True)
    df_score.reset_index(drop=True, inplace=True)
    print('......scoring data loaded:{}'.format(df_score.shape))
    time.sleep(10)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file_bucket)
    blobs = storage_client.list_blobs(file_bucket, prefix='{}{}_models_xgb_'.format(MODEL_PATH, service_type))

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
    print('.... scoring for {} tos cross sell bans base'.format(len(df_score)))

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
    time.sleep(60)

    batch_size = 1000
    n_batchs = int(df_score.shape[0] / batch_size) + 1
    print('...... will upsert {} batches'.format(n_batchs))

    # start batch prediction
    all_scores = np.array(result['score'].values)
    for i in range(n_batchs):
    
        s, e = i * batch_size, (i + 1) * batch_size
        if e >= df_score.shape[0]:
            e = df_score.shape[0]

        df_temp = df_score.iloc[s:e]
        pred_prob = all_scores[s:e]
        batch_result = pd.DataFrame(columns=['ban', 'score_date', 'model_id', 'score'])
        batch_result['score'] = list(pred_prob)
        batch_result['score'] = batch_result['score'].fillna(0.0).astype('float64')
        batch_result['ban'] = list(df_temp['ban'])
        batch_result['ban'] = batch_result['ban'].astype('str')
        batch_result['score_date'] = score_date_dash
        batch_result['model_id'] = MODEL_ID

        upsert_table(project_id,
                     dataset_id,
                     score_table,
                     sql,
                     batch_result,
                     )
        if i % 20 == 0:
            print('predict for batch {} done'.format(i), end=' ')

    time.sleep(120)
    
    