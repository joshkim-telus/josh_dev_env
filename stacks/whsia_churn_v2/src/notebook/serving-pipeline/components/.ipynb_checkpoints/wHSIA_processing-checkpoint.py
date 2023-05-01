from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/vertex_pipelines/kfp-preprocess-slim:latest",
    output_component_file="whsia_churn_process.yaml"
)
def wHSIA_processing(view_name: str,
                   project_id: str,
                   dataset_id: str,
                   table_id: str,
                   query_date: str,
                   file_bucket: str,
                   ):

    from google.cloud import bigquery
    import pandas as pd
    import time

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
        s += " ON a.score_date = b.score_date and a.ban = b.ban"
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

    MODEL_ID = '5070'
    bq_client = bigquery.Client(project=project_id)
    wHSIA_data = "{project_id}.{dataset_id}.{view_name}".format(project_id=project_id, dataset_id=dataset_id, view_name=view_name)

    wHSIA = '''SELECT * FROM `{wHSIA_data}`'''.format(wHSIA_data=wHSIA_data)
    df_wHSIA = bq_client.query(wHSIA).to_dataframe()
    cols = ['ban', 'score_date', 'model_id', 'score']
    df_wHSIA = df_wHSIA[cols]
    print('......wHSIA table generated with {} samples'.format(df_wHSIA.shape[0]))

    # save current results to bucket for UCAR inputs
    file_name = 'gs://{}/ucar/wHSIA_churn.csv.gz'.format(file_bucket)
    results = df_wHSIA.copy()
    results.to_csv(file_name, compression='gzip', index=False)

    ll = [('ban', 'string'), ('score_date', 'string'), ('model_id', 'string'), ('score', 'float64')]
    sql = generate_sql_file(ll)

    batch_size = 2000
    n_batchs = int(df_wHSIA.shape[0] / batch_size) + 1
    print('...... will upsert {} batches'.format(n_batchs))
    df_wHSIA['ban'] = df_wHSIA['ban'].astype(str)
    df_wHSIA['model_id'] = df_wHSIA['model_id'].astype(str)
    df_wHSIA['score_date'] = df_wHSIA['score_date'].astype(str)

    for i in range(n_batchs):
        s, e = i * batch_size, (i + 1) * batch_size
        if e >= df_wHSIA.shape[0]:
            e = df_wHSIA.shape[0]

        df_temp = df_wHSIA.iloc[s:e]

        upsert_table(project_id,
                     dataset_id,
                     table_id,
                     sql,
                     df_temp,
                     )
        if i % 20 == 0:
            print('predict for batch {} done'.format(i), end=' ')

    time.sleep(120)

