from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/kfp-pycaret-slim:latest",
    output_component_file="xgb_postprocess.yaml",
)
def postprocess(project_id: str,
                dataset_id: str,
                ucar_score_table: str,
                temp_table: str,
                file_bucket: str,
                model_id: str,
                score_date_dash: str, 
                pipeline_type: str, 
                score_file_name: str, 
                token: str
                ):
    
    # Import global modules
    import sys
    import os
    from pathlib import Path
    import time
    from datetime import date
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import pandas as pd
    from google.cloud import bigquery
    
    def if_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False
        
    # for prod
    pth_project = Path(os.getcwd())
    pth_model_config = pth_project / 'model_config.yaml'
    pth_queries = pth_project / 'queries'
    sys.path.insert(0, pth_project.as_posix())
    
    #### For wb
    import google.oauth2.credentials
    CREDENTIALS = google.oauth2.credentials.Credentials(token)
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)

    # #### For prod 
    # client = bigquery.Client(project=project_id)
    
    file_name = f'gs://{file_bucket}/{pipeline_type}/{score_file_name}'
    df_orig = pd.read_csv(file_name, index_col=False)
    df_orig.dropna(subset=['ban'], inplace=True)
    df_orig.reset_index(drop=True, inplace=True)
    df_orig['scoring_date'] = df_orig['score_date']
    df_orig.ban = df_orig.ban.astype(int)
    df_orig = df_orig.rename(columns={'ban': 'bus_bacct_num', 'score': 'score_num'})
    df_orig.score_num = df_orig.score_num.astype(float)
    df_orig['decile_grp_num'] = pd.qcut(df_orig['score_num'], q=10, labels=[i for i in range(10, 0, -1)])
    df_orig.decile_grp_num = df_orig.decile_grp_num.astype(int)
    df_orig['percentile_pct'] = (1 - df_orig.score_num.rank(pct=True))*100
    df_orig['percentile_pct'] = df_orig['percentile_pct'].apply(round, 0).astype(int)
    df_orig['predict_model_nm'] = 'SHS CHURN Model - DIVG'
    df_orig['model_type_cd'] = 'FFH'
    df_orig['subscriber_no'] = ""
    df_orig['prod_instnc_resrc_str'] = ""
    df_orig['service_instnc_id'] = ""
    df_orig['segment_nm'] = ""
    df_orig['segment_id'] = ""
    df_orig['classn_nm'] = ""
    df_orig['predict_model_id'] = model_id
    df_orig.drop(columns=['model_id', 'score_date'], axis=1, inplace=True)

    get_cust_id = """
    WITH bq_snpsht_max_date AS(
    SELECT PARSE_DATE('%Y%m%d', MAX(partition_id)) AS max_date
        FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.INFORMATION_SCHEMA.PARTITIONS` 
    WHERE table_name = 'bq_prod_instnc_snpsht' 
        AND partition_id <> '__NULL__'
    ),
    -- BANs can have multiple Cust ID. Create rank by product type and status, prioritizing ban/cust id with active FFH products
    rank_prod_type AS (
    SELECT DISTINCT
        bacct_bus_bacct_num,
        consldt_cust_bus_cust_id AS cust_id,
        CASE WHEN pi_prod_instnc_resrc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') AND pi_prod_instnc_stat_cd = 'A' THEN 1
                WHEN pi_prod_instnc_resrc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') THEN 2
                WHEN pi_prod_instnc_stat_cd = 'A' THEN 3
                ELSE 4
                END AS prod_rank
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
    CROSS JOIN bq_snpsht_max_date
    WHERE CAST(prod_instnc_ts AS DATE)=bq_snpsht_max_date.max_date
    AND bus_prod_instnc_src_id = 1001
    ),
    --Rank Cust ID
    rank_cust_id AS (
    SELECT DISTINCT
        bacct_bus_bacct_num,
        cust_id,
        RANK() OVER(PARTITION BY bacct_bus_bacct_num
                        ORDER BY prod_rank,
                                    cust_id) AS cust_id_rank               
    FROM rank_prod_type
    )
    --Select best cust id
    SELECT bacct_bus_bacct_num,
        cust_id
    FROM rank_cust_id
    WHERE cust_id_rank = 1
    """

    df_cust = client.query(get_cust_id).to_dataframe()
    df_final = df_orig.set_index('bus_bacct_num').join(df_cust.set_index('bacct_bus_bacct_num')).reset_index()
    df_final = df_final.rename(columns={'index': 'bus_bacct_num', 'cust_bus_cust_id': 'cust_id'})
    df_final = df_final.sort_values(by=['score_num'], ascending=False)
    
    file_name_only = score_file_name.rsplit('.', 1)[0]
    save_file_name = f'gs://{file_bucket}/{pipeline_type}/{file_name_only}_final.csv'
    df_final.to_csv(save_file_name, index=False)
    
    todays_date = datetime.now().strftime("%Y-%m-%d")
    backup_file_name = f'gs://{file_bucket}/{pipeline_type}/{file_name_only}_final_{todays_date}.csv'
    df_final.to_csv(backup_file_name, index=False)
    
    time.sleep(120)

    # ------------------- directly write into UCAR score tables ----------------- #
    df_final['segment_fr_nm'] = ''
    df_final['create_ts'] = pd.Timestamp.now()
    df_final['create_dt'] = pd.Timestamp('today')
    df_final['month_used_dt'] = pd.Timestamp('today')
    df_final['scoring_ts'] = pd.Timestamp(date.today() - relativedelta(days=1))
    df_final['scoring_dt'] = date.today() - relativedelta(days=1)
    df_final.rename(columns={'cust_id': 'customer_id'}, inplace=True)

    table_ref = ucar_score_table  # UCAR's score table
    # client = bigquery.Client(project=project_id)
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
            df_final[col] = df_final[col].fillna(0).astype(int)
        if 'float' in str(d_type).lower():
            df_final[col] = df_final[col].fillna(0.0).astype(float)
        if 'string' in str(d_type).lower():
            df_final[col] = df_final[col].fillna('').astype(str)

    table_ref = '{}.{}.{}'.format(project_id, dataset_id, temp_table)
    client = bigquery.Client(project=project_id)
    if if_tbl_exists(client, table_ref):
        client.delete_table(table_ref)

    client.create_table(table_ref)
    config = bigquery.LoadJobConfig(schema=schema)
    config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    bq_table_instance = client.load_table_from_dataframe(df_final, table_ref, job_config=config)
    time.sleep(20)

    # check duplicate on ucar score table
    drop_sql = f''' delete from `{ucar_score_table}` where predict_model_id={model_id} and create_ts='{score_date_dash}' '''
    # client = bigquery.Client(project=project_id)
    client.query(drop_sql)
    time.sleep(20)

    # insert result into ucar score table
    insert_sql = 'select '
    for col, d_type in ll[:-1]:
        insert_sql += '{},'.format(col)
    insert_sql += '{}'.format(ll[-1][0])
    insert_sql += ' from `{}.{}.{}`'.format(project_id, dataset_id, temp_table)
    insert_sql = f'insert into `{ucar_score_table}` ' + insert_sql
    client = bigquery.Client(project=project_id)
    code = client.query(insert_sql)
    print(code.result())
    time.sleep(20)

    