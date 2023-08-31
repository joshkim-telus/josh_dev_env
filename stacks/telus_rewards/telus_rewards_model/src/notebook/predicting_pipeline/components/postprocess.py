from kfp.v2.dsl import (Artifact, Output, Input, HTML, component)

@component(
    base_image="northamerica-northeast1-docker.pkg.dev/cio-workbench-image-np-0ddefe/bi-platform/bi-aaaie/images/jupyter-kfp-base:1.0.0",
    output_component_file="xgb_postprocess.yaml",
)
def postprocess(
        project_id: str,
        file_bucket: str,
        dataset_id: str,
        service_type: str,
        score_date_dash: str,
        temp_table: str, 
        token: str
):
    import google
    import time
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import credentials
    from google.oauth2 import service_account
    
    def if_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False

    MODEL_ID = '5090'
    file_name = 'gs://{}/ucar/{}_prediction.csv.gz'.format(file_bucket, service_type)
    df_orig = pd.read_csv(file_name, compression='gzip')
    df_orig.dropna(subset=['ban'], inplace=True)
    df_orig.reset_index(drop=True, inplace=True)
    df_orig['scoring_date'] = score_date_dash
    df_orig.ban = df_orig.ban.astype(int)
    df_orig = df_orig.rename(columns={'ban': 'bus_bacct_num', 'score': 'score_num'})
    df_orig.score_num = df_orig.score_num.astype(float)
    df_orig['decile_grp_num'] = pd.qcut(df_orig['score_num'], q=10, labels=[i for i in range(10, 0, -1)])
    df_orig.decile_grp_num = df_orig.decile_grp_num.astype(int)
    df_orig['percentile_pct'] = (1 - df_orig.score_num.rank(pct=True))*100
    df_orig['percentile_pct'] = df_orig['percentile_pct'].apply(round, 0).astype(int)
    df_orig['predict_model_nm'] = 'FFH CALL TO RETENTION Model - DIVG'
    df_orig['model_type_cd'] = 'FFH'
    df_orig['subscriber_no'] = ""
    df_orig['prod_instnc_resrc_str'] = ""
    df_orig['service_instnc_id'] = ""
    df_orig['segment_nm'] = ""
    df_orig['segment_id'] = ""
    df_orig['classn_nm'] = ""
    df_orig['predict_model_id'] = MODEL_ID
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

    CREDENTIALS = google.oauth2.credentials.Credentials(token) # get credentials from token
    
    client = bigquery.Client(project=project_id, credentials=CREDENTIALS)
    df_cust = client.query(get_cust_id).to_dataframe()
    df_final = df_orig.set_index('bus_bacct_num').join(df_cust.set_index('bacct_bus_bacct_num')).reset_index()
    df_final = df_final.rename(columns={'index': 'bus_bacct_num', 'cust_bus_cust_id': 'cust_id'})
    df_final = df_final.sort_values(by=['score_num'], ascending=False)
    df_final.to_csv(file_name, compression='gzip', index=False)
    time.sleep(120)

#     # ------------------- directly write into UCAR score tables -----------------
#     df_final['segment_fr_nm'] = ''
#     df_final['create_ts'] = pd.Timestamp.now()
#     df_final['create_dt'] = pd.Timestamp('today')
#     df_final['month_used_dt'] = pd.Timestamp('today')
#     df_final['scoring_ts'] = pd.Timestamp(date.today() - relativedelta(days=5))
#     df_final['scoring_dt'] = date.today() - relativedelta(days=5)
#     df_final.rename(columns={'cust_id': 'customer_id'}, inplace=True)

#     table_ref = 'bi-stg-mobilityds-pr-db8ce2.ucar_ingestion.bq_product_instance_model_score_orc'  # UCAR's score table
#     table = client.get_table(table_ref)
#     schema = table.schema

#     ll = []
#     for item in schema:
#         col = item.name
#         d_type = item.field_type
#         if 'float' in str(d_type).lower():
#             d_type = 'FLOAT64'
#         ll.append((col, d_type))

#         if 'integer' in str(d_type).lower():
#             df_final[col] = df_final[col].fillna(0).astype(int)
#         if 'float' in str(d_type).lower():
#             df_final[col] = df_final[col].fillna(0.0).astype(float)
#         if 'string' in str(d_type).lower():
#             df_final[col] = df_final[col].fillna('').astype(str)

#     table_ref = '{}.{}.{}'.format(project_id, dataset_id, temp_table)
#     if if_tbl_exists(client, table_ref):
#         client.delete_table(table_ref)

#     client.create_table(table_ref)
#     config = bigquery.LoadJobConfig(schema=schema)
#     config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
#     bq_table_instance = client.load_table_from_dataframe(df_final, table_ref, job_config=config)
#     time.sleep(20)

# #     insert_sql = 'select '
# #     for col, d_type in ll[:-1]:
# #         insert_sql += '{},'.format(col)
# #     insert_sql += '{}'.format(ll[-1][0])
# #     insert_sql += ' from `{}.{}.temp_call_to_retention`'.format(project_id, dataset_id)
# #     insert_sql = 'insert into `bi-stg-mobilityds-pr-db8ce2.ucar_ingestion.bq_product_instance_model_score_orc` ' + insert_sql
# #     client = bigquery.Client(project=project_id)
# #     code = client.query(insert_sql)
# #     print(code.result())
# #     time.sleep(20)
    