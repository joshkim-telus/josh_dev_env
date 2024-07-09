
WITH stage_cust_monthly_usage AS (
SELECT base.ref_dt,
       base.ban,
       EXTRACT(YEAR FROM usg.hsia_data_dtl_dt)||'-'||LPAD(CAST(EXTRACT(MONTH FROM usg.hsia_data_dtl_dt) AS STRING), 2, '0') AS usage_yr_mth,
       SUM(usg.dnload_unit_qty)/1000000000 AS hs_dl_usg_gb,
       SUM(usg.upload_unit_qty)/1000000000 AS hs_ul_usg_gb
 FROM `{master_feature_table}`(_from_dt, _to_dt) AS base
 INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_product_instance_dim` AS prod
    ON prod.bacct_num = CAST(base.ban AS STRING)
   AND prod.bacct_src_id = base.ban_src_id
   AND prod.current_ind = 1
   AND prod.prod_instnc_type_cd = 'HSIC'
 INNER JOIN `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wln_hsia_usg_dtl` AS usg
    ON CAST(usg.prod_instnc_id AS STRING) = prod.prod_instnc_id
 WHERE DATE(usg.hsia_data_dtl_dt) BETWEEN DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 12 MONTH) 
                                      AND DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 1 DAY)
   AND DATE(usg.hsia_data_dtl_dt) BETWEEN DATE_SUB(DATE_TRUNC(_from_dt, MONTH), INTERVAL 24 MONTH) 
                                      AND DATE_SUB(DATE_TRUNC(_to_dt, MONTH), INTERVAL 1 DAY)
 GROUP BY 1, 2, 3
)
SELECT ref_dt,
       ban,
       ROUND(AVG(hs_dl_usg_gb + hs_ul_usg_gb), 2) AS hs_usg_avg_tot_gb,
       ROUND(AVG(hs_dl_usg_gb), 2) AS hs_usg_avg_dl_gb,
       ROUND(AVG(hs_ul_usg_gb), 2) AS hs_usg_avg_ul_gb,
  FROM stage_cust_monthly_usage
 GROUP BY 1, 2
 
