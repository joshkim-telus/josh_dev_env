-- CREATE TEMP DATE DIM
WITH temp_date AS (
    SELECT EXTRACT(YEAR FROM my_date)||'-'||LPAD(CAST(EXTRACT(MONTH FROM my_date) AS STRING), 2, '0') AS year_month
      FROM (
            SELECT DATE_ADD(CAST("{v_start_date}" AS DATE), INTERVAL param MONTH) AS my_date
              FROM UNNEST(GENERATE_ARRAY(0, 5, 1)) AS param
           )
),
-- GET HS USAGE
temp_stage_hs_usage AS (
    SELECT prod.bacct_bus_bacct_num AS ban,
           EXTRACT(YEAR FROM usg.hsia_data_dtl_dt)||'-'||LPAD(CAST(EXTRACT(MONTH FROM usg.hsia_data_dtl_dt) AS STRING), 2, '0') AS usage_year_month,
           SUM(usg.upload_unit_qty)/1000000000 AS hs_ul_gb,
           SUM(usg.dnload_unit_qty)/1000000000 AS hs_dl_gb
      FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
     INNER JOIN `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wln_hsia_usg_dtl` AS usg
        ON CAST(usg.prod_instnc_id AS STRING) = prod.bus_prod_instnc_id
     WHERE DATE(prod.prod_instnc_ts) = CAST("{v_report_date}" AS DATE)
       AND DATE(usg.hsia_data_dtl_dt) BETWEEN CAST("{v_start_date}" AS DATE) AND CAST("{v_end_date}" AS DATE)
       AND prod.PI_PROD_INSTNC_TYP_CD = 'HSIC'
     GROUP BY 
           prod.bacct_bus_bacct_num,
           usage_year_month
),
-- CROSS JOIN WITH DATE DIM
temp_hs_usage AS (
    SELECT base.ban,
           dt.year_month,
           MAX(CASE WHEN dt.year_month = base.usage_year_month THEN base.hs_ul_gb
                    ELSE NULL
                    END) AS hs_ul_gb,
           MAX(CASE WHEN dt.year_month = base.usage_year_month THEN base.hs_dl_gb
                    ELSE NULL
                    END) AS hs_dl_gb
      FROM temp_stage_hs_usage AS base
     CROSS JOIN temp_date AS dt
     GROUP BY
           base.ban,
           dt.year_month
),
-- GENERATE MONTHS BACK RANK
temp_hs_usage_rank AS (
    SELECT ban,
           hs_ul_gb,
           hs_dl_gb,
           hs_ul_gb + hs_dl_gb AS hs_tot_gb,
           RANK() OVER(PARTITION BY ban ORDER BY year_month DESC) -1 AS months_back
      FROM temp_hs_usage
)
  SELECT ban,
        SUM(CASE WHEN months_back = 0 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_0,
        SUM(CASE WHEN months_back = 1 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_1,
        SUM(CASE WHEN months_back = 2 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_2,
        SUM(CASE WHEN months_back = 3 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_3,
        SUM(CASE WHEN months_back = 4 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_4,
        SUM(CASE WHEN months_back = 5 THEN hs_tot_gb ELSE 0 END) AS hs_tot_gb_5
    FROM temp_hs_usage_rank
  GROUP BY ban
  ; 