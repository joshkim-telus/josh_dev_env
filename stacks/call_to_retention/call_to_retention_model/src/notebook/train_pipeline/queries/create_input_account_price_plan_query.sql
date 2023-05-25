




-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"),INTERVAL "{score_date_delta}" DAY) AS max_date 
)


SELECT DISTINCT prod.bacct_bus_bacct_num
, REGEXP_EXTRACT(sub.bus_pp_catlg_itm_nm, r'Internet\s*\d+') as price_plan
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
 INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` AS sub
    ON sub.bus_prod_instnc_id = prod.BUS_PROD_INSTNC_ID
    AND DATE(prod.prod_instnc_ts) = DATE(sub.snpsht_dt)
    CROSS JOIN cte_max_prod_instnc_date dt 
 WHERE DATE(prod.prod_instnc_ts) BETWEEN DATE_SUB(dt.max_date , INTERVAL 1 WEEK) AND DATE_SUB(dt.max_date , INTERVAL 1 DAY) 
   AND DATE(sub.snpsht_dt) BETWEEN DATE_SUB(dt.max_date , INTERVAL 1 WEEK) AND DATE_SUB(dt.max_date , INTERVAL 1 DAY) 
   -- AND DATE(sub.pi_prod_instnc_stat_ts) BETWEEN "2023-04-01" AND "2023-05-10"
   AND prod.PI_PROD_INSTNC_TYP_CD = 'HSIC' --internet
   AND prod.PI_PROD_INSTNC_STAT_CD = 'A' --active products
   AND REGEXP_CONTAINS(sub.bus_pp_catlg_itm_nm, r'Internet\s(\d+)\s?(\w*)$')






