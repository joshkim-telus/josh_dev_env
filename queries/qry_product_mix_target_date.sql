
--qry_product_mix_target_date--
--PRODUCT MIX COUNT OF CUSTOMERS AS OF "target_date"--

DECLARE target_date DATE DEFAULT "2023-02-01";
DECLARE interval_days INT64 DEFAULT 0;

-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_ADD(target_date,INTERVAL interval_days DAY) AS max_date 
),

-- ACTIVE WIRELINE REGULAR CONSUMER BANS
cte_ffh_ban AS(
  SELECT DISTINCT
         bacct_bus_bacct_num AS ffh_ban
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
   WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 
     AND DATE(prod_instnc_ts) = (SELECT max_date FROM cte_max_prod_instnc_date)
     AND pi_prod_instnc_stat_cd = 'A'
     AND consldt_cust_typ_cd = 'R' --Regular (not Business)
),

-- BAN PRODUCT MIX FOR ACTIVE REGULAR CONSUMERS
cte_product_mix AS (
  SELECT ffh_prod.bacct_bus_bacct_num AS ban,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') THEN ffh_prod.pi_prod_instnc_typ_cd ELSE NULL END) AS product_mix_all,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'HSIC' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS hsic_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SING' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS sing_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SMHM' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS shs_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'TTV'  THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS ttv_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'STV'  THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS stv_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'DIIC' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS diic_count
   FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS ffh_prod
   CROSS JOIN cte_max_prod_instnc_date AS dt
   WHERE DATE(ffh_prod.prod_instnc_ts) = dt.max_date
     AND ffh_prod.bacct_bus_bacct_num_src_id = 1001 --Wireline
     AND ffh_prod.pi_prod_instnc_stat_cd = 'A' --Active Products
     AND ffh_prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
     AND ffh_prod.pi_prod_instnc_typ_cd IN 
         (
           'DIIC', --Dialup
           'HSIC', --High Speed
           'SING', --Home Phone
           'SMHM', --Smart Home
           'STV',  --Satelite
           'TTV'   --TV
         )
   GROUP BY ffh_prod.bacct_bus_bacct_num
)

SELECT camp_id, 
       ban_count, 
       sum_product_mix / ban_count as avg_product_mix

FROM
(
  SELECT camp_id, 
        COUNT(DISTINCT ban) as ban_count, 
        SUM(product_mix_all) as sum_product_mix
  FROM 
  (
  -- FFH BAN WITH AGE AND PRODUCT MIX
  SELECT b.camp_id, 
        ffh.ffh_ban AS ban,
        prod.product_mix_all,
        prod.sing_count,
        prod.hsic_count,
        prod.shs_count,
        prod.ttv_count,
        prod.stv_count,
        prod.diic_count
    FROM cte_ffh_ban AS ffh
    -- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.temp_tbl` b 
    INNER JOIN  `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans` b 
    ON ffh.ffh_ban = b.ban 
    LEFT JOIN cte_product_mix AS prod
      ON ffh.ffh_ban = prod.ban
  )
  GROUP BY camp_id
)

ORDER BY camp_id