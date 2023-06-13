

-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"),INTERVAL "{score_date_delta}" DAY) AS max_date 
), 

-- ACTIVE WIRELINE REGULAR CONSUMER BANS
cte_ffh_ban AS(
  SELECT DISTINCT
         bacct_bus_bacct_num AS ffh_ban
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
   WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC')
     AND DATE(prod_instnc_ts) = (SELECT max_date FROM cte_max_prod_instnc_date)
     AND pi_prod_instnc_stat_cd = 'A'
     AND consldt_cust_typ_cd = 'R' --Regular (not Business)
),

-- BAN MAPPING
cte_ban_map AS(
  SELECT DISTINCT
         ban AS ffh_ban,
         related_ban AS mob_ban
    FROM `bi-stg-divg-speech-pr-9d940b.common_dataset.bq_hpbi_ban_mapping`
   WHERE BAN_MATCH_WINNING_SCENARIO NOT LIKE '[6%'
     AND MASTER_SRC_ID = 1001
     AND RELATED_MASTER_SRC_ID = 130
),

-- BAN PRODUCT MIX FOR ACTIVE REGULAR CONSUMERS
cte_product_mix AS (
  SELECT ffh_prod.bacct_bus_bacct_num AS ban,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd IN ('SING', 'HSIC', 'TTV', 'SMHM', 'STV', 'DIIC') THEN ffh_prod.pi_prod_instnc_typ_cd ELSE NULL END) AS product_mix_all,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'HSIC' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS hsic_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SING' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS sing_count,
         COUNT(DISTINCT CASE WHEN mob_prod.pi_prod_instnc_typ_cd = 'C'    THEN mob_prod.bus_prod_instnc_id ELSE NULL END) AS mob_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SMHM' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS shs_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'TTV'  THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS ttv_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'STV'  THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS stv_count,
         COUNT(DISTINCT CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'DIIC' THEN ffh_prod.bus_prod_instnc_id ELSE NULL END) AS diic_count,
         MAX(CASE WHEN mob_prod.pi_prod_instnc_typ_cd = 'C' 
                   AND ABS(DATE_DIFF(dt.max_date, DATE(mob_prod.pi_actvn_ts), DAY)) BETWEEN 1 AND 30 
                  THEN 1 
                  ELSE 0 
                  END) AS new_c_ind,
         MAX(CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SING' 
                   AND ABS(DATE_DIFF(dt.max_date, DATE(ffh_prod.pi_actvn_ts), DAY)) BETWEEN 1 AND 30 
                  THEN 1 
                  ELSE 0 
                  END) AS new_sing_ind,
         MAX(CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'HSIC' 
                   AND ABS(DATE_DIFF(dt.max_date, DATE(ffh_prod.pi_actvn_ts), DAY)) BETWEEN 1 AND 30 
                  THEN 1 
                  ELSE 0 
                  END) AS new_hsic_ind,
         MAX(CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'TTV' 
                   AND ABS(DATE_DIFF(dt.max_date, DATE(ffh_prod.pi_actvn_ts), DAY)) BETWEEN 1 AND 30 
                  THEN 1 
                  ELSE 0 
                  END) AS new_ttv_ind,
         MAX(CASE WHEN ffh_prod.pi_prod_instnc_typ_cd = 'SMHM' 
                   AND ABS(DATE_DIFF(dt.max_date, DATE(ffh_prod.pi_actvn_ts), DAY)) BETWEEN 1 AND 30 
                  THEN 1 
                  ELSE 0 
                  END) AS new_smhm_ind,
         MAX(CASE WHEN mob_prod.pi_prod_instnc_typ_cd = 'C' THEN 1 ELSE 0 END) AS mnh_ind
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS ffh_prod
   CROSS JOIN cte_max_prod_instnc_date AS dt
    LEFT JOIN cte_ban_map AS ban_map
      ON ban_map.ffh_ban = ffh_prod.bacct_bus_bacct_num
    LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS mob_prod
      ON mob_prod.bacct_bus_bacct_num = ban_map.mob_ban
     AND DATE(mob_prod.prod_instnc_ts) = dt.max_date
     AND mob_prod.pi_prod_instnc_stat_cd = 'A' --Active Products
     AND mob_prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
   WHERE DATE(ffh_prod.prod_instnc_ts) = dt.max_date
     AND ffh_prod.bacct_bus_bacct_num_src_id = 1001 --Wireline
     AND ffh_prod.pi_prod_instnc_stat_cd = 'A' --Active Products
     AND ffh_prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
     AND ffh_prod.pi_prod_instnc_typ_cd IN 
			 (
			   'HSIC', --High Speed
			   'TTV'   --TV
			 )
   GROUP BY ffh_prod.bacct_bus_bacct_num
), 

cte_customer_tenure AS (
	SELECT ban
	, DATE_DIFF(curr_date, pi_actvn_dt, YEAR) as customer_tenure

	FROM
		(
		SELECT DISTINCT ffh_prod.bacct_bus_bacct_num as ban 
		, dt.max_date as curr_date
		, MIN(DATE(ffh_prod.pi_actvn_ts)) as pi_actvn_dt

		FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS ffh_prod
	   CROSS JOIN cte_max_prod_instnc_date AS dt
		LEFT JOIN cte_ban_map AS ban_map
		  ON ban_map.ffh_ban = ffh_prod.bacct_bus_bacct_num
		LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS mob_prod
		  ON mob_prod.bacct_bus_bacct_num = ban_map.mob_ban
		 AND DATE(mob_prod.prod_instnc_ts) = dt.max_date
		 AND mob_prod.pi_prod_instnc_stat_cd = 'A' --Active Products
		 AND mob_prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
	   WHERE DATE(ffh_prod.prod_instnc_ts) = dt.max_date
		 AND ffh_prod.bacct_bus_bacct_num_src_id = 1001 --Wireline
		 AND ffh_prod.pi_prod_instnc_stat_cd = 'A' --Active Products
		 AND ffh_prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
		 AND ffh_prod.pi_prod_instnc_typ_cd IN 
			 (
			   'HSIC', --High Speed
			   'TTV'   --TV
			 )

		GROUP BY ffh_prod.bacct_bus_bacct_num
		, dt.max_date
		)
)

-- FFH BAN WITH AGE AND PRODUCT MIX
SELECT ffh.ffh_ban AS ban,
	   tenure.customer_tenure, 
       prod.product_mix_all,
       prod.hsic_count,
       prod.ttv_count,
	   prod.sing_count,
       prod.mob_count,
       prod.shs_count,
       prod.new_hsic_ind,
       prod.new_ttv_ind,
       prod.new_sing_ind,
       prod.new_c_ind,
       prod.new_smhm_ind,
       prod.mnh_ind
  FROM cte_ffh_ban AS ffh
  LEFT JOIN cte_product_mix AS prod
    ON ffh.ffh_ban = prod.ban
  LEFT JOIN cte_customer_tenure as tenure
	ON ffh.ffh_ban = tenure.ban











