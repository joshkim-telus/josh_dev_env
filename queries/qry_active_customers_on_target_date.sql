

--qry_active_customers_on_target_date--
--CUSTOMERS THAT ARE ACTIVE AS OF "target_date"--
--This query joins bq_prod_instnc_snpsht table with temp_tbl--
--change the joining table (temp_tbl) to meet your needs--


DECLARE target_date DATE DEFAULT "2023-02-01";
DECLARE interval_days INT64 DEFAULT 0;

-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_ADD(target_date,INTERVAL interval_days DAY) AS max_date 
)

SELECT    camp_id, 
          COUNT(DISTINCT ffh_ban) as active_ban_count 

FROM 

(

  SELECT  A.camp_id, 
          A.ffh_ban, 
          A.bus_prod_instnc_id, 
          A.pi_prod_instnc_stat_ts, 
          A.pi_prod_instnc_stat_cd

  FROM
  (
  -- ALL WIRELINE REGULAR CONSUMER BANS
    SELECT DISTINCT
          b.CAMP_ID as camp_id, 
          bacct_bus_bacct_num AS ffh_ban, 
          bus_prod_instnc_id, 
          pi_prod_instnc_stat_ts, 
          pi_prod_instnc_stat_cd
      FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a  
      --INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.temp_tbl` b 
      INNER JOIN  `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans` b 
      ON a.bacct_bus_bacct_num = b.ban 

    WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 
      AND DATE(prod_instnc_ts) = (SELECT max_date FROM cte_max_prod_instnc_date)
      AND pi_prod_instnc_stat_cd = 'A'
      AND consldt_cust_typ_cd = 'R' --Regular (not Business)

    ORDER BY bacct_bus_bacct_num, 
            bus_prod_instnc_id
  ) A 
)

GROUP BY camp_id

ORDER BY camp_id

