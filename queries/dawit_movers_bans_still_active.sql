

--qry_active_customers_on_target_date--
--CUSTOMERS THAT ARE ACTIVE AS OF "target_date"--
--This query joins bq_prod_instnc_snpsht table with temp_tbl--
--change the joining table (temp_tbl) to meet your needs--


DECLARE target_date DATE DEFAULT "2023-01-15";
DECLARE interval_days INT64 DEFAULT 0;

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.move_journey.dawit_movers_bans_still_active` 

AS 

-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_ADD(target_date,INTERVAL interval_days DAY) AS max_date 
)

SELECT    DISTINCT ffh_ban as ffh_ban

FROM 

(

  SELECT  A.ffh_ban, 
          A.bus_prod_instnc_id, 
          A.pi_prod_instnc_stat_ts, 
          A.pi_prod_instnc_stat_cd

  FROM
  (
  -- ALL WIRELINE REGULAR CONSUMER BANS
    SELECT DISTINCT
          bacct_bus_bacct_num AS ffh_ban, 
          bus_prod_instnc_id, 
          pi_prod_instnc_stat_ts, 
          pi_prod_instnc_stat_cd
      FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a  
      INNER JOIN 
      (
      select distinct ban 
      from `divg-josh-pr-d1cc3a.move_journey.dawit_movers_bans`
      ) b 
      ON a.bacct_bus_bacct_num = b.ban 

    WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM', 'DIIC', 'STV') 
      AND DATE(prod_instnc_ts) = (SELECT max_date FROM cte_max_prod_instnc_date)
      AND pi_prod_instnc_stat_cd = 'A'
      AND consldt_cust_typ_cd = 'R' --Regular (not Business)

    ORDER BY bacct_bus_bacct_num, 
            bus_prod_instnc_id
  ) A 
)












