

DECLARE target_date DATE DEFAULT "2022-02-01";
DECLARE interval_days INT64 DEFAULT 0;

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.promo_expiry_analysis.earliest_contract_end_dt_feb_2022` 

AS

      SELECT a.bacct_bus_bacct_num as ban
      , a.pi_prod_instnc_typ_cd
      , min(pi_cntrct_end_ts) as earliest_contract_end_dt

      FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a 
      
      WHERE DATE(prod_instnc_ts) = target_date
      AND pi_prod_instnc_stat_cd = 'A'
      AND pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 

      GROUP BY a.bacct_bus_bacct_num
    , a.pi_prod_instnc_typ_cd


