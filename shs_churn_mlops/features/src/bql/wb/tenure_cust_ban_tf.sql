CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.tenure_cust_ban_tf`(_from_dt DATE, _to_dt DATE) AS 

(

SELECT base.ref_dt,
       base.cust_id,
       base.ban,
       MAX(IF(prod.prod_instnc_type_cd = 'HSIC', DATE_DIFF(base.ref_dt, DATE(prod.actvn_ts), DAY), 0)) AS hsic_tenure_days,
       MAX(IF(prod.prod_instnc_type_cd = 'HSIC', DATE(prod.cntrct_end_ts), NULL)) AS contract_end_date_hsic,
       MAX(IF(prod.prod_instnc_type_cd = 'SING', DATE_DIFF(base.ref_dt, DATE(prod.actvn_ts), DAY), 0)) AS sing_tenure_days,
       MAX(IF(prod.prod_instnc_type_cd = 'SING', DATE(prod.cntrct_end_ts), NULL)) AS contract_end_date_sing,
       MAX(IF(prod.prod_instnc_type_cd = 'TTV',  DATE_DIFF(base.ref_dt, DATE(prod.actvn_ts), DAY), 0)) AS ttv_tenure_days,
       MAX(IF(prod.prod_instnc_type_cd = 'TTV',  DATE(prod.cntrct_end_ts), NULL)) AS contract_end_date_ttv,
       MAX(IF(prod.prod_instnc_type_cd = 'SMHM', DATE_DIFF(base.ref_dt, DATE(prod.actvn_ts), DAY), 0)) AS smhm_tenure_days,
       MAX(IF(prod.prod_instnc_type_cd = 'SMHM', DATE(prod.cntrct_end_ts), NULL)) AS contract_end_date_smhm,
       MAX(IF(DATE(acct.actvn_ts) <= base.ref_dt,
              DATE_DIFF(base.ref_dt, DATE(acct.actvn_ts), DAY),
              0
             )) AS ffh_tenure,
       MAX(IF(DATE(acct.actvn_ts) > base.ref_dt, 1, 0)) AS new_account_ind
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_billing_account_dim` AS acct
    ON SAFE_CAST(acct.bacct_num AS INT64) = base.ban
   AND acct.bacct_src_id = base.ban_src_id
   AND acct.current_ind = 1
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_product_instance_dim` AS prod
    ON SAFE_CAST(prod.bacct_num AS INT64) = base.ban
   AND prod.bacct_src_id = base.ban_src_id
   AND base.ref_dt BETWEEN DATE(prod.eff_start_ts) AND DATE(prod.eff_end_ts)
   AND prod.prod_instnc_stat_cd = 'A'
   AND DATE(prod.actvn_ts) <= base.ref_dt --only include products that are activated as of the reference date
 GROUP BY 1, 2, 3
 
)