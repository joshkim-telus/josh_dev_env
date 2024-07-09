CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.cust_ban_tf`(_from_dt DATE, _to_dt DATE) AS 

(

SELECT base.ref_dt,
       base.cust_id,
       base.ban,
       ANY_VALUE(acct.cr_risk_txt) AS acct_cr_risk_txt,
       ANY_VALUE(acct.ebill_ind) AS acct_ebill_ind,
       ANY_VALUE(cust.cr_val_txt) AS cust_cr_val_txt,
       ANY_VALUE(cust.pref_lang_txt) AS cust_pref_lang_txt,
       ANY_VALUE(cust.prov_state_cd) AS cust_prov_state_cd,
       ANY_VALUE(cust.age_yr_num) AS cust_age_yr_num
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_billing_account_dim` AS acct
    ON SAFE_CAST(acct.bacct_num AS INT64) = base.ban
   AND acct.bacct_src_id = base.ban_src_id
   AND base.ref_dt BETWEEN DATE(acct.eff_start_ts) AND DATE(acct.eff_end_ts)
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_customer_dim` AS cust
    ON cust.cust_id = acct.cust_id
   AND cust.cust_src_id = acct.cust_src_id
   AND base.ref_dt BETWEEN DATE(cust.eff_start_ts) AND DATE(cust.eff_end_ts)
 GROUP BY 1, 2, 3

)