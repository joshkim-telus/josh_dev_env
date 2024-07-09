CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.prod_mix_cust_tf`(_from_dt DATE, _to_dt DATE) AS 

(

SELECT base.ref_dt,
       base.ban,
       MAX(prod.latest_prod_actvn_dt) AS prod_latest_actvn_dt,
       MAX(prod.latest_prod_deactvn_dt) AS prod_latest_deactvn_dt,
       SUM(prod.prod_cnt) AS prod_tot_cnt, 
       SUM(prod.wln_cnt) AS prod_wln_cnt,
       SUM(prod.wls_cnt) AS prod_wls_cnt,
       SUM(prod.mob_cnt) AS prod_mob_cnt,
       SUM(prod.sing_cnt) AS prod_sing_cnt,
       SUM(prod.hsic_cnt) AS prod_hsic_cnt,
       SUM(prod.whsia_cnt) AS prod_whsia_cnt, 
       SUM(prod.ttv_cnt) AS prod_ttv_cnt, 
       SUM(prod.smhm_cnt) AS prod_smhm_cnt, 
       SUM(prod.tos_cnt) AS prod_tos_cnt, 
       SUM(prod.wifiplus_cnt) AS prod_wifiplus_cnt, 
       SUM(prod.stv_cnt) AS prod_stv_cnt,
       SUM(prod.other_cnt) AS prod_other_cnt,
       SUM(prod.deact_prod_cnt) AS prod_deact_prod_cnt, 
       SUM(prod.act_prod_cnt_r7d) AS prod_act_prod_cnt_r7d, 
       SUM(prod.act_wln_cnt_r7d) AS prod_act_wln_cnt_r7d, 
       SUM(prod.deact_prod_cnt_r7d) AS prod_deact_prod_cnt_r7d, 
       SUM(prod.deact_wln_cnt_r7d) AS prod_deact_wln_cnt_r7d
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_billing_account_dim` AS acct
    ON acct.cust_id = base.cust_id
   AND acct.cust_src_id = base.cust_src_id
   AND acct.current_ind = 1
  LEFT JOIN `bi-srv-features-pr-ef5a93.ban_product.bq_ban_product_mix` AS prod
    ON prod.ban = SAFE_CAST(acct.bacct_num AS INT64)
   AND prod.ban_src_id = acct.bacct_src_id
   AND prod.part_dt = DATE_SUB(base.ref_dt, INTERVAL 1 DAY)
   AND prod.part_dt BETWEEN DATE_SUB(_from_dt, INTERVAL 1 DAY) AND _to_dt
 GROUP BY 1, 2

)