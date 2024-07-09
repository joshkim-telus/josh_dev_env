CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt DATE, _to_dt DATE) AS 

(

SELECT DISTINCT
       ref_dt,
       cust_id,
       cust_src_id,
       ban,
       ban_src_id
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.master_target_table`
 WHERE from_dt = _from_dt
   AND to_dt = _to_dt

)