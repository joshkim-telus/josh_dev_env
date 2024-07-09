
CREATE OR REPLACE TABLE `divg-groovyhoon-pr-d2eab4.shs_churn.bq_telus_shs_customer_base` AS 

with base as (
SELECT 
    PARSE_DATE('%d-%b-%y', shs_mthly_product_snapshot.activation_ts) AS activation_ts, 
    shs_mthly_product_snapshot.bacct_group_id AS bacct_group_id,
    shs_mthly_product_snapshot.bacct_group_key AS bacct_group_key,
    shs_mthly_product_snapshot.bacct_key AS bacct_key,
    shs_bsgd.bi_product_type_cd AS bi_product_type_cd,
    shs_bsgd.bi_service_grp_key AS bi_service_grp_key,
    shs_mthly_product_snapshot.brand_cd AS brand_cd,
    shs_bad.bus_billing_account_num AS bus_billing_account_num,
    shs_mthly_product_snapshot.bus_prod_instnc_id AS bus_prod_instnc_id,
    shs_cod.chnl_org_txt AS chnl_org_txt,
    shs_cod.chnl_org_type_cd AS chnl_org_type_cd,
    shs_cod.chnl_org_type_txt AS chnl_org_type_txt,
    shs_ctd.commitment_type_cd AS commitment_type_cd,
    PARSE_DATE('%d-%b-%y', shs_mthly_product_snapshot.contract_end_dt) AS contract_end_dt,
    PARSE_DATE('%d-%b-%y', shs_mthly_product_snapshot.contract_start_dt) AS contract_start_dt,
    shs_bad.credit_rating_cd AS credit_rating_cd,
    shs_bad.credit_risk_cd AS credit_risk_cd,
    shs_cd.credit_value_cd AS credit_value_cd,
    shs_mthly_product_snapshot.cust_id AS cust_id,
    shs_cd.cust_type_cd AS cust_type_cd,
    shs_mthly_product_snapshot.fifa_product_offer_id AS fifa_product_offer_id,
    shs_mthly_product_snapshot.fifa_product_offer_key AS fifa_product_offer_key,
    shs_cd.indvdl_birth_dt AS indvdl_birth_dt_customer_dim,
    shs_cd.indvdl_team_member_ind AS indvdl_team_member_ind,
    cast(shs_mthly_product_snapshot.master_src_id as integer) AS master_src_id,
    DATE_ADD(PARSE_DATE('%d-%b-%y', shs_mthly_product_snapshot.mthly_prod_instnc_snpsht_ts), INTERVAL 1 DAY) AS mthly_prod_instnc_snpsht_ts, 
    shs_pid.municipality_nm AS municipality_nm,
    shs_bad.pap_ind AS pap_ind,
    shs_pid.postal_cd AS postal_cd,
    shs_mthly_product_snapshot.prepaid_ind AS prepaid_ind,
    shs_ppd.price_plan_cd AS price_plan_cd,
    shs_ppd.price_plan_rate_amt AS price_plan_rate_amt,
    shs_ppd.price_plan_txt AS price_plan_txt,
    shs_pid.prim_prod_instnc_resrc_str AS prim_prod_instnc_resrc_str_pr,
    shs_mthly_product_snapshot.prim_prod_instnc_resrc_str AS prim_prod_instnc_resrc_str,
    shs_pid.prod_instnc_type_cd AS prod_instnc_type_cd,
    shs_pid.province_state_cd AS province_state_cd,
    shs_bad.pymt_mthd_cd AS pymt_mthd_cd,
    shs_mthly_product_snapshot.self_install_ind AS self_install_ind,
    shs_pid.shs_tos_bundle_ind AS shs_tos_bundle_ind,
    shs_cd.team_member_concesn_cd AS team_member_concesn_cd,
    shs_cd.team_member_concesn_txt AS team_member_concesn_txt,
    shs_mthly_product_snapshot.technology_type_cd AS technology_type_cd,
    shs_td.term_length_num AS term_length_num,
    shs_td.term_txt AS term_txt,
    shs_mthly_product_snapshot.total_mrc_amt AS total_mrc_amt,
    shs_mthly_product_snapshot.total_mrc_core_amt AS total_mrc_core_amt,
    shs_mthly_product_snapshot.total_mrc_disc_amt AS total_mrc_disc_amt,
    shs_mthly_product_snapshot.total_mrc_extra_amt AS total_mrc_extra_amt,
    cast(shs_mthly_product_snapshot.unit_count_ind as integer) AS unit_count_ind,
    concat(shs_mthly_product_snapshot.mthly_prod_instnc_snpsht_ts,shs_mthly_product_snapshot.cust_id) as cc_id,
    concat(shs_mthly_product_snapshot.mthly_prod_instnc_snpsht_ts,shs_bad.bus_billing_account_num) as bc_id

FROM `shs_churn.shs_mthly_product_snapshot` shs_mthly_product_snapshot
  INNER JOIN `shs_churn.shs_ppd` shs_ppd ON (shs_mthly_product_snapshot.prim_price_plan_key = shs_ppd.price_plan_key)
  INNER JOIN `shs_churn.shs_pid` shs_pid ON (shs_mthly_product_snapshot.prod_instnc_key = shs_pid.prod_instnc_key)
  INNER JOIN `shs_churn.shs_cd` shs_cd ON (shs_mthly_product_snapshot.cust_key = shs_cd.cust_key)
  INNER JOIN `shs_churn.shs_bad` shs_bad ON (shs_mthly_product_snapshot.bacct_key = shs_bad.bacct_key)
  INNER JOIN `shs_churn.shs_td` shs_td ON (shs_mthly_product_snapshot.term_key = shs_td.term_key)
  INNER JOIN `shs_churn.shs_ctd` shs_ctd ON (shs_mthly_product_snapshot.commitment_type_key = shs_ctd.commitment_type_key)
  INNER JOIN `shs_churn.shs_cod` shs_cod ON (shs_mthly_product_snapshot.actvn_chnl_org_key = shs_cod.chnl_org_key)
  INNER JOIN `shs_churn.shs_bsgd` shs_bsgd ON (shs_mthly_product_snapshot.service_grp_key = shs_bsgd.bi_service_grp_key)

WHERE 
  cust_type_cd = 'R' 
  AND cast(unit_count_ind as integer) = 1 
  AND cast(shs_mthly_product_snapshot.master_src_id as integer) <> 130 
  AND shs_pid.prod_instnc_type_cd NOT IN ('CPE','DIIC','TRAN') 
  AND lower(bi_product_type_cd) like '%telus home security%'
)

select * from base 


