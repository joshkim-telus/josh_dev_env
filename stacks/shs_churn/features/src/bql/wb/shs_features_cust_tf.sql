
CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.shs_features_cust_tf`(_from_dt DATE, _to_dt DATE) AS 

(

WITH shs_base AS (
SELECT mthly_prod_instnc_snpsht_ts
, cust_id
, bi_product_type_cd
, bus_billing_account_num
, bus_prod_instnc_id
, chnl_org_txt
, chnl_org_type_cd
, chnl_org_type_txt
, activation_ts
, DATE_DIFF(mthly_prod_instnc_snpsht_ts, activation_ts, DAY) AS tenure_days
, DATE_DIFF(mthly_prod_instnc_snpsht_ts, activation_ts, MONTH) AS tenure_months
, commitment_type_cd
, contract_end_dt
, DATE_DIFF(contract_end_dt, mthly_prod_instnc_snpsht_ts, DAY) AS days_until_expiry
, credit_value_cd
, municipality_nm
, price_plan_cd
, price_plan_rate_amt
, prim_prod_instnc_resrc_str_pr
, province_state_cd
, self_install_ind
, shs_tos_bundle_ind
, term_length_num
, term_txt
, total_mrc_amt
, total_mrc_core_amt
, total_mrc_disc_amt
, total_mrc_extra_amt
FROM shs_churn.bq_telus_shs_customer_base 
)

SELECT mthly_prod_instnc_snpsht_ts AS ref_dt
, CAST(shs.cust_id AS INT64) AS cust_id
-- , bi_product_type_cd -- 
, CAST(bus_billing_account_num AS INT64) AS ban
-- , CAST(bus_prod_instnc_id AS INT64) AS bus_prod_instnc_id
-- , chnl_org_txt -- 
, chnl_org_type_cd
, chnl_org_type_txt
, tenure_days
-- , activation_ts -- 
, tenure_months

, CASE WHEN tenure_months <= 2 THEN "0 to 2 Mo" 
        WHEN tenure_months BETWEEN 3 AND 6 THEN "3 to 6 Mo"
        WHEN tenure_months BETWEEN 7 AND 12 THEN "7 to 12 Mo"
        WHEN tenure_months BETWEEN 13 AND 24 THEN "13 to 24 Mo"
        WHEN tenure_months BETWEEN 25 AND 35 THEN "25 to 35 Mo"
        WHEN tenure_months BETWEEN 36 AND 47 THEN "36 to 47 Mo"
        WHEN tenure_months BETWEEN 48 AND 60 THEN "48 to 60 Mo"
        ELSE "OTHER" 
        END AS tenure_months_groups

, commitment_type_cd
, days_until_expiry
-- , contract_end_dt --

, CASE WHEN days_until_expiry BETWEEN 0 AND 150 THEN "Expiring Soon"
        WHEN days_until_expiry > 150 THEN "Contracted"
        WHEN DATE_DIFF(mthly_prod_instnc_snpsht_ts, contract_end_dt, DAY) < 0 AND commitment_type_cd = "MTM" THEN "Month-to-Month"
        WHEN DATE_DIFF(mthly_prod_instnc_snpsht_ts, contract_end_dt, DAY) < 0 AND commitment_type_cd = "NOC" THEN "Not On Contract" 
        WHEN commitment_type_cd = "MTM" THEN "Month-to-Month"
        WHEN commitment_type_cd = "NOC" THEN "Not On Contract" 
        ELSE "UNKNOWN"
        END AS contract_expiring_soon

, credit_value_cd
, municipality_nm
-- , price_plan_cd
, CAST(price_plan_rate_amt AS INT64) AS price_plan_rate_amt
-- , prim_prod_instnc_resrc_str_pr
, province_state_cd
, self_install_ind
, shs_tos_bundle_ind
, CAST(term_length_num AS INT64) AS term_length_num
, term_txt
, CAST(total_mrc_amt AS FLOAT64) AS total_mrc_amt
, CAST(total_mrc_core_amt AS FLOAT64) AS total_mrc_core_amt
, CAST(total_mrc_disc_amt AS FLOAT64) AS total_mrc_disc_amt
, CAST(total_mrc_extra_amt AS FLOAT64) AS total_mrc_extra_amt
FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
LEFT JOIN shs_base shs 
ON CAST(base.ban AS INT64) = CAST(shs.bus_billing_account_num AS INT64)
AND base.ref_dt = shs.mthly_prod_instnc_snpsht_ts
AND shs.mthly_prod_instnc_snpsht_ts BETWEEN _from_dt AND _to_dt

)









