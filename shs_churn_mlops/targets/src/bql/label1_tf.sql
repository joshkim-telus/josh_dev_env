
-- CREATE OR REPLACE TABLE FUNCTION {dataset_id}.{stack_name}_label1_tf(_from_dt DATE, _to_dt DATE)

-- AS 

-- CREATE OR REPLACE TABLE FUNCTION shs_churn.shs_churn_label1_tf(_from_dt DATE, _to_dt DATE)

-- AS 

-- (
  WITH
  /* 
  ----------------------------------------------------------------------------------------------------
  DESCRIPTION:
  This table function returns a list of customers (BANs) who churned SHS between _from_dt and _to_dt, the event dates, and the target labels. 
  ----------------------------------------------------------------------------------------------------
  */

  ---------------------------------------------------------------------------------------
  -- TARGETS DETAILS
  ---------------------------------------------------------------------------------------

  base AS (
    SELECT cust_id
    , bus_billing_account_num	AS ban
    , DATE_TRUNC(deact_dt, MONTH) AS deact_dt
    , b.vol AS vol
    , b.invol AS invol 
    , b.churn AS churn
    FROM shs_churn.bq_telus_shs_customer_base a 
    LEFT JOIN shs_churn.bq_bq_telus_shs_churn_labels b 
    ON CAST(a.bus_prod_instnc_id AS INT64) = CAST(b.bus_prod_instnc_id AS INT64)
    AND a.mthly_prod_instnc_snpsht_ts = DATE_TRUNC(deact_dt, MONTH)
    WHERE a.mthly_prod_instnc_snpsht_ts BETWEEN _from_dt AND _to_dt
    AND b.deact_dt BETWEEN _from_dt AND _to_dt
  )
  /* 
  Generate a final table output to return. Final output format is to be kept consistent in below format. 

  Granularity: 
  - Customer
  - Billing Account

  Output Schema: 
    - from_dt	DATE
    - to_dt	DATE
    - cust_id	INTEGER
    - ban	INTEGER
    - _ref_dt	DATE
    - target_ind INTEGER
  */

  SELECT _from_dt AS from_dt
  , _to_dt AS to_dt
  , CAST(cust_id AS INT64) AS cust_id
  , 1012 AS cust_src_id
  , CAST(ban AS INT64) AS ban
  , 1001 AS ban_src_id
  , DATE(deact_dt) AS _ref_dt
  , vol AS target_ind
  FROM base 
  WHERE vol = 1

-- )



