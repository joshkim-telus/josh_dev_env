
-- CREATE OR REPLACE TABLE FUNCTION {dataset_id}.{stack_name}_label0_tf(_from_dt DATE, _to_dt DATE)

-- AS 

--   (
  WITH 
    /* 
    ----------------------------------------------------------------------------------------------------
    DESCRIPTION:
    Calculate Label 0s for SHS Churn Model

    LABEL 0 DEFINITION:
    - All Active residential Home Solutions customers with an eligibility service at the customer address.
    - Must have an active monthly subscribed product (Home Phone, Internet, SHS, or TOS), as of the Reference Date.
    - Must be a residential customer. Business customers are out of the scope, for now.
    - Must not have the product / service as of Reference Date
    - Must exclude accounts with customer name "telus" or "test"
    - Remove accounts that have more than 10 active products

    GRANULARITY:
    - Customer ID
    - BAN

    MODEL SCENARIOS:
    - SHS Churn

    DEPENDENCY: 
    - `{project}.shs_churn.bq_telus_shs_customer_base`

    ----------------------------------------------------------------------------------------------------
    */

  -- STEP 1. Pull unique sets of cust_id, ban, and snapshot_dt from `{project}.shs_churn.bq_telus_shs_customer_base`
  label_0_rand AS (
    SELECT cust_id
    , bus_billing_account_num AS ban
    , mthly_prod_instnc_snpsht_ts as snapshot_dt 
    , rand() as rand_num
    FROM shs_churn.bq_telus_shs_customer_base
    WHERE mthly_prod_instnc_snpsht_ts BETWEEN _from_dt AND _to_dt
    GROUP BY 1,2,3
  ), 

  -- STEP 2. Randomly select a snapshot_dt out of all available dates for each cust_id + ban
  label_0_ref_dt AS (
    SELECT cust_id
    , ban
    , snapshot_dt
    , row_number() OVER(PARTITION BY ban ORDER BY rand_num DESC) AS row_number
    FROM label_0_rand
    qualify row_number = 1
  )

  -- STEP 3. Return values in the below format.
  SELECT _from_dt AS from_dt
  , _to_dt AS to_dt 
  , CAST(cust_id AS INT64) AS cust_id
  , 1012 AS cust_src_id
  , CAST(ban AS INT64) AS ban 
  , 1001 AS ban_src_id
  , DATE(snapshot_dt) AS _ref_dt 
  , 0 AS target_ind
  FROM label_0_ref_dt

-- )



