
-- insert new rows into main bq table
INSERT INTO
  `{project_id}.{dataset_id}.{table_id}`
SELECT
  DATE(part_dt) AS part_dt,
  SAFE_CAST(model_id AS int) AS model_id,
  SAFE_CAST(cust_id AS int) AS cust_id,
  SAFE_CAST(cust_src_id AS int) AS cust_src_id,
  SAFE_CAST(ban AS int) AS ban,
  SAFE_CAST(ban_src_id AS int) AS ban_src_id,
  SAFE_CAST(lpds_id AS int) AS lpds_id,
  SAFE_CAST(fms_address_id AS string) AS fms_address_id,
  SAFE_CAST(hsic_low_tier_acquisition AS numeric) AS hsic_low_tier_acquisition,
  SAFE_CAST(hsic_medium_tier_acquisition AS numeric) AS hsic_medium_tier_acquisition,
  SAFE_CAST(hsic_high_tier_acquisition AS numeric) AS hsic_high_tier_acquisition,
  SAFE_CAST(tos_basic_tier_acquisition AS numeric) AS tos_basic_tier_acquisition,
  SAFE_CAST(tos_standard_tier_acquisition AS numeric) AS tos_standard_tier_acquisition,
  SAFE_CAST(tos_ultimate_tier_acquisition AS numeric) AS tos_ultimate_tier_acquisition,
  SAFE_CAST(tos_complete_tier_acquisition AS numeric) AS tos_complete_tier_acquisition,
FROM
  `{temp_table_id}`

