INSERT INTO
  `{project_id}.{dataset_id}.{table_id}`
SELECT
  DATE(part_dt) AS part_dt,
  SAFE_CAST(cust_id AS int) AS cust_id,
  SAFE_CAST(ban AS int) AS ban,
  SAFE_CAST(ban_src_id AS int) AS ban_src_id,
  SAFE_CAST(lpds_id AS int) AS lpds_id,
  SAFE_CAST(product_name AS string) AS product_name,
  SAFE_CAST(reco AS string) AS reco,
  SAFE_CAST(rank AS int) AS rank,
  SAFE_CAST(score AS numeric) AS score,
  SAFE_CAST(tier_score AS numeric) AS tier_score
FROM
  `{temp_table_id}`