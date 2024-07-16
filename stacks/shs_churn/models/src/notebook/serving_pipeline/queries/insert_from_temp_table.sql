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
  SAFE_CAST(hsic_acquisition AS numeric) AS hsic_acquisition,
  SAFE_CAST(ttv_acquisition AS numeric) AS ttv_acquisition,
  SAFE_CAST(shs_acquisition AS numeric) AS shs_acquisition,
  SAFE_CAST(sing_acquisition AS numeric) AS sing_acquisition,
  SAFE_CAST(tos_acquisition AS numeric) AS tos_acquisition,
  SAFE_CAST(lwc_acquisition AS numeric) AS lwc_acquisition,
  SAFE_CAST(sws_acquisition AS numeric) AS sws_acquisition,
  SAFE_CAST(wifi_acquisition AS numeric) AS wifi_acquisition,
  SAFE_CAST(whsia_acquisition AS numeric) AS whsia_acquisition,
  SAFE_CAST(hpro_acquisition AS numeric) AS hpro_acquisition
FROM
  `{temp_table_id}`


