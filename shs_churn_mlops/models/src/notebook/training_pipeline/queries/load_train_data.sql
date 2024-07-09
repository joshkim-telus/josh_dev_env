-- load target and features from training dataset
SELECT 
  ref_dt AS part_dt,
  {target_column}, 
  {customer_ids},
  {feature_names}
FROM 
  `{project_id}.{dataset_id}.{table_id}`
WHERE model_scenario in ({target_labels})
AND label > 0
