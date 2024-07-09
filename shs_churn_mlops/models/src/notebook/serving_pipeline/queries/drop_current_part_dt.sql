DECLARE current_part_dt DATE;
SET current_part_dt = DATE('{current_part_dt}');

-- remove rows where part dt is equal to current_part_dt
DELETE 
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE part_dt = current_part_dt

