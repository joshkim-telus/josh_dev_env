-- insert new rows into main bq table
INSERT INTO
  `{project_id}.{dataset_id}.{table_id}`
SELECT
    CAST(ban AS INT64) AS ban, 
    DATE(score_date) AS score_date, 
    model_id AS model_id, 
    CAST(score AS FLOAT64) AS score  
FROM
  `{temp_table_id}`


