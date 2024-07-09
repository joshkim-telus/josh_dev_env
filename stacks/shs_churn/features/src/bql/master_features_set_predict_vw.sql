SELECT *
  FROM `{project}.{dataset}.master_features_set` 
 WHERE training_mode = 0
   AND ref_dt = (SELECT MAX(ref_dt) FROM `{project}.{dataset}.master_features_set` WHERE training_mode = 0)