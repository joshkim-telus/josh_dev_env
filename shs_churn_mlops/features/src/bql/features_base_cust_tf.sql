SELECT DISTINCT
       ref_dt,
       cust_id,
       cust_src_id
  FROM `{project}.{dataset}.master_target_table`
 WHERE from_dt = _from_dt
   AND to_dt = _to_dt
   