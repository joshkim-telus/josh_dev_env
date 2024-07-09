CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.call_centre_cust_tf`(_from_dt DATE, _to_dt DATE) AS 

(
  
WITH stage_calls AS (
SELECT base.ref_dt,
       base.ban,
       base.ban_src_id,
       calls.sess_id,
       IFNULL((SELECT CAST(value AS NUMERIC) FROM UNNEST(calls.map_durtn_str) WHERE key = 'Talk_Duration'), 0) AS talk_duration,
       IFNULL((SELECT CAST(value AS NUMERIC) FROM UNNEST(calls.map_durtn_str) WHERE key = 'Hold_Duration'), 0) AS hold_duration,
       IFNULL(calls.emp_id_cnt, 0) AS emp_id_cnt,
       IFNULL(calls.escaln_cnt, 0) AS escaln_cnt
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_call_sess_sum` AS calls
    ON calls.bus_bacct_num = base.ban
   AND calls.bus_bacct_num_src_id = base.ban_src_id
 WHERE calls.part_dt BETWEEN DATE_SUB(_from_dt, INTERVAL 180 DAY) 
                         AND DATE_SUB(_to_dt, INTERVAL 1 DAY)
   AND calls.part_dt BETWEEN DATE_SUB(base.ref_dt, INTERVAL 180 DAY) 
                         AND DATE_SUB(base.ref_dt, INTERVAL 1 DAY)
   AND (   --Filter for calls with a talk or hold duration > 0 secondscalls.
           (SELECT CAST(value AS NUMERIC) FROM UNNEST(calls.map_durtn_str) WHERE key = 'Talk_Duration') > 0
        OR (SELECT CAST(value AS NUMERIC) FROM UNNEST(calls.map_durtn_str) WHERE key = 'Hold_Duration') > 0
       )
)

SELECT ref_dt,
       ban,
       ban_src_id,
       COUNT(DISTINCT sess_id) AS call_cnt,
       AVG(talk_duration)      AS call_avg_talk_time,
       AVG(hold_duration)      AS call_avg_hold_time,
       AVG(emp_id_cnt)         AS call_avg_emp_cnt,
       AVG(escaln_cnt)         AS call_avg_esc_cnt,
       STDDEV(talk_duration)   AS call_std_talk_time,
       STDDEV(hold_duration)   AS call_std_hold_time,
       STDDEV(emp_id_cnt)      AS call_std_emp_cnt,
       STDDEV(escaln_cnt)      AS call_std_esc_cnt,
       SUM(talk_duration)      AS call_sum_talk_time,
       SUM(hold_duration)      AS call_sum_hold_time,
       SUM(emp_id_cnt)         AS call_sum_emp_cnt,
       SUM(escaln_cnt)         AS call_sum_esc_cnt,
       MAX(talk_duration)      AS call_max_talk_time,
       MAX(hold_duration)      AS call_max_hold_time,
       MAX(emp_id_cnt)         AS call_max_emp_cnt,
       MAX(escaln_cnt)         AS call_max_esc_cnt,
  FROM stage_calls
 GROUP BY 1, 2, 3
 
)