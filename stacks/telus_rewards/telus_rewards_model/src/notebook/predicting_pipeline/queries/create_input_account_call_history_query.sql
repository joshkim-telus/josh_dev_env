



-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"),INTERVAL "{score_date_delta}" DAY) AS max_date 
),


--the number of times the customer called to retention in the past 3 years
--default value to 0 when na
cte_call_frequency AS (
SELECT ban
, COUNT(DISTINCT CT_CALL_DT_MST) AS frequency 
FROM `divg-josh-pr-d1cc3a.call_to_retention_dataset.vw_ct_ffh_lnr`
CROSS JOIN cte_max_prod_instnc_date AS dt 
WHERE CT_CALL_DT_MST BETWEEN DATE_SUB(dt.max_date, INTERVAL 3 YEAR) and DATE_SUB(dt.max_date, INTERVAL 1 DAY)
AND ban IS NOT NULL
AND ban NOT IN (0) 
GROUP BY ban
), 


--the number of times the customer called to retention in the past 3 years
--default value to 999 when na
cte_call_recency AS (
SELECT ban
, date_diff(run_date, max_call_date, DAY) AS recency
FROM 
  (
  SELECT ban
  , MAX(CT_CALL_DT_MST) AS max_call_date
  , dt.max_date AS run_date
  FROM `divg-josh-pr-d1cc3a.call_to_retention_dataset.vw_ct_ffh_lnr`
  CROSS JOIN cte_max_prod_instnc_date AS dt 
  WHERE CT_CALL_DT_MST BETWEEN DATE_SUB(dt.max_date, INTERVAL 3 YEAR) and DATE_SUB(dt.max_date, INTERVAL 1 DAY)
  AND ban IS NOT NULL
  AND ban NOT IN (0) 
  GROUP BY ban
  , dt.max_date
  )
)


SELECT a.ban
, frequency 
, recency 
, CASE WHEN frequency > 0 THEN 1 ELSE 0 END AS have_called 

FROM cte_call_frequency a 
LEFT JOIN cte_call_recency b 
ON a.ban = b.ban















