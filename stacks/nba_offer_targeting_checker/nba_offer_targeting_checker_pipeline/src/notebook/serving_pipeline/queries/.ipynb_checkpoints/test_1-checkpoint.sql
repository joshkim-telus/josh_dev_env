
-- No offer #2 if there's no offer #1. Etc...
WITH CTE AS (
  SELECT 
    cust_id,
    mobility_ban,
    lpds_id,
    COUNT(CASE WHEN ranking = 1 THEN ranking ELSE NULL END) as ranking_1,
    COUNT(CASE WHEN ranking = 2 THEN ranking ELSE NULL END) as ranking_2,
    COUNT(CASE WHEN ranking = 3 THEN ranking ELSE NULL END) as ranking_3,
    COUNT(CASE WHEN ranking = 4 THEN ranking ELSE NULL END) as ranking_4,
    COUNT(CASE WHEN ranking = 5 THEN ranking ELSE NULL END) as ranking_5,
    COUNT(CASE WHEN ranking = 6 THEN ranking ELSE NULL END) as ranking_6,
    COUNT(CASE WHEN ranking = 7 THEN ranking ELSE NULL END) as ranking_7,
    COUNT(CASE WHEN ranking = 8 THEN ranking ELSE NULL END) as ranking_8,
    COUNT(CASE WHEN ranking = 9 THEN ranking ELSE NULL END) as ranking_9,
    COUNT(CASE WHEN ranking = 10 THEN ranking ELSE NULL END) as ranking_10,
    SUM(ranking) AS ranking_sum
  FROM 
    {dataset_id}.nba_ffh_offer_ranking
  GROUP BY
    cust_id,
    mobility_ban,
    lpds_id
),
ODD_ONES AS (
  SELECT 
    *
  FROM 
    CTE
  WHERE
    ranking_sum NOT IN (1,3,6,10,15,21,28,36,45,55,66,78,91,105,120,136,153,171,190,210,231,253,276,300,325,351,378,406,435,465,496,528,561,595,630,666,703,741,780,820,861,903,946,990,1035,1081,1128,1176,1225,1275)
),
valid_t1 AS (
  SELECT *,
  'valid_t1' as table_1 FROM {dataset_id}.nba_ffh_offer_ranking_cat3
),
valid_t2 AS (
  SELECT *,
  'valid_t2' as table_2 FROM {dataset_id}.nba_ffh_offer_ranking_existing
),
valid_t3 AS (
  SELECT *,
  'valid_t3' as table_3 FROM {dataset_id}.nba_ffh_offer_ranking_prospects
)
SELECT 
  OO.cust_id,
  OO.mobility_ban,
  OO.lpds_id,
  OO.ranking_1,
  OO.ranking_2,
  OO.ranking_3,
  OO.ranking_4,
  OO.ranking_5,
  OO.ranking_6,
  OO.ranking_7,
  OO.ranking_8,
  OO.ranking_9,
  OO.ranking_10,
  OO.ranking_sum,
  CASE WHEN (table_1 IS NOT NULL) AND (table_2 IS NOT NULL) AND (table_3 IS NOT NULL) THEN 1 ELSE 0 END as validation_ind
FROM 
  ODD_ONES OO
LEFT JOIN valid_t1 as t1
  on OO.cust_id = t1.cust_id
LEFT JOIN valid_t2 as t2
  on OO.cust_id = t2.cust_id
LEFT JOIN valid_t3 as t3
  on OO.cust_id = t3.cust_id
GROUP BY
  OO.cust_id,
  OO.mobility_ban,
  OO.lpds_id,
  OO.ranking_1,
  OO.ranking_2,
  OO.ranking_3,
  OO.ranking_4,
  OO.ranking_5,
  OO.ranking_6,
  OO.ranking_7,
  OO.ranking_8,
  OO.ranking_9,
  OO.ranking_10,
  OO.ranking_sum,
  validation_ind
HAVING
  validation_ind = 0


