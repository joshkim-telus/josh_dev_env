-- No overlap between the tables
WITH CTE AS (
SELECT coalesce(safe_cast(a.cust_id as int64), 0) as cust_id
,coalesce(safe_cast(a.mobility_ban as int64), 0) as mobility_ban
,coalesce(safe_cast(a.lpds_id as int64), 0) as lpds_id
, 1 AS _count 
FROM nba_offer_targeting_np.nba_ffh_offer_ranking_cat3 a
GROUP BY cust_id
,mobility_ban
,lpds_id

UNION ALL

SELECT coalesce(safe_cast(a.cust_id as int64), 0) as cust_id
,coalesce(safe_cast(a.mobility_ban as int64), 0) as mobility_ban
,coalesce(safe_cast(a.lpds_id as int64), 0) as lpds_id
, 1 AS _count 
FROM nba_offer_targeting_np.nba_ffh_offer_ranking_prospects a
GROUP BY cust_id
,mobility_ban
,lpds_id

UNION ALL

SELECT coalesce(safe_cast(a.cust_id as int64), 0) as cust_id
,coalesce(safe_cast(a.mobility_ban as int64), 0) as mobility_ban
,coalesce(safe_cast(a.lpds_id as int64), 0) as lpds_id
, 1 AS _count 
FROM nba_offer_targeting_np.nba_ffh_offer_ranking_existing a
GROUP BY cust_id
,mobility_ban
,lpds_id
)

SELECT cust_id
,mobility_ban
,lpds_id
, SUM(_count) as _count 
FROM CTE 
GROUP BY cust_id
,mobility_ban
,lpds_id 
HAVING _count > 1 
ORDER BY _count DESC





