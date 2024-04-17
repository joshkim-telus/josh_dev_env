

-- each row should contain either {cust_id ONLY, cust_id+lpds_id, mobility_ban ONLY, or mobility_ban+lpds_id). please let me know if you find any cases other than these 4 scenarios
WITH CTE AS (
  SELECT  
    cust_id,
    mobility_ban,
    lpds_id,
    CASE 
      WHEN (cust_id IS NOT NULL) AND (mobility_ban IS NULL) AND (lpds_id IS NULL) THEN 'cust_id only'
      WHEN (cust_id IS NOT NULL) AND (mobility_ban IS NULL) AND (lpds_id IS NOT NULL) THEN 'cust_id + lpds_id'
      WHEN (cust_id IS NULL) AND (mobility_ban IS NOT NULL) AND (lpds_id IS NULL) THEN 'mobility_ban only'
      WHEN (cust_id IS NULL) AND (mobility_ban IS NOT NULL) AND (lpds_id IS NOT NULL) THEN 'mobility_ban + lpds_id'
      ELSE 'Other' END AS validation
  FROM 
    bi-stg-mobilityds-pr-db8ce2.nba_offer_targeting_np.nba_ffh_offer_ranking 
)
SELECT
  validation,
  count(*)
FROM 
  CTE
GROUP BY 
  validation


