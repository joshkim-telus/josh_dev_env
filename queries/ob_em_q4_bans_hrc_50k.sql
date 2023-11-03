
----------------------------------------------------------------------------------------
----------------------------------Jan 2022 to Apr 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k`
AS
SELECT A.ban
  ,A.decile_grp_num
  ,A.percentile_pct
  ,A.scor_qty
  ,A.predict_modl_id
  ,CASE WHEN B.hrc_50k = 1 THEN 1 ELSE 0 END AS hrc_50k
FROM
(
  SELECT  
  bus_bacct_num AS ban
  ,decile_grp_num
  ,percentile_pct
  ,scor_qty
  ,predict_modl_id
  FROM 
  (
    SELECT   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_predict_modl_scor_snpsht` 
    WHERE part_load_dt = "2022-01-01"
    and predict_modl_id='5020' 
    GROUP BY   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
  )
  ORDER BY scor_qty DESC
) A 
LEFT JOIN
(
SELECT  
  bus_bacct_num AS ban
  ,decile_grp_num
  ,percentile_pct
  ,scor_qty
  ,predict_modl_id
  ,1 AS hrc_50k
  FROM 
  (
    SELECT   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_predict_modl_scor_snpsht` 
    WHERE part_load_dt = "2022-01-01"
    and predict_modl_id='5020' 
    GROUP BY   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
  )
  ORDER BY scor_qty DESC
  LIMIT 50000
) B 
ON A.ban = B.ban
ORDER BY A.scor_qty DESC
;


CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k`
AS
SELECT CAMP_ID
, COALESCE(a.BACCT_NUM, b.ban) as ban
, COALESCE(b.hrc_50k, 0) as hrc_50k
FROM 
  (
  SELECT * 
  FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.ob_q4`
  WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
  ) a 
FULL OUTER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k` b 
ON a.BACCT_NUM = b.ban 
GROUP BY CAMP_ID
, BACCT_NUM
, b.ban
, b.hrc_50k 
; 

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_q4_bans_hrc_50k`
AS
SELECT CAMP_ID
, COALESCE(a.BACCT_NUM, b.ban) as ban
, COALESCE(b.hrc_50k, 0) as hrc_50k
FROM 
  (
  SELECT * 
  FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.ob_q4`
  WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
  ) a 
FULL OUTER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k` b 
ON a.BACCT_NUM = b.ban 
GROUP BY CAMP_ID
, BACCT_NUM
, b.ban
, b.hrc_50k 
; 


----------------------------------------------------------------------------------------
----------------------------------Feb 2022 to May 2022----------------------------------
---------------------------------------HRC 50K------------------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k`
AS
SELECT A.ban
  ,A.decile_grp_num
  ,A.percentile_pct
  ,A.scor_qty
  ,A.predict_modl_id
  ,CASE WHEN B.hrc_50k = 1 THEN 1 ELSE 0 END AS hrc_50k
FROM
(
  SELECT  
  bus_bacct_num AS ban
  ,decile_grp_num
  ,percentile_pct
  ,scor_qty
  ,predict_modl_id
  FROM 
  (
    SELECT   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_predict_modl_scor_snpsht` a
  
    -- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_cntrct_expiry` c 
    -- ON a.bus_bacct_num = c.ban 

    -- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_disc` d 
    -- ON a.bus_bacct_num = d.ban 

    WHERE part_load_dt = "2022-02-01"
    and predict_modl_id='5020' 
    GROUP BY   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
  )
  ORDER BY scor_qty DESC
) A 
LEFT JOIN
(
SELECT  
  bus_bacct_num AS ban
  ,decile_grp_num
  ,percentile_pct
  ,scor_qty
  ,predict_modl_id
  ,1 AS hrc_50k
  FROM 
  (
    SELECT   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_predict_modl_scor_snpsht` a 
        
    -- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_cntrct_expiry` c 
    -- ON a.bus_bacct_num = c.ban 
	
    -- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_disc` d 
    -- ON a.bus_bacct_num = d.ban 

    WHERE part_load_dt = "2022-02-01"
    and predict_modl_id='5020' 
    GROUP BY   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
  )
  ORDER BY scor_qty DESC
  LIMIT 50000
) B 
ON A.ban = B.ban
ORDER BY A.scor_qty DESC
;


CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k`
AS
SELECT CAMP_ID
, COALESCE(a.BACCT_NUM, b.ban) as ban
, COALESCE(b.hrc_50k, 0) as hrc_50k
FROM 
  (
  SELECT * 
  FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
  WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
  AND CAMP_INHOME BETWEEN "2022-02-01" AND "2022-04-30"
  ) a 
FULL OUTER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k` b 
ON a.BACCT_NUM = b.ban 

-- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_cntrct_expiry` c 
-- ON a.BACCT_NUM = c.ban 

-- INNER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_elig_disc` d 
-- ON a.BACCT_NUM = d.ban 

GROUP BY CAMP_ID
, BACCT_NUM
, b.ban
, b.hrc_50k 
; 

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_q4_bans_hrc_50k`
AS
SELECT CAMP_ID
, COALESCE(a.BACCT_NUM, b.ban) as ban
, COALESCE(b.hrc_50k, 0) as hrc_50k
FROM 
  (
  SELECT * 
  FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
  WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
  AND CAMP_INHOME BETWEEN "2022-02-01" AND "2022-04-30"
  ) a 
FULL OUTER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k` b 
ON a.BACCT_NUM = b.ban 
GROUP BY CAMP_ID
, BACCT_NUM
, b.ban
, b.hrc_50k 
; 



