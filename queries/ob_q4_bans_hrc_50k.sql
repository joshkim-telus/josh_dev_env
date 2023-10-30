
  DECLARE target_date DATE DEFAULT "2022-07-01";
  DECLARE target_end_date DATE DEFAULT "2022-09-30";
  DECLARE interval_days INT64 DEFAULT 0;

----------------------------------------------------------------------------------------
---------------------------------------HRC 50K------------------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k_by_quarter`
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

    WHERE part_load_dt = target_date
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

    WHERE part_load_dt = target_date
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


CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k_by_quarter`
AS
SELECT CAMP_ID
, COALESCE(a.BACCT_NUM, b.ban) as ban
, COALESCE(b.hrc_50k, 0) as hrc_50k
FROM 
  (
  SELECT * 
  FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
  WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
  AND CAMP_INHOME BETWEEN target_date AND target_end_date
  ) a 
FULL OUTER JOIN `divg-josh-pr-d1cc3a.campaign_performance_analysis.hrc_50k_by_quarter` b 
ON a.BACCT_NUM = b.ban 

GROUP BY CAMP_ID
, BACCT_NUM
, b.ban
, b.hrc_50k 
; 


SELECT CAMP_ID
, hrc_50k
, COUNT(DISTINCT ban) 

FROM `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k_by_quarter`

GROUP BY CAMP_ID
, hrc_50k

ORDER BY CAMP_ID
, hrc_50k DESC 
; 

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k_q3`
AS
SELECT * FROM `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_hrc_50k_by_quarter`






