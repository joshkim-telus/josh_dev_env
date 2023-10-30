

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_cohorts_5020_scores` 
AS
SELECT a.ban
      ,decile_grp_num
      ,percentile_pct
      ,scor_qty
      ,predict_modl_id
FROM
  (
  SELECT DISTINCT BAN as ban 
  , ANALYSIS_DATE
  , month_col
  FROM `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_cohorts` 
  ) a 
INNER JOIN 
  (
    SELECT   bus_bacct_num as ban
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
            ,part_load_dt
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_predict_modl_scor_snpsht` 
    WHERE part_load_dt IN ("2022-02-01", "2022-03-01", "2022-04-01")
    and predict_modl_id='5020' 
    GROUP BY   bus_bacct_num
            ,decile_grp_num
            ,percentile_pct
            ,scor_qty
            ,predict_modl_id
            ,part_load_dt
  ) b 
ON a.ban = b.ban
AND a.ANALYSIS_DATE = b.part_load_dt