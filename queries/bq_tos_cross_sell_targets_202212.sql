

DECLARE target_month_start DATE DEFAULT "2022-12-01";
DECLARE target_month_end DATE DEFAULT "2022-12-31"; 

--all BANs that added TOS to the service in 202212
CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.tos_cross_sell.tos_cross_sell_targets_202212`
AS
--25095
SELECT prod.bacct_bus_bacct_num AS ban,
       MAX(CASE WHEN CONTAINS_SUBSTR(lower(sub.bus_pp_catlg_itm_nm), 'telus online security') = TRUE
                  OR CONTAINS_SUBSTR(lower(sub.bus_pp_catlg_itm_nm), 'tos ') = TRUE
                THEN 1
                ELSE 0 END) AS tos_ind
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
 INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` AS sub
    ON sub.bus_prod_instnc_id = prod.BUS_PROD_INSTNC_ID
    AND DATE(prod.pi_prod_instnc_stat_ts) = DATE(sub.pi_prod_instnc_stat_ts)
 WHERE DATE(prod.prod_instnc_ts) = target_month_end
   AND DATE(sub.snpsht_dt) = target_month_end
   AND prod.PI_PROD_INSTNC_TYP_CD = 'DIIC' --dialup
   AND prod.PI_PROD_INSTNC_STAT_CD = 'A' --active products
   AND DATE(sub.pi_prod_instnc_stat_ts) BETWEEN target_month_start and target_month_end
 GROUP BY ban
 HAVING tos_ind = 1
; 


CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.tos_cross_sell.norton_cross_sell_targets_202212` 

AS

SELECT DISTINCT b.ban
FROM
(
  SELECT DISTINCT bus_cust_id
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_tos.bq_tos_norton_subscn_snpsht` 
  WHERE part_snpsht_dt = "2023-02-17"
  AND DATE(norton_subsc_dt) BETWEEN target_month_start AND target_month_end
  AND lob_cd IS NOT NULL 
  AND lob_cd NOT IN ("Mobility")
) a 
INNER JOIN  
(
  select ban, cust_id 
  from `bi-srv-features-pr-ef5a93.ban_base.bq_ban_base_ffh` 
  where part_dt = '2023-02-17'
  group by ban, cust_id 
) b 
ON a.bus_cust_id = b.cust_id
; 

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.tos_cross_sell.bq_tos_cross_sell_targets_202212`

AS

WITH cte_ffh_ban AS(
  SELECT DISTINCT
         bacct_bus_bacct_num AS ffh_ban
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
   WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 
     AND DATE(prod_instnc_ts) BETWEEN target_month_start AND target_month_end
     AND pi_prod_instnc_stat_cd = 'A'
     AND consldt_cust_typ_cd = 'R' --Regular (not Business)
)

select "2022-12" AS YEAR_MONTH
, a.ffh_ban as ban
, (CASE WHEN b.ban IS NOT NULL 
THEN 1
ELSE 0
END) AS product_acq_ind
from cte_ffh_ban a 
left join 
(
  select ban
  from 
  (
  (select distinct ban from `divg-josh-pr-d1cc3a.tos_cross_sell.norton_cross_sell_targets_202212`) 
  union all 
  (select distinct ban from `divg-josh-pr-d1cc3a.tos_cross_sell.tos_cross_sell_targets_202212`)
  )
  group by ban
) b 
on a.ffh_ban = b.ban 
; 

-- INSERT INTO `bi-stg-divg-speech-pr-9d940b.tos_cross_sell_dataset.bq_tos_cross_sell_targets`

-- SELECT YEAR_MONTH	
-- , ban			
-- , product_acq_ind	

--  FROM `divg-josh-pr-d1cc3a.tos_cross_sell.bq_tos_cross_sell_targets_202212`









