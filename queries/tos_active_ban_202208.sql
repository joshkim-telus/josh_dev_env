
DECLARE target_month_start DATE DEFAULT "2022-08-01";
DECLARE target_month_end DATE DEFAULT "2022-08-31"; 


--all active TOS BANs in the month of Aug 2022
CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.tos_crosssell.tos_active_ban_202208`
AS

SELECT distinct ban, 
1 AS tos_ind
from 
(
SELECT prod.bacct_bus_bacct_num AS ban,
       MAX(CASE WHEN CONTAINS_SUBSTR(sub.bus_pp_catlg_itm_nm, 'telus online security') = TRUE
                  OR CONTAINS_SUBSTR(sub.bus_pp_catlg_itm_nm, 'tos ') = TRUE
                THEN 1
                ELSE 0 END) AS tos_ind,
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
 INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` AS sub
    ON sub.bus_prod_instnc_id = prod.BUS_PROD_INSTNC_ID
    AND DATE(prod.prod_instnc_ts) = DATE(sub.snpsht_dt)
 WHERE DATE(prod.prod_instnc_ts) BETWEEN  target_month_start AND target_month_end
   AND DATE(sub.snpsht_dt) BETWEEN  target_month_start AND target_month_end
   AND DATE(sub.pi_prod_instnc_stat_ts) <= target_month_end
   AND prod.PI_PROD_INSTNC_TYP_CD = 'DIIC' --dialup
   AND prod.PI_PROD_INSTNC_STAT_CD = 'A' --active products
 GROUP BY ban
 HAVING tos_ind = 1
)
;



select * from `divg-josh-pr-d1cc3a.tos_crosssell.tos_active_ban_202208`

