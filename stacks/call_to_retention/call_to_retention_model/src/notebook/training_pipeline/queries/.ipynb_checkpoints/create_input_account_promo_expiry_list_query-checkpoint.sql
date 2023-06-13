

-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH score_date AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"),INTERVAL "{score_date_delta}" DAY) AS max_date 
), 

promo_expiry_start AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{promo_expiry_start}"),INTERVAL "{score_date_delta}" DAY) AS start_date 
), 

promo_expiry_end AS(
  SELECT DATE_SUB(PARSE_DATE('%Y%m%d', "{promo_expiry_end}"),INTERVAL "{score_date_delta}" DAY) AS end_date 
)

select distinct bacct_bus_bacct_num as ban 
from `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a 
cross join score_date dt1 
cross join promo_expiry_start dt2 
cross join promo_expiry_end dt3 
where a.pi_prod_instnc_typ_cd in ("HSIC", "TV")
and date(a.prod_instnc_ts) = date_sub(dt1.max_date, INTERVAL 6 DAY) 
and date(a.pi_cntrct_end_ts) between dt2.start_date and dt3.end_date
and pi_prod_instnc_stat_cd = 'A'
AND consldt_cust_typ_cd = 'R' --Regular (not Business)