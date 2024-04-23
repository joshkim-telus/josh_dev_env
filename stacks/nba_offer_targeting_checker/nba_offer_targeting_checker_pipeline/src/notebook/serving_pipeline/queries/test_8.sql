--8) checks for duplicate ranking number for the same customer 
with bas as (
  SELECT 
    cust_id
    , mobility_ban
    , lpds_id
    , count(ranking) as cnt1
    , count(distinct ranking) as cnt2
  FROM `nba_offer_targeting_np.nba_ffh_offer_ranking`
    group by 
    cust_id
    , mobility_ban
    , lpds_id
)
select * 
from bas 
where cnt1 != cnt2
