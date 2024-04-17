--6) checks for duplicate ranking for the same customer, same offer
WITH base AS (
SELECT 
    cust_id
    , mobility_ban
    , lpds_id
    , promo_seg
    , count(ranking) as cnt1
    , count(distinct ranking) as cnt2
  FROM `nba_offer_targeting_np.nba_ffh_offer_ranking`
    group by 
    cust_id
    , mobility_ban
    , lpds_id
    , promo_seg
    order by cnt1 desc
)

select *
from base 
where cnt1-cnt2 != 0
or cnt1 != 1 
or cnt2 != 1
