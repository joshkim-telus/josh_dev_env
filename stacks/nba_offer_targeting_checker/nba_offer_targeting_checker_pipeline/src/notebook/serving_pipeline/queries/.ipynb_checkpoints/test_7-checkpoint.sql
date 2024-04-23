--7) checks whether there's any empty string in digital_category field 
WITH base AS (
SELECT 
    digital_category
    , count(cust_id) as cnt1
    , count(distinct cust_id) as cnt2
  FROM `nba_offer_targeting_np.nba_ffh_offer_ranking`
    group by digital_category
    order by cnt1 desc
) 

select * 
from base 
where trim(digital_category) = ""