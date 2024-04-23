--9) checks for validity of offer parameter sheet
with max_dt as (
    SELECT 
    max(part_dt) as part_dt
    FROM `nba_offer_targeting_np.bq_offer_targeting_params_upd` 
)

, tdy as (
    select a.* 
    from `nba_offer_targeting_np.bq_offer_targeting_params_upd` a 
    inner join max_dt b
    on a.part_dt = b.part_dt
    where a.if_active = 1 and a.HS_filters is not null
)

, checks as (
select 
count(*) as col1
, count(distinct ncid) as col2
, count(distinct promo_seg) as col3
from tdy
) 

select *
from checks 
where col1 != col2 
or col1 != col3
or col2 != col3
