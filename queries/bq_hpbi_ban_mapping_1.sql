

-- bq_hpbi_ban_mapping_1
-- mob ban to ffh ban mapping using bq_hpbi_ban_mapping_hist table in feature hub

create or replace table `divg-josh-pr-d1cc3a.move_journey.bq_hpbi_ban_mapping_1` 

as 

select distinct
  mob_ban
, ffh_ban
, cust_id

from 

(
(
select a.ban as  mob_ban
, a.ban_src_id as mob_src_id
, a.related_ban as ffh_ban
, a.related_ban_src_id as ffh_src_id
, a.related_cust_id as cust_id


from `bi-srv-features-pr-ef5a93.util_ref_table.bq_hpbi_ban_mapping_hist` a 

where ban_src_id = 130 
and related_ban_src_id = 1001

group by a.ban 
, a.ban_src_id 
, a.related_ban  
, a.related_ban_src_id 
, a.related_cust_id
) 

union all 

(
select a.related_ban as  mob_ban
, a.related_ban_src_id as mob_src_id
, a.ban as ffh_ban
, a.ban_src_id as ffh_src_id
, a.cust_id

from `bi-srv-features-pr-ef5a93.util_ref_table.bq_hpbi_ban_mapping_hist` a 

where ban_src_id = 1001 

group by a.related_ban 
, a.related_ban_src_id 
, a.ban 
, a.ban_src_id 
, a.cust_id
) 
)





