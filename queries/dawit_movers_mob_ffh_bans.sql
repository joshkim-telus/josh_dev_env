

create or replace table `divg-josh-pr-d1cc3a.move_journey.dawit_movers_mob_ffh_bans` 

as 

-- BAN MAPPING
WITH cte_ban_map AS(
  SELECT DISTINCT
         ban AS ffh_ban,
         related_ban AS mob_ban
    FROM `bi-stg-divg-speech-pr-9d940b.common_dataset.bq_hpbi_ban_mapping`
   WHERE BAN_MATCH_WINNING_SCENARIO NOT LIKE '[6%'
     AND MASTER_SRC_ID = 1001
     AND RELATED_MASTER_SRC_ID = 130
)

select a.ffh_ban as ffh_ban
      ,b.mob_ban as  mob_ban
from `divg-josh-pr-d1cc3a.move_journey.dawit_movers_bans_still_active` a 
inner join cte_ban_map b 
on a.ffh_ban = b.ffh_ban
group by a.ffh_ban 
      ,b.mob_ban 




