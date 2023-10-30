

--reached_targeted_by_ob_em_camp_apr_2022

declare start_date date default "2022-04-01";
declare end_date date default "2022-07-31";

select a.ban
, coalesce(earliest_ob_camp_reach, "1900-01-01") as earliest_ob_camp_reach
, coalesce(earliest_ob_camp_target, "1900-01-01") as earliest_ob_camp_target
, coalesce(earliest_em_camp_reach, "1900-01-01") as earliest_em_camp_reach
, coalesce(earliest_em_camp_target, "1900-01-01") as earliest_em_camp_target

from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 

left join 
  (
  select a.ban
  , b.earliest_ob_camp_reach
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  left join 
  (
  select distinct ban 
  , min(camp_inhome) as earliest_ob_camp_reach
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_call_results_2022` 
  where camp_inhome between start_date and end_date
  and call_result = "1+2"
  group by ban
  ) b
  on a.ban = b.ban
  ) b 

on a.ban = b.ban

left join 
  (
  select a.ban
  , b.earliest_ob_camp_target
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  left join 
  (
  select distinct ban 
  , min(camp_inhome) as earliest_ob_camp_target
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_call_results_2022` 
  where camp_inhome between start_date and end_date
  -- and call_result = "3Others"
  group by ban
  ) b
  on a.ban = b.ban
  ) c 

on a.ban = c.ban

left join 
  (
  select a.ban
  , b.earliest_em_camp_reach
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  , min(camp_inhome) as earliest_em_camp_reach
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_bans_2022` 
  where camp_inhome between start_date and end_date
  and opened = 1
  group by ban
  ) b
  on a.ban = b.ban
  ) d

on a.ban = d.ban

left join 
  (
  select a.ban
  , b.earliest_em_camp_target
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  , min(camp_inhome) as earliest_em_camp_target  
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_bans_2022` 
  where camp_inhome between start_date and end_date
  group by ban
  ) b
  on a.ban = b.ban
  ) e

on a.ban = e.ban
