

declare start_date date default "2022-01-01";
declare end_date date default "2022-10-31";

select a.ban
, coalesce(reached_by_ob_camp_oct_31, 0) as reached_by_ob_camp_oct_31
, coalesce(targeted_by_ob_camp_oct_31, 0) as targeted_by_ob_camp_oct_31
, coalesce(reached_by_em_camp_oct_31, 0) as reached_by_em_camp_oct_31
, coalesce(targeted_by_em_camp_oct_31, 0) as targeted_by_em_camp_oct_31

from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 

left join 
  (
  select a.ban
  , 1 as reached_by_OB_camp_oct_31
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_call_results_2022` 
  where camp_inhome between start_date and end_date
  and call_result = "1+2"
  ) b
  on a.ban = b.ban
  ) b 

on a.ban = b.ban

left join 
  (
  select a.ban
  , 1 as targeted_by_ob_camp_oct_31
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_call_results_2022` 
  where camp_inhome between start_date and end_date
  -- and call_result = "3Others"
  ) b
  on a.ban = b.ban
  ) c 

on a.ban = c.ban

left join 
  (
  select a.ban
  , 1 as reached_by_em_camp_oct_31
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_bans_2022` 
  where camp_inhome between start_date and end_date
  and delivered = 1 and opened = 1
  ) b
  on a.ban = b.ban
  ) d

on a.ban = d.ban

left join 
  (
  select a.ban
  , 1 as targeted_by_em_camp_oct_31
  from `divg-josh-pr-d1cc3a.promo_expiry_analysis.promo_expiry_bans_apr_2022` a 
  inner join 
  (
  select distinct ban 
  from `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_bans_2022` 
  where camp_inhome between start_date and end_date
  ) b
  on a.ban = b.ban
  ) e

on a.ban = e.ban
