SELECT 
  a.part_dt,
  -- customer ids
  a.cust_id,
  a.ban,
  a.ban_src_id,
  a.lpds_id,
  -- main model scores
  MAX(a.hsic_acquisition) AS hsic_acquisition,
  MAX(a.ttv_acquisition) AS ttv_acquisition,
  MAX(a.shs_acquisition) AS shs_acquisition,
  MAX(a.sing_acquisition) AS sing_acquisition,
  MAX(a.tos_acquisition) AS tos_acquisition,
  MAX(a.lwc_acquisition) AS lwc_acquisition,
  MAX(a.sws_acquisition) AS sws_acquisition,
  MAX(a.wifi_acquisition) AS wifi_acquisition,
  MAX(a.whsia_acquisition) AS whsia_acquisition,
  MAX(a.hpro_acquisition) AS hpro_acquisition,

  -- tier model scores
  MAX(b.hsic_medium_tier_acquisition) AS tos_ultimate_tier_acquisition,
  MAX(b.hsic_high_tier_acquisition) AS tos_complete_tier_acquisition,
  MAX(b.tos_ultimate_tier_acquisition) AS tos_ultimate_tier_acquisition,
  MAX(b.tos_complete_tier_acquisition) AS tos_complete_tier_acquisition
FROM 
  `telus_ffh_nba.bq_hs_nba_prospects_scores_vw` a
FULL OUTER JOIN
  `telus_ffh_nba.bq_hs_nba_prospects_tier_scores_vw` b
ON 
  a.part_dt = b.part_dt
  AND a.cust_id = b.cust_id 
  AND a.cust_src_id = b.cust_src_id 
  AND a.ban = b.ban 
  AND a.ban_src_id = b.ban_src_id
  AND a.lpds_id = b.lpds_id
WHERE
  a.ban_src_id = 130 -- ffh customers
GROUP BY
  a.part_dt,
  a.cust_id,
  a.ban,
  a.ban_src_id,
  a.lpds_id
;
