



-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
WITH cte_max_prod_instnc_date AS(
  SELECT DATE_SUB("2022-02-22", INTERVAL 0 DAY) AS max_date 
),

-- ACTIVE WIRELINE REGULAR CONSUMER BANS
cte_ffh_ban AS(
  SELECT DISTINCT
         bacct_bus_bacct_num AS ffh_ban
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
   WHERE pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 
     AND DATE(prod_instnc_ts) = (SELECT max_date FROM cte_max_prod_instnc_date)
     AND pi_prod_instnc_stat_cd = 'A'
     AND consldt_cust_typ_cd = 'R' --Regular (not Business)
),

  -- Get BAN & UUID Identities
  -- (Granularity: BAN, UUID)
stage_identity AS (
  SELECT DISTINCT 
         DATE(cust_idnty_billg_acct_ts) AS part_dt,
         bus_bacct_num AS ban,
         bus_bacct_src_id AS ban_src_id,
         uuid
    FROM `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht`
   WHERE DATE(cust_idnty_billg_acct_ts) BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 31 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date)
    --  AND DATE(cust_idnty_billg_acct_ts) BETWEEN DATE(cust_idnty_acc_init_actvn_dt) AND DATE(cust_idnty_acc_deact_dt);
), 

  -- Get Clicks from rolling 90 days
  -- (Granularity: UUID, Click Event)
 stage_clicks AS (
  SELECT uuid,
         clckstrm_event_id,
         DATE(clckstrm_event_ts) AS clckstrm_event_dt,
         pg_hit_url_str
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_clckstrm_telus_web_event`
   WHERE DATE(clckstrm_event_ts) BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 31 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date)
     AND CONTAINS_SUBSTR(pg_hit_url_str, 'authorization') = FALSE
     AND pg_hit_url_str IS NOT NULL
), 

  -- Join BAN and Clicks
  -- (Granularity: BAN, UUID, Click Event)
 join_identity_clicks AS (
    SELECT idnty.ban,
           idnty.ban_src_id,
           idnty.uuid,
           clcks.clckstrm_event_id,
           clcks.clckstrm_event_dt,
           clcks.pg_hit_url_str
      FROM stage_clicks AS clcks 
     INNER JOIN stage_identity AS idnty
        ON clcks.uuid=idnty.uuid
       AND clcks.clckstrm_event_dt = idnty.part_dt
), 

  -- Generate Common Click Event Indicators (Mobility, Homem Phone, HS, TV, etc)
  -- (Granularity: BAN, UUID, Click Event)
stage_click_type AS (
    SELECT ban,
           ban_src_id,
           uuid,
           clckstrm_event_id,
           clckstrm_event_dt,
           pg_hit_url_str,

           -- Wireline
           CASE WHEN (
                       CONTAINS_SUBSTR(pg_hit_url_str, 'ffh') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'wln') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'home') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'shop/eligibility') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'home-phone') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'homephone') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'cloud-phone') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'internet') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'fibre') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'mobile-internet') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'smart-hub') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'wifi-plus') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, '/tv') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'tv-') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, '-tv') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'tv_') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, '_tv') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'optik') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'pik') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'streamplus') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'netflix') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'crave') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'hayu') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'amazon-prime') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'smarthome') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'shs') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'adt') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'customsecurity') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'online-security') = TRUE
                       OR CONTAINS_SUBSTR(pg_hit_url_str, 'smartwear-security') = TRUE
                     )
                THEN 1 ELSE 0 END AS wln_ind,
           -- Internet
           CASE WHEN (CONTAINS_SUBSTR(pg_hit_url_str, 'internet') = TRUE
                      OR CONTAINS_SUBSTR(pg_hit_url_str, 'fibre') = TRUE
                      OR CONTAINS_SUBSTR(pg_hit_url_str, 'wifi-plus') = TRUE
                     )
                 AND CONTAINS_SUBSTR(pg_hit_url_str, 'mobile-internet') = FALSE
                 AND CONTAINS_SUBSTR(pg_hit_url_str, 'smart-hub') = FALSE
                THEN 1 ELSE 0 END AS wln_hsic_ind,
           -- TV
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, '/tv') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'tv-') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, '-tv') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'tv_') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, '_tv') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'optik') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'pik') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'streamplus') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'netflix') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'crave') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'hayu') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'amazon-prime') = TRUE
                THEN 1 ELSE 0 END AS wln_tv_ind,
           -- TV Streaming (netflix, crave, hayu, amazon prime)
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, 'netflix') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'crave') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'hayu') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'amazon-prime') = TRUE
                THEN 1 ELSE 0 END AS wln_streaming_ind,
           -- Security
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, 'smarthome') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'shs') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'adt') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'customsecurity') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'online-security') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'smartwear-security') = TRUE
                THEN 1 ELSE 0 END AS wln_security_ind,
           -- Security Smarthome
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, 'smarthome') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'shs') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'adt') = TRUE
                  OR CONTAINS_SUBSTR(pg_hit_url_str, 'customsecurity') = TRUE
                THEN 1 ELSE 0 END AS wln_smarthome_security_ind,
           -- Security Online
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, 'online-security') = TRUE
                THEN 1 ELSE 0 END AS wln_online_security_ind,
           -- Security Smartwear
           CASE WHEN CONTAINS_SUBSTR(pg_hit_url_str, 'smartwear-security') = TRUE
                THEN 1 ELSE 0 END AS wln_smartwear_security_ind

      FROM join_identity_clicks
), 

  -- Generate Deatiled Click Event Indicators
  -- (Granularity: BAN, UUID, Click Event)
stage_ban_clckstrm_telus AS (
  SELECT (SELECT max_date FROM cte_max_prod_instnc_date) AS part_dt,
         ban,
         ban_src_id,
         uuid,
         clckstrm_event_id,
         clckstrm_event_dt,
         pg_hit_url_str,
         ----------------------------------------------------------------------------------------------------
         -- wireline
         ----------------------------------------------------------------------------------------------------
         wln_ind,
         wln_hsic_ind,
         wln_tv_ind,
         wln_streaming_ind,
         wln_security_ind,
         wln_smarthome_security_ind,
         wln_online_security_ind,
         wln_smartwear_security_ind

    FROM stage_click_type
), 

  -- Aggregate clicks by BAN over each time period
  -- (Granularity: BAN)
aggregate_clicks AS (
  SELECT ban,
         ban_src_id,
         ----------------------------------------------------------------------------------------------------
         -- r7d
         ----------------------------------------------------------------------------------------------------
         COUNT(DISTINCT CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN uuid ELSE NULL END) AS uuid_cnt_r7d,
         ARRAY_AGG(DISTINCT CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN uuid ELSE NULL END IGNORE NULLS) AS uuid_array_r7d,
         COUNT(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN clckstrm_event_id               ELSE NULL END) AS tot_click_cnt_r7d,
         -- wln
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_ind                           ELSE 0 END) AS wln_tot_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_hsic_ind                      ELSE 0 END) AS wln_hsic_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_tv_ind                        ELSE 0 END) AS wln_tv_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_streaming_ind                 ELSE 0 END) AS wln_streaming_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_security_ind                  ELSE 0 END) AS wln_security_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_smarthome_security_ind        ELSE 0 END) AS wln_smarthome_security_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_online_security_ind           ELSE 0 END) AS wln_online_security_cnt_r7d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 6 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_smartwear_security_ind        ELSE 0 END) AS wln_smartwear_security_cnt_r7d,
         ----------------------------------------------------------------------------------------------------
         -- r30d
         ----------------------------------------------------------------------------------------------------
         COUNT(DISTINCT CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN uuid ELSE NULL END) AS uuid_cnt_r30d,
         ARRAY_AGG(DISTINCT CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN uuid ELSE NULL END IGNORE NULLS) AS uuid_array_r30d,
         COUNT(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN clckstrm_event_id               ELSE NULL END) AS tot_click_cnt_r30d,
         -- wln
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_ind                           ELSE 0 END) AS wln_tot_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_hsic_ind                      ELSE 0 END) AS wln_hsic_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_tv_ind                        ELSE 0 END) AS wln_tv_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_streaming_ind                 ELSE 0 END) AS wln_streaming_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_security_ind                  ELSE 0 END) AS wln_security_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_smarthome_security_ind        ELSE 0 END) AS wln_smarthome_security_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_online_security_ind           ELSE 0 END) AS wln_online_security_cnt_r30d,
         SUM(CASE WHEN clckstrm_event_dt BETWEEN DATE_SUB((SELECT max_date FROM cte_max_prod_instnc_date), INTERVAL 29 DAY) AND (SELECT max_date FROM cte_max_prod_instnc_date) THEN wln_smartwear_security_ind        ELSE 0 END) AS wln_smartwear_security_cnt_r30d,
    FROM stage_ban_clckstrm_telus
   GROUP BY ban,
         ban_src_id
)

  -- Store aggregated clicks by BAN over each time period in Staging Table
  -- (Granularity: BAN)
  SELECT ban,
        --  ban_src_id,
         --------------------------------------------------
         -- r7d
         --------------------------------------------------
        --  uuid_cnt_r7d,
        --  uuid_array_r7d,
         sum(tot_click_cnt_r7d) as tot_click_cnt_r7d, 
         -- wln
         sum(wln_tot_cnt_r7d) as wln_tot_cnt_r7d,
         sum(wln_hsic_cnt_r7d) as wln_hsic_cnt_r7d,
         sum(wln_tv_cnt_r7d) as wln_tv_cnt_r7d,
         sum(wln_streaming_cnt_r7d) as wln_streaming_cnt_r7d,
         sum(wln_security_cnt_r7d) as wln_security_cnt_r7d,
         sum(wln_smarthome_security_cnt_r7d) as wln_smarthome_security_cnt_r7d,
         sum(wln_online_security_cnt_r7d) as wln_online_security_cnt_r7d,
         sum(wln_smartwear_security_cnt_r7d) as wln_smartwear_security_cnt_r7d,
         --------------------------------------------------
         -- r30d
         --------------------------------------------------
        --  uuid_cnt_r30d,
        --  uuid_array_r30d,
         sum(tot_click_cnt_r30d) as tot_click_cnt_r30d,
         -- wln
         sum(wln_tot_cnt_r30d) as wln_tot_cnt_r30d,
         sum(wln_hsic_cnt_r30d) as wln_hsic_cnt_r30d,
         sum(wln_tv_cnt_r30d) as wln_tv_cnt_r30d,
         sum(wln_streaming_cnt_r30d) as wln_streaming_cnt_r30d,
         sum(wln_security_cnt_r30d) as wln_security_cnt_r30d,
         sum(wln_smarthome_security_cnt_r30d) as wln_smarthome_security_cnt_r30d,
         sum(wln_online_security_cnt_r30d) as wln_online_security_cnt_r30d,
         sum(wln_smartwear_security_cnt_r30d) as wln_smartwear_security_cnt_r30d,
    FROM aggregate_clicks
    GROUP BY ban ;



