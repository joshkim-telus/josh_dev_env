
DECLARE _part_dt DATE DEFAULT '2023-01-09';

  -- Get BAN IMSI mapping
  -- (Granularity: BAN, IMSI)
  CREATE OR REPLACE TEMPORARY TABLE get_ban_imsi AS 
  SELECT DISTINCT
         prod.bacct_bus_bacct_num AS ban,
         imsi.prod_instnc_alias_str AS imsi_id,
         prod.pi_actvn_ts AS start_dt,
         CASE WHEN prod.pi_prod_instnc_stat_cd = 'C' THEN prod.pi_prod_instnc_stat_ts ELSE '9999-12-31' END AS end_dt
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
   INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_alias_snpsht` AS imsi
      ON imsi.bus_prod_instnc_id = prod.bus_prod_instnc_id
   WHERE imsi.prod_instnc_alias_typ_cd = 'IMSI'
     AND imsi.bus_prod_instnc_src_id = 130
     AND prod.bus_prod_instnc_src_id = 130
     AND DATE(prod.prod_instnc_ts) = _part_dt
     AND DATE(imsi.prod_instnc_alias_ts) = _part_dt;
  
  -- Join BAN and App Usage, and aggregate by BAN
  -- (Granularity: BAN, IMSI, Event Date, App Name))
  CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.tos_crosssell.create_input_account_alarmdotcom_app_usage` AS 
  SELECT mob.ban,
         usg.*
    FROM get_ban_imsi AS mob
   INNER JOIN `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` AS usg
      ON usg.imsi_num = mob.imsi_id
     AND usg.event_dt BETWEEN mob.start_dt AND mob.end_dt
   WHERE usg.event_dt BETWEEN DATE_SUB(_part_dt, INTERVAL 30 DAY) AND _part_dt
     AND usg.app_nm = 'alarm.com';

