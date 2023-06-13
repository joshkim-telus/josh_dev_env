WITH cte_ffh_ban AS(
      SELECT DISTINCT 
             bacct_bus_bacct_num AS ban,
             addr.pstl_zip_cd_txt AS postal_code
        FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
       INNER JOIN `cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_service_address_daily_snpsht` AS addr
          ON addr.extrnl_srvc_addr_id = prod.pi_srvc_addr_fms_id
       WHERE prod.pi_prod_instnc_typ_cd IN ('HSIC','TTV','SMHM','SING') 
         AND DATE(prod.prod_instnc_ts) = DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"), INTERVAL "{score_date_delta}" DAY) #subtract X days to get latest data from snapshot table
         AND DATE(addr.part_create_dt) = DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"), INTERVAL "{score_date_delta}" DAY) #subtract X days to get latest data from snapshot table
         AND prod.pi_prod_instnc_stat_cd = 'A' 
         AND prod.consldt_cust_typ_cd = 'R' --Regular (not Business)
    ),

    cte_demo_data AS (
    SELECT Avg_Income,
           fsaldu,
           sgname,
           lsname,
      FROM `bi-stg-divg-speech-pr-9d940b.common_dataset.bq_exttable_adhoc_income_levels`
    )

    SELECT ffh.ban,
           demo.Avg_Income as demo_avg_income,
           demo.sgname as demo_sgname,
           demo.lsname as demo_lsname
      FROM cte_ffh_ban AS ffh
      LEFT JOIN cte_demo_data AS demo
        ON ffh.postal_code = demo.fsaldu