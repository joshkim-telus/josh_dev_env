 WITH wln AS
        (
          SELECT 
            bacct_bus_bacct_num AS ban,
            consldt_cust_bus_cust_id,

          FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
          WHERE pi_prod_instnc_typ_cd IN ('HSIC','TTV','SMHM','SING')
          AND DATE(prod_instnc_ts) = DATE_SUB(PARSE_DATE('%Y%m%d', "{score_date}"),  INTERVAL "{score_date_delta}" DAY) --subtract X days to get latest data from snapshot table
          AND consldt_cust_typ_cd = 'R'
          AND consldt_cust_subtyp_cd = 'I'
          AND pi_prod_instnc_stat_cd = 'A'
          GROUP BY
            bacct_bus_bacct_num,
            consldt_cust_bus_cust_id
        ),

        wln_cd AS 
        (
          SELECT
           wln.ban,
            CASE WHEN srvc.conn_path_srvc_ntwk_typ_cd = "GPON" THEN 0 ELSE 1 END AS num_srvc_typ_copper,
            CASE WHEN srvc.conn_path_srvc_ntwk_typ_cd = "GPON" THEN 1 ELSE 0 END AS num_srvc_typ_gpon

          FROM wln
          LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_product_instance_gateway_daily_snpsht` as srvc
          ON srvc.cust_id = wln.consldt_cust_bus_cust_id
          WHERE DATE(snapshot_load_dt) = DATE_SUB(PARSE_DATE('%Y%m%d',"{score_date}"),  INTERVAL "{score_date_delta}" DAY) --subtract X days to get latest data from snapshot table 
        )

        SELECT
          ban,
          SUM(num_srvc_typ_copper) AS num_srvc_typ_copper_sum,
          SUM(num_srvc_typ_gpon) AS num_srvc_typ_gpon_sum 

          FROM wln_cd
         GROUP BY ban