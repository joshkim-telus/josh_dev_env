
CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.discount_apr_2022`

AS

WITH latest_bill AS (
                SELECT billg_acct_num AS ban,
                       MAX(bill_dt) AS max_bill_dt
                  FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_inv_sum_view`
                 WHERE bill_dt <= "2022-04-01"
                 AND bill_dt >  DATE_SUB("2022-04-01", INTERVAL 31 DAY)
                 GROUP BY billg_acct_num
            ),
            latest_bill_details AS (
            SELECT bill.ban,
                   bill.max_bill_dt AS latest_bill_dt,
                   doc.bill_doc_id,
                   doc.tot_inv_amt AS bill_tot_inv_amt,
                   doc.tot_tax_inv_amt AS bill_tot_tax_amt,
                   dtl.bill_chrg_dtl_id,
                   dtl.bill_chrg_dtl_typ_cd,
                   dtl.chrg_rev_cd,
                   dtl.srvc_resrc_typ_cd,
                   dtl.bill_itm_dsply_nm,
                   CASE WHEN disc.disc_prd_cvrg_start_dt IS NOT NULL
                        THEN DATE(disc.disc_prd_cvrg_start_dt)
                        WHEN REGEXP_CONTAINS(dtl.bill_itm_dsply_nm, r'effective [a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}} to [a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}}')
                        THEN PARSE_DATE("%b %d, %Y", REGEXP_EXTRACT(dtl.bill_itm_dsply_nm, r'[a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}}', 1, 1))
                        ELSE NULL
                        END AS discount_start_date,
                   CASE WHEN disc.disc_prd_cvrg_end_dt IS NOT NULL
                        THEN DATE(disc.disc_prd_cvrg_end_dt)
                        WHEN REGEXP_CONTAINS(dtl.bill_itm_dsply_nm, r'effective [a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}} to [a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}}')
                        THEN PARSE_DATE("%b %d, %Y", REGEXP_EXTRACT(dtl.bill_itm_dsply_nm, r'[a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}}', 1, 2))
                        WHEN dtl.bill_itm_dsply_nm LIKE '%discount end%'
                        THEN PARSE_DATE("%b %d, %Y", REGEXP_EXTRACT(dtl.bill_itm_dsply_nm, r'[a-zA-Z]{{3}} [0-9]{{2}}, [0-9]{{4}}', 1, 1))
                        ELSE NULL
                        END AS discount_end_date,
                   dtl.chrg_typ_cd,
                   dtl.net_chrg_amt
              FROM latest_bill AS bill
             INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_inv_sum_view` AS doc
                ON doc.billg_acct_num = bill.ban
               AND doc.bill_dt = bill.max_bill_dt
             INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` AS dtl
                ON dtl.bill_doc_id = doc.bill_doc_id
              LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_discount_chrg_dtl` AS disc
                ON disc.bill_chrg_dtl_id = dtl.bill_chrg_dtl_id
             WHERE dtl.bill_itm_sum_lvl_cd = 'D'
            )
            SELECT ban,
                   #latest_bill_dt,
                   #bill_doc_id,
                   bill_tot_inv_amt,
                   bill_tot_tax_amt, 
                   SUM(net_chrg_amt) AS tot_net_amt,
                   SUM(CASE WHEN chrg_typ_cd = 'DBT' THEN net_chrg_amt ELSE 0 END) AS tot_chrg_amt,
                   SUM(CASE WHEN chrg_typ_cd = 'CRD' THEN net_chrg_amt ELSE 0 END) AS tot_crdt_amt,
                   SUM(CASE WHEN chrg_typ_cd = 'DSC' THEN net_chrg_amt ELSE 0 END) AS tot_disc_amt, 
                   --SING (Single Line)
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SING' AND chrg_typ_cd = 'DBT' THEN net_chrg_amt ELSE 0 END) AS sing_chrg_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SING' AND chrg_typ_cd = 'CRD' THEN net_chrg_amt ELSE 0 END) AS sing_crdt_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SING' AND chrg_typ_cd = 'DSC' THEN net_chrg_amt ELSE 0 END) AS sing_disc_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SING' THEN net_chrg_amt ELSE 0 END) AS sing_net_amt,
                   --HSIC (High Speed Internet)
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'HSIC' AND chrg_typ_cd = 'DBT' THEN net_chrg_amt ELSE 0 END) AS hsic_chrg_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'HSIC' AND chrg_typ_cd = 'CRD' THEN net_chrg_amt ELSE 0 END) AS hsic_crdt_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'HSIC' AND chrg_typ_cd = 'DSC' THEN net_chrg_amt ELSE 0 END) AS hsic_disc_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'HSIC' THEN net_chrg_amt ELSE 0 END) AS hsic_net_amt,
                   --TTV (Telus TV)
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'TTV' AND chrg_typ_cd = 'DBT' THEN net_chrg_amt ELSE 0 END) AS ttv_chrg_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'TTV' AND chrg_typ_cd = 'CRD' THEN net_chrg_amt ELSE 0 END) AS ttv_crdt_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'TTV' AND chrg_typ_cd = 'DSC' THEN net_chrg_amt ELSE 0 END) AS ttv_disc_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'TTV' THEN net_chrg_amt ELSE 0 END) AS ttv_net_amt,
                   --SMHM (Smart Home Monitoring)
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SMHM' AND chrg_typ_cd = 'DBT' THEN net_chrg_amt ELSE 0 END) AS smhm_chrg_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SMHM' AND chrg_typ_cd = 'CRD' THEN net_chrg_amt ELSE 0 END) AS smhm_crdt_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SMHM' AND chrg_typ_cd = 'DSC' THEN net_chrg_amt ELSE 0 END) AS smhm_disc_amt,
                   SUM(CASE WHEN srvc_resrc_typ_cd = 'SMHM' THEN net_chrg_amt ELSE 0 END) AS smhm_net_amt

              FROM latest_bill_details
             GROUP BY
                   ban,
                   latest_bill_dt,
                   bill_doc_id,
                   bill_tot_inv_amt,
                   bill_tot_tax_amt