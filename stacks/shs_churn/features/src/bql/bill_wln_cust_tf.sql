
WITH level1_agg AS (
SELECT base.ref_dt,
       base.ban,
       ROUND(AVG(bill.ban_subtotal_amt), 2) AS bill_wln_avg_ban_subtotal_amt,
       ROUND(STDDEV(bill.ban_subtotal_amt), 2) AS bill_wln_stddev_ban_subtotal_amt,
       -- --  ROUND(SAFE_DIVIDE(SUM(bill.ban_dbt_sum_amt) - AVG(bill.ban_dbt_sum_amt), STDDEV(bill.ban_dbt_sum_amt)), 2) AS bill_wln_zscore_ban_debit_amt,
       ROUND(AVG(bill.ban_dbt_sum_amt), 2)  AS bill_wln_avg_ban_debit_amt,
       ROUND(STDDEV(bill.ban_dbt_sum_amt), 2)  AS bill_wln_stddev_ban_debit_amt,
       -- --  ROUND(SAFE_DIVIDE(SUM(bill.ban_dsc_sum_amt) - AVG(bill.ban_dsc_sum_amt), STDDEV(bill.ban_dsc_sum_amt)), 2) AS bill_wln_zscore_ban_discount_amt,
       ROUND(AVG(bill.ban_dsc_sum_amt), 2)  AS bill_wln_avg_ban_discount_amt,
       ROUND(STDDEV(bill.ban_dsc_sum_amt), 2)  AS bill_wln_stddev_ban_discount_amt,
       -- --  ROUND(SAFE_DIVIDE(SUM(bill.ban_crd_sum_amt) - AVG(bill.ban_crd_sum_amt), STDDEV(bill.ban_crd_sum_amt)), 2) AS bill_wln_zscore_ban_credit_amt,
       ROUND(AVG(bill.ban_crd_sum_amt), 2)  AS bill_wln_avg_ban_credit_amt,
       ROUND(STDDEV(bill.ban_crd_sum_amt), 2)  AS bill_wln_stddev_ban_credit_amt,

       IFNULL(ROUND(AVG(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_avg_sing_debit_amt,
       IFNULL(ROUND(AVG(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_avg_sing_discount_amt,
       IFNULL(ROUND(AVG(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_crd_sum_amt, NULL)), 2), 0) AS bill_wln_avg_sing_credit_amt,

       IFNULL(ROUND(AVG(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_avg_hsic_debit_amt,
       IFNULL(ROUND(AVG(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_avg_hsic_discount_amt,
       IFNULL(ROUND(AVG(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_crd_sum_amt, NULL)), 2), 0) AS bill_wln_avg_hsic_credit_amt,

       IFNULL(ROUND(AVG(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dbt_sum_amt,  NULL)), 2), 0) AS bill_wln_avg_ttv_debit_amt,
       IFNULL(ROUND(AVG(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dsc_sum_amt,  NULL)), 2), 0) AS bill_wln_avg_ttv_discount_amt,
       IFNULL(ROUND(AVG(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_crd_sum_amt,  NULL)), 2), 0) AS bill_wln_avg_ttv_credit_amt,

       IFNULL(ROUND(AVG(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_avg_smhm_debit_amt,
       IFNULL(ROUND(AVG(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_avg_smhm_discount_amt,
       IFNULL(ROUND(AVG(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_crd_sum_amt, NULL)), 2), 0) AS bill_wln_avg_smhm_credit_amt,

       IFNULL(ROUND(AVG(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_na_call_cnt, NULL)), 2), 0) AS bill_wln_avg_sing_ld_na_call_cnt,
       IFNULL(ROUND(AVG(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_intl_call_cnt, NULL)), 2), 0) AS bill_wln_avg_sing_ld_intl_call_cnt,
       IFNULL(ROUND(AVG(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_usg_gb, NULL)), 0), 0) AS bill_wln_avg_hsic_usg_gb,
       IFNULL(ROUND(AVG(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_ppv_cnt, NULL)), 2), 0) AS bill_wln_avg_ttv_ppv_cnt,
       IFNULL(ROUND(AVG(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_vod_cnt, NULL)), 2), 0) AS bill_wln_avg_ttv_vod_cnt,

       IFNULL(ROUND(STDDEV(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_sing_debit_amt,
       IFNULL(ROUND(STDDEV(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_sing_discount_amt,
       IFNULL(ROUND(STDDEV(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_crd_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_sing_credit_amt,

       IFNULL(ROUND(STDDEV(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_hsic_debit_amt,
       IFNULL(ROUND(STDDEV(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_hsic_discount_amt,
       IFNULL(ROUND(STDDEV(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_crd_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_hsic_credit_amt,

       IFNULL(ROUND(STDDEV(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dbt_sum_amt,  NULL)), 2), 0) AS bill_wln_stddev_ttv_debit_amt,
       IFNULL(ROUND(STDDEV(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dsc_sum_amt,  NULL)), 2), 0) AS bill_wln_stddev_ttv_discount_amt,
       IFNULL(ROUND(STDDEV(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_crd_sum_amt,  NULL)), 2), 0) AS bill_wln_stddev_ttv_credit_amt,

       IFNULL(ROUND(STDDEV(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dbt_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_smhm_debit_amt,
       IFNULL(ROUND(STDDEV(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dsc_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_smhm_discount_amt,
       IFNULL(ROUND(STDDEV(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_crd_sum_amt, NULL)), 2), 0) AS bill_wln_stddev_smhm_credit_amt,

       IFNULL(ROUND(STDDEV(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_na_call_cnt, NULL)), 2), 0) AS bill_wln_stddev_sing_ld_na_call_cnt,
       IFNULL(ROUND(STDDEV(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_intl_call_cnt, NULL)), 2), 0) AS bill_wln_stddev_sing_ld_intl_call_cnt,
       IFNULL(ROUND(STDDEV(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_usg_gb, NULL)), 0), 0) AS bill_wln_stddev_hsic_usg_gb,
       IFNULL(ROUND(STDDEV(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_ppv_cnt, NULL)), 2), 0) AS bill_wln_stddev_ttv_ppv_cnt,
       IFNULL(ROUND(STDDEV(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_vod_cnt, NULL)), 2), 0) AS bill_wln_stddev_ttv_vod_cnt

  FROM `{master_feature_table}`(_from_dt, _to_dt) AS base
  LEFT JOIN `bi-srv-features-pr-ef5a93.ban_billing.bq_wln_ban_bill_mthly` AS bill
    ON bill.ban = base.ban
   AND bill.part_dt BETWEEN DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 12 MONTH) 
                        AND DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 1 MONTH) 
   AND bill.part_dt BETWEEN DATE_SUB(DATE_TRUNC(_from_dt, MONTH), INTERVAL 24 MONTH) 
                        AND DATE_SUB(DATE_TRUNC(_to_dt, MONTH), INTERVAL 1 MONTH)
 WHERE bill.ban_subtotal_amt > 0
 GROUP BY 1, 2
)

SELECT base.ref_dt,
       base.ban,
       ROUND(AVG(SAFE_DIVIDE(bill.ban_subtotal_amt - level1_agg.bill_wln_avg_ban_subtotal_amt, level1_agg.bill_wln_stddev_ban_subtotal_amt)), 2) AS bill_wln_avg_zscore_ban_subtotal_amt,
       ANY_VALUE( bill_wln_avg_ban_subtotal_amt) as  bill_wln_avg_ban_subtotal_amt ,
       ANY_VALUE( bill_wln_stddev_ban_subtotal_amt) as  bill_wln_stddev_ban_subtotal_amt ,
     
       ROUND(AVG(SAFE_DIVIDE(bill.ban_dbt_sum_amt - level1_agg.bill_wln_avg_ban_debit_amt, level1_agg.bill_wln_stddev_ban_debit_amt)), 2) AS bill_wln_avg_zscore_ban_debit_amt,
       ANY_VALUE( bill_wln_avg_ban_debit_amt) as  bill_wln_avg_ban_debit_amt ,
       ANY_VALUE( bill_wln_stddev_ban_debit_amt) as  bill_wln_stddev_ban_debit_amt ,
    
       ROUND(AVG(SAFE_DIVIDE(bill.ban_dsc_sum_amt - level1_agg.bill_wln_avg_ban_discount_amt, level1_agg.bill_wln_stddev_ban_discount_amt)), 2) AS bill_wln_avg_zscore_ban_discount_amt,
       ANY_VALUE( bill_wln_avg_ban_discount_amt) as  bill_wln_avg_ban_discount_amt ,
       ANY_VALUE( bill_wln_stddev_ban_discount_amt) as  bill_wln_stddev_ban_discount_amt ,

       ROUND(AVG(SAFE_DIVIDE(bill.ban_crd_sum_amt - level1_agg.bill_wln_avg_ban_credit_amt, level1_agg.bill_wln_stddev_ban_credit_amt)), 2) AS bill_wln_avg_zscore_ban_credit_amt,
       ANY_VALUE( bill_wln_avg_ban_credit_amt) as  bill_wln_avg_ban_credit_amt ,
       ANY_VALUE( bill_wln_stddev_ban_discount_amt) as  bill_wln_stddev_ban_credit_amt ,
   
       ROUND(AVG(SAFE_DIVIDE(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dbt_sum_amt, 0)      - level1_agg.bill_wln_avg_sing_debit_amt,        level1_agg.bill_wln_stddev_sing_debit_amt       )),2) AS bill_wln_avg_zscore_sing_debit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_dsc_sum_amt, 0)      - level1_agg.bill_wln_avg_sing_discount_amt,     level1_agg.bill_wln_stddev_sing_discount_amt    )),2) AS bill_wln_avg_zscore_sing_discount_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_crd_sum_amt, 0)      - level1_agg.bill_wln_avg_sing_credit_amt,       level1_agg.bill_wln_stddev_sing_credit_amt      )),2) AS bill_wln_avg_zscore_sing_credit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dbt_sum_amt, 0)      - level1_agg.bill_wln_avg_hsic_debit_amt,        level1_agg.bill_wln_stddev_hsic_debit_amt       )),2) AS bill_wln_avg_zscore_hsic_debit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_dsc_sum_amt, 0)      - level1_agg.bill_wln_avg_hsic_discount_amt,     level1_agg.bill_wln_stddev_hsic_discount_amt    )),2) AS bill_wln_avg_zscore_hsic_discount_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_crd_sum_amt, 0)      - level1_agg.bill_wln_avg_hsic_credit_amt,       level1_agg.bill_wln_stddev_hsic_credit_amt      )),2) AS bill_wln_avg_zscore_hsic_credit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dbt_sum_amt,  0)      - level1_agg.bill_wln_avg_ttv_debit_amt,         level1_agg.bill_wln_stddev_ttv_debit_amt        )),2) AS bill_wln_avg_zscore_ttv_debit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_dsc_sum_amt,  0)      - level1_agg.bill_wln_avg_ttv_discount_amt,      level1_agg.bill_wln_stddev_ttv_discount_amt     )),2) AS bill_wln_avg_zscore_ttv_discount_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.ttv_dbt_sum_amt  <> 0, bill.ttv_crd_sum_amt,  0)      - level1_agg.bill_wln_avg_ttv_credit_amt,        level1_agg.bill_wln_stddev_ttv_credit_amt       )),2) AS bill_wln_avg_zscore_ttv_credit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dbt_sum_amt, 0)      - level1_agg.bill_wln_avg_smhm_debit_amt,        level1_agg.bill_wln_stddev_smhm_debit_amt       )),2) AS bill_wln_avg_zscore_smhm_debit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_dsc_sum_amt, 0)      - level1_agg.bill_wln_avg_smhm_discount_amt,     level1_agg.bill_wln_stddev_smhm_discount_amt    )),2) AS bill_wln_avg_zscore_smhm_discount_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.smhm_dbt_sum_amt <> 0, bill.smhm_crd_sum_amt, 0)      - level1_agg.bill_wln_avg_smhm_credit_amt,       level1_agg.bill_wln_stddev_smhm_credit_amt      )),2) AS bill_wln_avg_zscore_smhm_credit_amt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_na_call_cnt, 0)   - level1_agg.bill_wln_avg_sing_ld_na_call_cnt,   level1_agg.bill_wln_stddev_sing_ld_na_call_cnt  )),2) AS bill_wln_avg_zscore_sing_ld_na_call_cnt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.sing_dbt_sum_amt <> 0, bill.sing_ld_intl_call_cnt, 0) - level1_agg.bill_wln_avg_sing_ld_intl_call_cnt, level1_agg.bill_wln_stddev_sing_ld_intl_call_cnt)),2) AS bill_wln_avg_zscore_sing_ld_intl_call_cnt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.hsic_dbt_sum_amt <> 0, bill.hsic_usg_gb, 0)           - level1_agg.bill_wln_avg_hsic_usg_gb,           level1_agg.bill_wln_stddev_hsic_usg_gb          )),2) AS bill_wln_avg_zscore_hsic_usg_gb,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_ppv_cnt, 0)           - level1_agg.bill_wln_avg_ttv_ppv_cnt,           level1_agg.bill_wln_stddev_ttv_ppv_cnt          )),2) AS bill_wln_avg_zscore_ttv_ppv_cnt,
       ROUND(AVG(SAFE_DIVIDE(IF(bill.ttv_dbt_sum_amt <> 0,  bill.ttv_vod_cnt, 0)           - level1_agg.bill_wln_avg_ttv_vod_cnt,           level1_agg.bill_wln_stddev_ttv_vod_cnt          )),2) AS bill_wln_avg_zscore_ttv_vod_cnt,

       ANY_VALUE(bill_wln_avg_sing_debit_amt) AS bill_wln_avg_sing_debit_amt,
       ANY_VALUE(bill_wln_avg_sing_discount_amt) AS bill_wln_avg_sing_discount_amt,
       ANY_VALUE(bill_wln_avg_sing_credit_amt) AS bill_wln_avg_sing_credit_amt,

       ANY_VALUE(bill_wln_avg_hsic_debit_amt) AS bill_wln_avg_hsic_debit_amt,
       ANY_VALUE(bill_wln_avg_hsic_discount_amt) AS bill_wln_avg_hsic_discount_amt,
       ANY_VALUE(bill_wln_avg_hsic_credit_amt) AS bill_wln_avg_hsic_credit_amt,

       ANY_VALUE(bill_wln_avg_ttv_debit_amt) AS bill_wln_avg_ttv_debit_amt,
       ANY_VALUE(bill_wln_avg_ttv_discount_amt) AS bill_wln_avg_ttv_discount_amt,
       ANY_VALUE(bill_wln_avg_ttv_credit_amt) AS bill_wln_avg_ttv_credit_amt,

       ANY_VALUE(bill_wln_avg_smhm_debit_amt) AS bill_wln_avg_smhm_debit_amt,
       ANY_VALUE(bill_wln_avg_smhm_discount_amt) AS bill_wln_avg_smhm_discount_amt,
       ANY_VALUE(bill_wln_avg_smhm_credit_amt) AS bill_wln_avg_smhm_credit_amt,

       ANY_VALUE(bill_wln_avg_sing_ld_na_call_cnt) AS bill_wln_avg_sing_ld_na_call_cnt,
       ANY_VALUE(bill_wln_avg_sing_ld_intl_call_cnt) AS bill_wln_avg_sing_ld_intl_call_cnt,
       ANY_VALUE(bill_wln_avg_hsic_usg_gb) AS bill_wln_avg_hsic_usg_gb,
       ANY_VALUE(bill_wln_avg_ttv_ppv_cnt) AS bill_wln_avg_ttv_ppv_cnt,
       ANY_VALUE(bill_wln_avg_ttv_vod_cnt) AS bill_wln_avg_ttv_vod_cnt,

       ANY_VALUE(bill_wln_stddev_sing_debit_amt) AS bill_wln_stddev_sing_debit_amt,
       ANY_VALUE(bill_wln_stddev_sing_discount_amt) AS bill_wln_stddev_sing_discount_amt,
       ANY_VALUE(bill_wln_stddev_sing_credit_amt) AS bill_wln_stddev_sing_credit_amt,
       ANY_VALUE(bill_wln_stddev_hsic_debit_amt) AS bill_wln_stddev_hsic_debit_amt,
       ANY_VALUE(bill_wln_stddev_hsic_discount_amt) AS bill_wln_stddev_hsic_discount_amt,
       ANY_VALUE(bill_wln_stddev_hsic_credit_amt) AS bill_wln_stddev_hsic_credit_amt,
       ANY_VALUE(bill_wln_stddev_ttv_debit_amt) AS bill_wln_stddev_ttv_debit_amt,
       ANY_VALUE(bill_wln_stddev_ttv_discount_amt) AS bill_wln_stddev_ttv_discount_amt,
       ANY_VALUE(bill_wln_stddev_ttv_credit_amt) AS bill_wln_stddev_ttv_credit_amt,
       ANY_VALUE(bill_wln_stddev_smhm_debit_amt) AS bill_wln_stddev_smhm_debit_amt,
       ANY_VALUE(bill_wln_stddev_smhm_discount_amt) AS bill_wln_stddev_smhm_discount_amt,
       ANY_VALUE(bill_wln_stddev_smhm_credit_amt) AS bill_wln_stddev_smhm_credit_amt,
       ANY_VALUE(bill_wln_stddev_sing_ld_na_call_cnt) AS bill_wln_stddev_sing_ld_na_call_cnt,
       ANY_VALUE(bill_wln_stddev_sing_ld_intl_call_cnt) AS bill_wln_stddev_sing_ld_intl_call_cnt,
       ANY_VALUE(bill_wln_stddev_hsic_usg_gb) AS bill_wln_stddev_hsic_usg_gb,
       ANY_VALUE(bill_wln_stddev_ttv_ppv_cnt) AS bill_wln_stddev_ttv_ppv_cnt,
       ANY_VALUE(bill_wln_stddev_ttv_vod_cnt) AS bill_wln_stddev_ttv_vod_cnt,

       ANY_VALUE(ROUND(CASE WHEN bill_wln_avg_ban_discount_amt * -1 > bill_wln_avg_ban_debit_amt
                            THEN 1
                            WHEN bill_wln_avg_ban_discount_amt > 0
                            THEN 0
                            ELSE SAFE_DIVIDE(bill_wln_avg_ban_discount_amt * -1, bill_wln_avg_ban_debit_amt)
                            END, 2)) as bill_wln_tot_disc_pct,
       ANY_VALUE(ROUND(CASE WHEN bill_wln_avg_sing_discount_amt * -1 > bill_wln_avg_sing_debit_amt
                            THEN 1
                            WHEN bill_wln_avg_sing_discount_amt > 0
                            THEN 0
                            ELSE SAFE_DIVIDE(bill_wln_avg_sing_discount_amt * -1, bill_wln_avg_sing_debit_amt)
                            END, 2)) as bill_wln_sing_disc_pct,
       ANY_VALUE(ROUND(CASE WHEN bill_wln_avg_hsic_discount_amt * -1 > bill_wln_avg_hsic_debit_amt
                            THEN 1
                            WHEN bill_wln_avg_hsic_discount_amt > 0
                            THEN 0
                            ELSE SAFE_DIVIDE(bill_wln_avg_hsic_discount_amt * -1, bill_wln_avg_hsic_debit_amt)
                            END, 2)) as bill_wln_hsic_disc_pct,
       ANY_VALUE(ROUND(CASE WHEN bill_wln_avg_ttv_discount_amt * -1 > bill_wln_avg_ttv_debit_amt
                            THEN 1
                            WHEN bill_wln_avg_ttv_discount_amt > 0
                            THEN 0
                            ELSE SAFE_DIVIDE(bill_wln_avg_ttv_discount_amt * -1, bill_wln_avg_ttv_debit_amt)
                            END, 2)) as bill_wln_ttv_disc_pct,
       ANY_VALUE(ROUND(CASE WHEN bill_wln_avg_smhm_discount_amt * -1 > bill_wln_avg_smhm_debit_amt
                            THEN 1
                            WHEN bill_wln_avg_smhm_discount_amt > 0
                            THEN 0
                            ELSE SAFE_DIVIDE(bill_wln_avg_smhm_discount_amt * -1, bill_wln_avg_smhm_debit_amt)
                            END, 2)) as bill_wln_smhm_disc_pct
  FROM `{master_feature_table}`(_from_dt, _to_dt) AS base
  LEFT JOIN `bi-srv-features-pr-ef5a93.ban_billing.bq_wln_ban_bill_mthly` AS bill
    ON bill.ban = base.ban
   AND bill.part_dt BETWEEN DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 12 MONTH) 
                        AND DATE_SUB(DATE_TRUNC(base.ref_dt, MONTH), INTERVAL 1 MONTH) 
   AND bill.part_dt BETWEEN DATE_SUB(DATE_TRUNC(_from_dt, MONTH), INTERVAL 24 MONTH) 
                        AND DATE_SUB(DATE_TRUNC(_to_dt, MONTH), INTERVAL 1 MONTH)
  LEFT JOIN level1_agg
    ON base.ban = level1_agg.ban
   AND base.ref_dt = level1_agg.ref_dt

 WHERE bill.ban_subtotal_amt > 0 
 GROUP BY 1,2
 
