CREATE OR REPLACE TABLE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.clickstream_cust_tf`(_from_dt DATE, _to_dt DATE) AS 

(
  
SELECT base.ref_dt,
       base.ban,
       SUM(clk.wls_tot_cnt) AS clk_wls_tot_cnt_r30d,
       SUM(clk.wls_plan_cnt) AS clk_wls_plan_cnt_r30d,
       SUM(clk.wls_device_cnt) AS clk_wls_device_cnt_r30d,
       SUM(clk.wls_smartwatch_cnt) AS clk_wls_smartwatch_cnt_r30d,
       SUM(clk.wls_tablet_cnt) AS clk_wls_tablet_cnt_r30d,
       SUM(clk.wln_tot_cnt) AS clk_wln_tot_cnt_r30d,
       SUM(clk.wln_eligibility_cnt) AS clk_wln_eligibility_cnt_r30d,
       SUM(clk.wln_sing_cnt) AS clk_wln_sing_cnt_r30d,
       SUM(clk.wln_hsic_cnt) AS clk_wln_hsic_cnt_r30d,
       SUM(clk.wln_fibre_cnt) AS clk_wln_fibre_cnt_r30d,
       SUM(clk.wln_whsia_cnt) AS clk_wln_whsia_cnt_r30d,
       SUM(clk.wln_wifi_plus_cnt) AS clk_wln_wifi_plus_cnt_r30d,
       SUM(clk.wln_tv_cnt) AS clk_wln_tv_cnt_r30d,
       SUM(clk.wln_optik_cnt) AS clk_wln_optik_cnt_r30d,
       SUM(clk.wln_pik_cnt) AS clk_wln_pik_cnt_r30d,
       SUM(clk.wln_streamplus_cnt) AS clk_wln_streamplus_cnt_r30d,
       SUM(clk.wln_streaming_cnt) AS clk_wln_streaming_cnt_r30d,
       SUM(clk.wln_security_cnt) AS clk_wln_security_cnt_r30d,
       SUM(clk.wln_smarthome_security_cnt) AS clk_wln_smarthome_security_cnt_r30d,
       SUM(clk.wln_online_security_cnt) AS clk_wln_online_security_cnt_r30d,
       SUM(clk.wln_smartwear_security_cnt) AS clk_wln_smartwear_security_cnt_r30d,
       SUM(clk.deal_tot_cnt) AS clk_deal_tot_cnt_r30d,
       SUM(clk.deal_wls_cnt) AS clk_deal_wls_cnt_r30d,
       SUM(clk.deal_wln_cnt) AS clk_deal_wln_cnt_r30d,
       SUM(clk.deal_wln_sing_cnt) AS clk_deal_wln_sing_cnt_r30d,
       SUM(clk.deal_wln_hsic_cnt) AS clk_deal_wln_hsic_cnt_r30d,
       SUM(clk.deal_wln_whsia_cnt) AS clk_deal_wln_whsia_cnt_r30d,
       SUM(clk.deal_wln_tv_cnt) AS clk_deal_wln_tv_cnt_r30d,
       SUM(clk.deal_wln_security_cnt) AS clk_deal_wln_security_cnt_r30d,
       SUM(clk.upgr_tot_cnt) AS clk_upgr_tot_cnt_r30d,
       SUM(clk.upgr_wls_cnt) AS clk_upgr_wls_cnt_r30d,
       SUM(clk.upgr_wln_cnt) AS clk_upgr_wln_cnt_r30d,
       SUM(clk.upgr_wln_sing_cnt) AS clk_upgr_wln_sing_cnt_r30d,
       SUM(clk.upgr_wln_hsic_cnt) AS clk_upgr_wln_hsic_cnt_r30d,
       SUM(clk.upgr_wln_whsia_cnt) AS clk_upgr_wln_whsia_cnt_r30d,
       SUM(clk.upgr_wln_tv_cnt) AS clk_upgr_wln_tv_cnt_r30d,
       SUM(clk.upgr_wln_security_cnt) AS clk_upgr_wln_security_cnt_r30d,
       SUM(clk.chg_tot_cnt) AS clk_chg_tot_cnt_r30d,
       SUM(clk.chg_wls_cnt) AS clk_chg_wls_cnt_r30d,
       SUM(clk.chg_wln_cnt) AS clk_chg_wln_cnt_r30d,
       SUM(clk.chg_wln_sing_cnt) AS clk_chg_wln_sing_cnt_r30d,
       SUM(clk.chg_wln_hsic_cnt) AS clk_chg_wln_hsic_cnt_r30d,
       SUM(clk.chg_wln_whsia_cnt) AS clk_chg_wln_whsia_cnt_r30d,
       SUM(clk.chg_wln_tv_cnt) AS clk_chg_wln_tv_cnt_r30d,
       SUM(clk.chg_wln_security_cnt) AS clk_chg_wln_security_cnt_r30d,
       SUM(clk.health_livingwell_cnt) AS clk_health_livingwell_cnt_r30d,
       SUM(clk.health_mypet_cnt) AS clk_health_mypet_cnt_r30d,
       SUM(clk.travel_cnt) AS clk_travel_cnt_r30d,
       SUM(clk.billing_cnt) AS clk_billing_cnt_r30d,
       SUM(clk.service_agreement_cnt) AS clk_service_agreement_cnt_r30d,
  FROM `divg-groovyhoon-pr-d2eab4.shs_churn.features_base_cust_ban_tf`(_from_dt, _to_dt) AS base
  LEFT JOIN `bi-srv-features-pr-ef5a93.ban_clckstrm.bq_ban_clckstrm_telus` AS clk
    ON clk.ban = base.ban
   AND clk.ban_src_id = base.ban_src_id
   AND clk.part_dt BETWEEN DATE_SUB(base.ref_dt, INTERVAL 60 DAY) AND DATE_SUB(base.ref_dt, INTERVAL 1 DAY)
   AND clk.part_dt BETWEEN DATE_SUB(_from_dt, INTERVAL 60 DAY) AND DATE_SUB(_to_dt, INTERVAL 1 DAY)
  GROUP BY 1, 2
   
)