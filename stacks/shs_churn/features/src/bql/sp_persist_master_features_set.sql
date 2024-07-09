BEGIN
/* 
----------------------------------------------------------------------------------------------------
DESCRIPTION
Persist Master Feature Set.
Take all targets from the from/to date period, left join them with features, and persist into Master Feature Set table.
There can only be 1 set of Training Data per Model Scenario.
There can be multiple sets of Prediction Data per Model Scenario.

INPUT PARAMETERS
- _from_dt DATE: date from which to start the target period
- _to_dt DATE: date to which to end the target period

TARGET TABLE
- {dataset}.master_target_table

SOURCE TABLES
- {dataset}.master_target_table (DRIVING TABLE)
- Features located under {dataset} dataset
----------------------------------------------------------------------------------------------------
*/

  DECLARE _training_mode INT64;

  SET _from_dt = COALESCE(_from_dt, CURRENT_DATE('America/Vancouver')-1);
  SET _to_dt = COALESCE(_to_dt, CURRENT_DATE('America/Vancouver')-1);
  SET _training_mode = IF(_from_dt <> _to_dt, 1, 0);

  ####################################################################################################
  # Persist the features for performance
  ####################################################################################################
  CREATE OR REPLACE TEMPORARY TABLE cust_ban AS 
  SELECT * FROM `{project}.{dataset}.cust_ban_tf`(_from_dt, _to_dt);
  
  CREATE OR REPLACE TEMPORARY TABLE prod_mix AS 
  SELECT * FROM `{project}.{dataset}.prod_mix_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE bill_wln AS 
  SELECT * FROM `{project}.{dataset}.bill_wln_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE tenure AS 
  SELECT * FROM `{project}.{dataset}.tenure_cust_ban_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE demogr AS 
  SELECT * FROM `{project}.{dataset}.demogr_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE clickstream AS 
  SELECT * FROM `{project}.{dataset}.clickstream_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE app_usg AS 
  SELECT * FROM `{project}.{dataset}.app_usg_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE call_centre AS 
  SELECT * FROM `{project}.{dataset}.call_centre_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE hs_usage AS 
  SELECT * FROM `{project}.{dataset}.hs_usage_cust_tf`(_from_dt, _to_dt);

  CREATE OR REPLACE TEMPORARY TABLE shs_features AS 
  SELECT * FROM `{project}.{dataset}.shs_features_cust_tf`(_from_dt, _to_dt);

  ####################################################################################################
  # Join features to base master target table
  ####################################################################################################
  CREATE OR REPLACE TEMPORARY TABLE stage_data AS
  SELECT base.*,
         cust_ban.*      EXCEPT(cust_id, ban, ref_dt),
         prod_mix.*      EXCEPT(ban, ref_dt),
         bill_wln.*      EXCEPT(ban, ref_dt),
         tenure.*        EXCEPT(cust_id, ban, ref_dt),
         demogr.*        EXCEPT(ban, ref_dt),
         clickstream.*   EXCEPT(ban, ref_dt),
         app_usg.*       EXCEPT(ban, ref_dt),
         call_centre.*   EXCEPT(ban, ban_src_id, ref_dt),
         hs_usage.*      EXCEPT(ban, ref_dt),
         shs_features.*  EXCEPT(cust_id, ban, ref_dt),

    FROM `{project}.{dataset}.master_target_table` AS base
    LEFT JOIN cust_ban
      ON base.cust_id = cust_ban.cust_id
     AND base.ban = cust_ban.ban
     AND base.ref_dt = cust_ban.ref_dt
    LEFT JOIN prod_mix 
      ON base.ban = prod_mix.ban
     AND base.ref_dt = prod_mix.ref_dt
    LEFT JOIN bill_wln  
      ON base.ban = bill_wln.ban
     AND base.ref_dt = bill_wln.ref_dt
    LEFT JOIN tenure 
      ON base.cust_id = tenure.cust_id
     AND base.ban = tenure.ban
     AND base.ref_dt = tenure.ref_dt
    LEFT JOIN demogr
      ON base.ban = demogr.ban
     AND base.ref_dt = demogr.ref_dt
    LEFT JOIN clickstream
      ON base.ban = clickstream.ban
     AND base.ref_dt = clickstream.ref_dt
    LEFT JOIN app_usg
      ON base.ban = app_usg.ban
     AND base.ref_dt = app_usg.ref_dt
    LEFT JOIN call_centre
      ON base.ban = call_centre.ban
     AND base.ban_src_id = call_centre.ban_src_id
     AND base.ref_dt = call_centre.ref_dt
    LEFT JOIN hs_usage
      ON base.ban = hs_usage.ban
     AND base.ref_dt = hs_usage.ref_dt
    LEFT JOIN shs_features
      ON base.ban = shs_features.ban
     AND base.ref_dt = shs_features.ref_dt
    WHERE base.from_dt = _from_dt
     AND base.to_dt = _to_dt;

  ####################################################################################################
  # Delete
  ####################################################################################################
   /* for prediction mode, delete only records for the day loaded */
   /* for training mode, delete all records */

  DELETE FROM `{project}.{dataset}.master_features_set`
   WHERE training_mode = _training_mode
     AND (
           (
             _training_mode = 0
             AND from_dt = _from_dt
             AND to_dt = _to_dt
           )
           OR
           (
             _training_mode = 1
           )
         );

  INSERT INTO `{project}.{dataset}.master_features_set`
  (
    from_dt,
    to_dt,
    training_mode,
    split_type,
    prediction_period,
    ref_dt,
    cust_id,
    cust_src_id,
    ban,
    ban_src_id,
    label,
    label_dt,
    -- cust ban
    acct_cr_risk_txt,
    acct_ebill_ind,
    cust_cr_val_txt,
    cust_pref_lang_txt,
    cust_prov_state_cd,
    -- prod mix
    prod_latest_actvn_dt,
    prod_latest_deactvn_dt,
    prod_tot_cnt,
    prod_wln_cnt,
    prod_wls_cnt,
    prod_mob_cnt,
    prod_sing_cnt,
    prod_hsic_cnt,
    prod_whsia_cnt,
    prod_ttv_cnt,
    prod_smhm_cnt,
    prod_tos_cnt,
    prod_wifiplus_cnt,
    prod_stv_cnt,
    prod_other_cnt,
    prod_deact_prod_cnt,
    prod_act_prod_cnt_r7d,
    prod_act_wln_cnt_r7d,
    prod_deact_prod_cnt_r7d,
    prod_deact_wln_cnt_r7d,
    -- bill_wln
    bill_wln_avg_zscore_ban_subtotal_amt,
    bill_wln_avg_ban_subtotal_amt,
    bill_wln_stddev_ban_subtotal_amt,
    bill_wln_avg_zscore_ban_debit_amt,
    bill_wln_avg_ban_debit_amt,
    bill_wln_stddev_ban_debit_amt,
    bill_wln_avg_zscore_ban_discount_amt,
    bill_wln_avg_ban_discount_amt,
    bill_wln_stddev_ban_discount_amt,
    bill_wln_avg_zscore_ban_credit_amt,
    bill_wln_avg_ban_credit_amt,
    bill_wln_stddev_ban_credit_amt,
    bill_wln_avg_zscore_sing_debit_amt,
    bill_wln_avg_zscore_sing_discount_amt,
    bill_wln_avg_zscore_sing_credit_amt,
    bill_wln_avg_zscore_hsic_debit_amt,
    bill_wln_avg_zscore_hsic_discount_amt,
    bill_wln_avg_zscore_hsic_credit_amt,
    bill_wln_avg_zscore_ttv_debit_amt,
    bill_wln_avg_zscore_ttv_discount_amt,
    bill_wln_avg_zscore_ttv_credit_amt,
    bill_wln_avg_zscore_smhm_debit_amt,
    bill_wln_avg_zscore_smhm_discount_amt,
    bill_wln_avg_zscore_smhm_credit_amt,
    bill_wln_avg_zscore_sing_ld_na_call_cnt,
    bill_wln_avg_zscore_sing_ld_intl_call_cnt,
    bill_wln_avg_zscore_hsic_usg_gb,
    bill_wln_avg_zscore_ttv_ppv_cnt,
    bill_wln_avg_zscore_ttv_vod_cnt,
    bill_wln_avg_sing_debit_amt,
    bill_wln_avg_sing_discount_amt,
    bill_wln_avg_sing_credit_amt,
    bill_wln_avg_hsic_debit_amt,
    bill_wln_avg_hsic_discount_amt,
    bill_wln_avg_hsic_credit_amt,
    bill_wln_avg_ttv_debit_amt,
    bill_wln_avg_ttv_discount_amt,
    bill_wln_avg_ttv_credit_amt,
    bill_wln_avg_smhm_debit_amt,
    bill_wln_avg_smhm_discount_amt,
    bill_wln_avg_smhm_credit_amt,
    bill_wln_avg_sing_ld_na_call_cnt,
    bill_wln_avg_sing_ld_intl_call_cnt,
    bill_wln_avg_hsic_usg_gb,
    bill_wln_avg_ttv_ppv_cnt,
    bill_wln_avg_ttv_vod_cnt,
    bill_wln_stddev_sing_debit_amt,
    bill_wln_stddev_sing_discount_amt,
    bill_wln_stddev_sing_credit_amt,
    bill_wln_stddev_hsic_debit_amt,
    bill_wln_stddev_hsic_discount_amt,
    bill_wln_stddev_hsic_credit_amt,
    bill_wln_stddev_ttv_debit_amt,
    bill_wln_stddev_ttv_discount_amt,
    bill_wln_stddev_ttv_credit_amt,
    bill_wln_stddev_smhm_debit_amt,
    bill_wln_stddev_smhm_discount_amt,
    bill_wln_stddev_smhm_credit_amt,
    bill_wln_stddev_sing_ld_na_call_cnt,
    bill_wln_stddev_sing_ld_intl_call_cnt,
    bill_wln_stddev_hsic_usg_gb,
    bill_wln_stddev_ttv_ppv_cnt,
    bill_wln_stddev_ttv_vod_cnt,
    bill_wln_tot_disc_pct,
    bill_wln_sing_disc_pct,
    bill_wln_hsic_disc_pct,
    bill_wln_ttv_disc_pct,
    bill_wln_smhm_disc_pct,
    -- tenure
    hsic_tenure_days,
    contract_end_date_hsic,
    sing_tenure_days,
    contract_end_date_sing,
    ttv_tenure_days,
    contract_end_date_ttv,
    smhm_tenure_days,
    contract_end_date_smhm,
    ffh_tenure,
    new_account_ind,
    -- demographics
    demogr_med_age,
    demogr_avg_child,
    demogr_pct_family_with_child_living_at_home,
    demogr_employment_rate,
    demogr_avg_household_size,
    demogr_avg_income,
    demogr_med_income,
    demogr_urban_flag,
    demogr_rural_flag,
    demogr_family_flag,
    demogr_lsname_large_diverse_families,
    demogr_lsname_younger_singles_and_couples,
    demogr_lsname_very_young_singles_and_couples,
    demogr_lsname_older_families_and_empty_nests,
    demogr_lsname_middle_age_families,
    demogr_lsname_mature_singles_and_couples,
    demogr_lsname_young_families,
    demogr_lsname_school_age_families,
    demogr_retired_pstl_cd_ind,
    demogr_census_division_typ,
    demogr_lifestage_sort,
    -- clickstream
    clk_wls_tot_cnt_r30d,
    clk_wls_plan_cnt_r30d,
    clk_wls_device_cnt_r30d,
    clk_wls_smartwatch_cnt_r30d,
    clk_wls_tablet_cnt_r30d,
    clk_wln_tot_cnt_r30d,
    clk_wln_eligibility_cnt_r30d,
    clk_wln_sing_cnt_r30d,
    clk_wln_hsic_cnt_r30d,
    clk_wln_fibre_cnt_r30d,
    clk_wln_whsia_cnt_r30d,
    clk_wln_wifi_plus_cnt_r30d,
    clk_wln_tv_cnt_r30d,
    clk_wln_optik_cnt_r30d,
    clk_wln_pik_cnt_r30d,
    clk_wln_streamplus_cnt_r30d,
    clk_wln_streaming_cnt_r30d,
    clk_wln_security_cnt_r30d,
    clk_wln_smarthome_security_cnt_r30d,
    clk_wln_online_security_cnt_r30d,
    clk_wln_smartwear_security_cnt_r30d,
    clk_deal_tot_cnt_r30d,
    clk_deal_wls_cnt_r30d,
    clk_deal_wln_cnt_r30d,
    clk_deal_wln_sing_cnt_r30d,
    clk_deal_wln_hsic_cnt_r30d,
    clk_deal_wln_whsia_cnt_r30d,
    clk_deal_wln_tv_cnt_r30d,
    clk_deal_wln_security_cnt_r30d,
    clk_upgr_tot_cnt_r30d,
    clk_upgr_wls_cnt_r30d,
    clk_upgr_wln_cnt_r30d,
    clk_upgr_wln_sing_cnt_r30d,
    clk_upgr_wln_hsic_cnt_r30d,
    clk_upgr_wln_whsia_cnt_r30d,
    clk_upgr_wln_tv_cnt_r30d,
    clk_upgr_wln_security_cnt_r30d,
    clk_chg_tot_cnt_r30d,
    clk_chg_wls_cnt_r30d,
    clk_chg_wln_cnt_r30d,
    clk_chg_wln_sing_cnt_r30d,
    clk_chg_wln_hsic_cnt_r30d,
    clk_chg_wln_whsia_cnt_r30d,
    clk_chg_wln_tv_cnt_r30d,
    clk_chg_wln_security_cnt_r30d,
    clk_health_livingwell_cnt_r30d,
    clk_health_mypet_cnt_r30d,
    clk_travel_cnt_r30d,
    clk_billing_cnt_r30d,
    clk_service_agreement_cnt_r30d,
    -- app usage
    avg_usg_cnt_4w_news,
    avg_usg_dl_4w_news,
    avg_usg_ul_4w_news,
    avg_usg_cnt_4w_sports,
    avg_usg_dl_4w_sports,
    avg_usg_ul_4w_sports,
    avg_usg_cnt_4w_tv_and_movies,
    avg_usg_dl_4w_tv_and_movies,
    avg_usg_ul_4w_tv_and_movies,
    -- callcentre
    call_cnt,
    call_avg_talk_time,
    call_avg_hold_time,
    call_avg_emp_cnt,
    call_avg_esc_cnt,
    call_std_talk_time,
    call_std_hold_time,
    call_std_emp_cnt,
    call_std_esc_cnt,
    call_sum_talk_time,
    call_sum_hold_time,
    call_sum_emp_cnt,
    call_sum_esc_cnt,
    call_max_talk_time,
    call_max_hold_time,
    call_max_emp_cnt,
    call_max_esc_cnt, 
    -- hs usage
    hs_usg_avg_tot_gb,
    hs_usg_avg_dl_gb,
    hs_usg_avg_ul_gb, 
    -- shs features
    chnl_org_type_cd,
    chnl_org_type_txt,
    tenure_days,
    tenure_months,
    tenure_months_groups,
    commitment_type_cd,
    days_until_expiry,
    contract_expiring_soon,
    credit_value_cd,
    municipality_nm,
    price_plan_rate_amt,
    province_state_cd,
    self_install_ind,
    shs_tos_bundle_ind,
    term_length_num,
    term_txt,
    total_mrc_amt,
    total_mrc_core_amt,
    total_mrc_disc_amt,
    total_mrc_extra_amt
  )
  SELECT
    from_dt,
    to_dt,
    training_mode,
    split_type,
    prediction_period,
    ref_dt,
    cust_id,
    cust_src_id,
    ban,
    ban_src_id,
    label,
    label_dt,
    -- cust ban
    acct_cr_risk_txt,
    acct_ebill_ind,
    cust_cr_val_txt,
    cust_pref_lang_txt,
    cust_prov_state_cd,
    -- prod mix
    prod_latest_actvn_dt,
    prod_latest_deactvn_dt,
    prod_tot_cnt,
    prod_wln_cnt,
    prod_wls_cnt,
    prod_mob_cnt,
    prod_sing_cnt,
    prod_hsic_cnt,
    prod_whsia_cnt,
    prod_ttv_cnt,
    prod_smhm_cnt,
    prod_tos_cnt,
    prod_wifiplus_cnt,
    prod_stv_cnt,
    prod_other_cnt,
    prod_deact_prod_cnt,
    prod_act_prod_cnt_r7d,
    prod_act_wln_cnt_r7d,
    prod_deact_prod_cnt_r7d,
    prod_deact_wln_cnt_r7d,
    -- bill_wln
    bill_wln_avg_zscore_ban_subtotal_amt,
    bill_wln_avg_ban_subtotal_amt,
    bill_wln_stddev_ban_subtotal_amt,
    bill_wln_avg_zscore_ban_debit_amt,
    bill_wln_avg_ban_debit_amt,
    bill_wln_stddev_ban_debit_amt,
    bill_wln_avg_zscore_ban_discount_amt,
    bill_wln_avg_ban_discount_amt,
    bill_wln_stddev_ban_discount_amt,
    bill_wln_avg_zscore_ban_credit_amt,
    bill_wln_avg_ban_credit_amt,
    bill_wln_stddev_ban_credit_amt,
    bill_wln_avg_zscore_sing_debit_amt,
    bill_wln_avg_zscore_sing_discount_amt,
    bill_wln_avg_zscore_sing_credit_amt,
    bill_wln_avg_zscore_hsic_debit_amt,
    bill_wln_avg_zscore_hsic_discount_amt,
    bill_wln_avg_zscore_hsic_credit_amt,
    bill_wln_avg_zscore_ttv_debit_amt,
    bill_wln_avg_zscore_ttv_discount_amt,
    bill_wln_avg_zscore_ttv_credit_amt,
    bill_wln_avg_zscore_smhm_debit_amt,
    bill_wln_avg_zscore_smhm_discount_amt,
    bill_wln_avg_zscore_smhm_credit_amt,
    bill_wln_avg_zscore_sing_ld_na_call_cnt,
    bill_wln_avg_zscore_sing_ld_intl_call_cnt,
    bill_wln_avg_zscore_hsic_usg_gb,
    bill_wln_avg_zscore_ttv_ppv_cnt,
    bill_wln_avg_zscore_ttv_vod_cnt,
    bill_wln_avg_sing_debit_amt,
    bill_wln_avg_sing_discount_amt,
    bill_wln_avg_sing_credit_amt,
    bill_wln_avg_hsic_debit_amt,
    bill_wln_avg_hsic_discount_amt,
    bill_wln_avg_hsic_credit_amt,
    bill_wln_avg_ttv_debit_amt,
    bill_wln_avg_ttv_discount_amt,
    bill_wln_avg_ttv_credit_amt,
    bill_wln_avg_smhm_debit_amt,
    bill_wln_avg_smhm_discount_amt,
    bill_wln_avg_smhm_credit_amt,
    bill_wln_avg_sing_ld_na_call_cnt,
    bill_wln_avg_sing_ld_intl_call_cnt,
    bill_wln_avg_hsic_usg_gb,
    bill_wln_avg_ttv_ppv_cnt,
    bill_wln_avg_ttv_vod_cnt,
    bill_wln_stddev_sing_debit_amt,
    bill_wln_stddev_sing_discount_amt,
    bill_wln_stddev_sing_credit_amt,
    bill_wln_stddev_hsic_debit_amt,
    bill_wln_stddev_hsic_discount_amt,
    bill_wln_stddev_hsic_credit_amt,
    bill_wln_stddev_ttv_debit_amt,
    bill_wln_stddev_ttv_discount_amt,
    bill_wln_stddev_ttv_credit_amt,
    bill_wln_stddev_smhm_debit_amt,
    bill_wln_stddev_smhm_discount_amt,
    bill_wln_stddev_smhm_credit_amt,
    bill_wln_stddev_sing_ld_na_call_cnt,
    bill_wln_stddev_sing_ld_intl_call_cnt,
    bill_wln_stddev_hsic_usg_gb,
    bill_wln_stddev_ttv_ppv_cnt,
    bill_wln_stddev_ttv_vod_cnt,
    bill_wln_tot_disc_pct,
    bill_wln_sing_disc_pct,
    bill_wln_hsic_disc_pct,
    bill_wln_ttv_disc_pct,
    bill_wln_smhm_disc_pct,
    -- tenure
    hsic_tenure_days,
    contract_end_date_hsic,
    sing_tenure_days,
    contract_end_date_sing,
    ttv_tenure_days,
    contract_end_date_ttv,
    smhm_tenure_days,
    contract_end_date_smhm,
    ffh_tenure,
    new_account_ind, 
    -- demographics
    demogr_med_age,
    demogr_avg_child,
    demogr_pct_family_with_child_living_at_home,
    demogr_employment_rate,
    demogr_avg_household_size,
    demogr_avg_income,
    demogr_med_income,
    demogr_urban_flag,
    demogr_rural_flag,
    demogr_family_flag,
    demogr_lsname_large_diverse_families,
    demogr_lsname_younger_singles_and_couples,
    demogr_lsname_very_young_singles_and_couples,
    demogr_lsname_older_families_and_empty_nests,
    demogr_lsname_middle_age_families,
    demogr_lsname_mature_singles_and_couples,
    demogr_lsname_young_families,
    demogr_lsname_school_age_families,
    demogr_retired_pstl_cd_ind,
    demogr_census_division_typ,
    demogr_lifestage_sort,
    -- clickstream
    clk_wls_tot_cnt_r30d,
    clk_wls_plan_cnt_r30d,
    clk_wls_device_cnt_r30d,
    clk_wls_smartwatch_cnt_r30d,
    clk_wls_tablet_cnt_r30d,
    clk_wln_tot_cnt_r30d,
    clk_wln_eligibility_cnt_r30d,
    clk_wln_sing_cnt_r30d,
    clk_wln_hsic_cnt_r30d,
    clk_wln_fibre_cnt_r30d,
    clk_wln_whsia_cnt_r30d,
    clk_wln_wifi_plus_cnt_r30d,
    clk_wln_tv_cnt_r30d,
    clk_wln_optik_cnt_r30d,
    clk_wln_pik_cnt_r30d,
    clk_wln_streamplus_cnt_r30d,
    clk_wln_streaming_cnt_r30d,
    clk_wln_security_cnt_r30d,
    clk_wln_smarthome_security_cnt_r30d,
    clk_wln_online_security_cnt_r30d,
    clk_wln_smartwear_security_cnt_r30d,
    clk_deal_tot_cnt_r30d,
    clk_deal_wls_cnt_r30d,
    clk_deal_wln_cnt_r30d,
    clk_deal_wln_sing_cnt_r30d,
    clk_deal_wln_hsic_cnt_r30d,
    clk_deal_wln_whsia_cnt_r30d,
    clk_deal_wln_tv_cnt_r30d,
    clk_deal_wln_security_cnt_r30d,
    clk_upgr_tot_cnt_r30d,
    clk_upgr_wls_cnt_r30d,
    clk_upgr_wln_cnt_r30d,
    clk_upgr_wln_sing_cnt_r30d,
    clk_upgr_wln_hsic_cnt_r30d,
    clk_upgr_wln_whsia_cnt_r30d,
    clk_upgr_wln_tv_cnt_r30d,
    clk_upgr_wln_security_cnt_r30d,
    clk_chg_tot_cnt_r30d,
    clk_chg_wls_cnt_r30d,
    clk_chg_wln_cnt_r30d,
    clk_chg_wln_sing_cnt_r30d,
    clk_chg_wln_hsic_cnt_r30d,
    clk_chg_wln_whsia_cnt_r30d,
    clk_chg_wln_tv_cnt_r30d,
    clk_chg_wln_security_cnt_r30d,
    clk_health_livingwell_cnt_r30d,
    clk_health_mypet_cnt_r30d,
    clk_travel_cnt_r30d,
    clk_billing_cnt_r30d,
    clk_service_agreement_cnt_r30d,
    -- app usage
    avg_usg_cnt_4w_news,
    avg_usg_dl_4w_news,
    avg_usg_ul_4w_news,
    avg_usg_cnt_4w_sports,
    avg_usg_dl_4w_sports,
    avg_usg_ul_4w_sports,
    avg_usg_cnt_4w_tv_and_movies,
    avg_usg_dl_4w_tv_and_movies,
    avg_usg_ul_4w_tv_and_movies,
    -- callcentre
    call_cnt,
    call_avg_talk_time,
    call_avg_hold_time,
    call_avg_emp_cnt,
    call_avg_esc_cnt,
    call_std_talk_time,
    call_std_hold_time,
    call_std_emp_cnt,
    call_std_esc_cnt,
    call_sum_talk_time,
    call_sum_hold_time,
    call_sum_emp_cnt,
    call_sum_esc_cnt,
    call_max_talk_time,
    call_max_hold_time,
    call_max_emp_cnt,
    call_max_esc_cnt, 
    -- hs usage
    hs_usg_avg_tot_gb,
    hs_usg_avg_dl_gb,
    hs_usg_avg_ul_gb,
        -- shs features
    chnl_org_type_cd,
    chnl_org_type_txt,
    tenure_days,
    tenure_months,
    tenure_months_groups,
    commitment_type_cd,
    days_until_expiry,
    contract_expiring_soon,
    credit_value_cd,
    municipality_nm,
    price_plan_rate_amt,
    province_state_cd,
    self_install_ind,
    shs_tos_bundle_ind,
    term_length_num,
    term_txt,
    total_mrc_amt,
    total_mrc_core_amt,
    total_mrc_disc_amt,
    total_mrc_extra_amt
  FROM stage_data;

END