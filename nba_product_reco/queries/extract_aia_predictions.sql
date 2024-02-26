select
  lpds_id,
  ban,
  cust_id,
  ref_dt,
  split_type,
  -- predictions
  lwc_predicted_score_calibrated,
  hsic_predicted_score_calibrated,
  hpro_predicted_score_calibrated,
  shs_predicted_score_calibrated,
  sing_predicted_score_calibrated,
  ttv_predicted_score_calibrated,
  sws_predicted_score_calibrated,
  tos_predicted_score_calibrated,
  whsia_predicted_score_calibrated,
  wifi_predicted_score_calibrated,
  -- labels
  hpro_label,
  hsic_label,
  lwc_label,
  shs_label,
  sing_label,
  sws_label,
  tos_label,
  ttv_label,
  whsia_label,
  wifi_label


from `wb-nba-1-pr-08a45b.wb_nba_1_pr_dataset.blended_features_10_prods_with_labels` 

  where split_type = '3-test'

    and greatest(
      coalesce(hpro_label, 0),
      coalesce(hsic_label, 0),
      coalesce(lwc_label, 0),
      coalesce(shs_label, 0),
      coalesce(sing_label, 0),
      coalesce(sws_label, 0),
      coalesce(tos_label, 0),
      coalesce(ttv_label, 0),
      coalesce(whsia_label, 0),
      coalesce(wifi_label, 0)
    ) > 0
