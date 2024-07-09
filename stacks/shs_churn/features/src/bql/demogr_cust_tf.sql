
SELECT base.ref_dt,
       base.ban,
       prizm.lifestage_nm AS demo_lsname, 
       prizm.social_grp_nm AS demo_sgname, 
       ROUND(AVG(demostats.ECYPTAMED), 1) AS demogr_med_age,
       ROUND(AVG(demostats.ECYCHAFHCH), 1) AS demogr_avg_child,
       ROUND(AVG(demostats.ECYHFSWC), 1) AS demogr_pct_family_with_child_living_at_home,
       ROUND(AVG(demostats.ECYACTER), 1) AS demogr_employment_rate,
       ROUND(AVG(demostats.ECYHSZAVG), 1) AS demogr_avg_household_size,
       ROUND(AVG(demostats.ECYHNIAVG), 0) AS demogr_avg_income,
       ROUND(AVG(demostats.ECYHNIMED), 0) AS demogr_med_income,
       ROUND(AVG(IF(LOWER(prizm.social_grp_nm) LIKE "%urban%", 1, 0)), 1) AS demogr_urban_flag,
       ROUND(AVG(IF(LOWER(prizm.social_grp_nm) LIKE "%rural%", 1, 0)), 1) AS demogr_rural_flag,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) LIKE "%families%", 1, 0)), 1) AS demogr_family_flag,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'large diverse families', 1, 0)), 1) AS demogr_lsname_large_diverse_families,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'younger singles & couples', 1, 0)), 1) AS demogr_lsname_younger_singles_and_couples,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'very young singles & couples', 1, 0)), 1) AS demogr_lsname_very_young_singles_and_couples,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'older families & empty nests', 1, 0)), 1) AS demogr_lsname_older_families_and_empty_nests,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'middle-age families', 1, 0)), 1) AS demogr_lsname_middle_age_families,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'mature singles & couples', 1, 0)), 1) AS demogr_lsname_mature_singles_and_couples,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'young families', 1, 0)), 1) AS demogr_lsname_young_families,
       ROUND(AVG(IF(LOWER(prizm.lifestage_nm) = 'school-age families', 1, 0)), 1) AS demogr_lsname_school_age_families,
       ROUND(AVG(prizm.retired_pstl_cd_ind), 1) AS demogr_retired_pstl_cd_ind,
       ANY_VALUE(prizm.census_division_typ) AS demogr_census_division_typ,
       ROUND(AVG(prizm.lifestage_sort), 1) AS demogr_lifestage_sort, 
  FROM `{master_feature_table}`(_from_dt, _to_dt) AS base
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_billing_account_dim` AS acct
    ON acct.bacct_num = CAST(base.ban AS STRING)
   AND acct.bacct_src_id = base.ban_src_id
   AND acct.current_ind = 1
  LEFT JOIN `bi-srv-features-pr-ef5a93.pstl_demogr.bq_pstl_demogr_prizm_vw` AS prizm
    ON REPLACE(TRIM(acct.pstl_cd), ' ', '') = prizm.pstl_cd
  LEFT JOIN `bi-srv-features-pr-ef5a93.util_ref_table.demostats2023_fsaldu`  AS demostats -- DemoStats 2023 DATA; requested from Emily Silver and uploaded to GCS manually
    ON REPLACE(TRIM(acct.pstl_cd), ' ', '') = demostats.CODE
 GROUP BY 1,2,3,4
 