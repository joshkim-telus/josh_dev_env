
WITH cte_app_usg AS 
(
SELECT base.ref_dt,
       base.ban,
       IF(app_usg.tier_1 = 'news', app_usg.tier_1, REPLACE(app_usg.tier_2, ' ', '_')) AS tier,
       SUM(app_usg_array[SAFE_OFFSET(0)].app_usage_cnt) AS usg_cnt_4w,
       SUM(app_usg_array[SAFE_OFFSET(0)].app_bytes_in) AS usg_dl_4w,
       SUM(app_usg_array[SAFE_OFFSET(0)].app_bytes_out) AS usg_ul_4w 
  FROM `{master_feature_table}`(_from_dt, _to_dt) AS base
  LEFT JOIN `bi-srv-features-pr-ef5a93.ban_base.bq_related_ban_unnested_vw` AS related
    ON related.ban = base.ban
   AND related.ban_src_id = base.ban_src_id
   AND related.rel_ban_src_id = 130
  LEFT JOIN `bi-srv-features-pr-ef5a93.app_usage.bq_ban_imsi_app_cat_usg_daily` AS app_usg
    ON app_usg.ban = related.rel_ban
   AND (
        app_usg.tier_1 = 'news' 
        OR app_usg.tier_2 IN ('sports', 'tv and movies')
       )
   AND app_usg.event_dt BETWEEN DATE_SUB(base.ref_dt, INTERVAL 4 WEEK) AND DATE_SUB(base.ref_dt, INTERVAL 1 DAY)
   AND app_usg.event_dt BETWEEN DATE_SUB(_from_dt, INTERVAL 4 WEEK) AND DATE_SUB(_to_dt, INTERVAL 1 DAY)
 GROUP BY 1, 2, 3
)
SELECT *
  FROM cte_app_usg
 PIVOT (
         AVG(usg_cnt_4w) AS avg_usg_cnt_4w,
         AVG(usg_dl_4w) AS avg_usg_dl_4w,
         AVG(usg_ul_4w) AS avg_usg_ul_4w
         FOR tier IN ('news', 'sports', 'tv_and_movies')
       )
