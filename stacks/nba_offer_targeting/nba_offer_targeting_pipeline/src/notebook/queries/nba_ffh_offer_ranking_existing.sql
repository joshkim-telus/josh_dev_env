

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_nba_ffh_offer_ranking_existing()

-- This stored procedure pulls the list of customers at cust_id + ban + lpds_id level who are eligible for Casa IRPC Offers

BEGIN

	CREATE OR REPLACE TABLE nba_offer_targeting.nba_ffh_offer_ranking_existing AS 

	with nba_ranking as (
		select cust_id, lpds_id, rank as nba_ranking,
		case 
		when reco = 'hsic' then 'Cross-sell'  
		when reco = 'ht' then 'Cross-sell' 
		when reco = 'hs' then 'Cross-sell' 
		when reco = 'hts' then 'Cross-sell' 
		when reco = 'ts' then 'Cross-sell' 
		when reco = 'shs' then 'Cross-sell' 
		when reco = 'tv' then 'Cross-sell' 
		when reco = 'sws' then 'Cross-sell' 
		when reco = 'tos_c' then 'Cross-sell' 
		when reco = 'tos_u' then 'Cross-sell' 
		when reco = 'lwc' then 'Cross-sell' 
		when reco = 'hp' then 'Cross-sell' 
		when reco = 'wfp' then 'Upsell' -- This offer is technically a Cross Sell offer, but it's labelled as an Upsell offer in the Offer Targeting Parameters Sheet
		when reco = 'whsia' then 'Cross-sell' 
		when reco = 'hpro' then 'Cross-sell' 
		when reco = 'mob' then 'Cross-sell' 
		when reco = 'rn_shs' then 'Renewal'
		when reco = 'up_hsia' then 'Upsell'
		when reco = 'up_tv' then 'Upsell' 
		when reco = 'hsic_1' then 'Cross-sell'
		when reco = 'hsic_2' then 'Cross-sell'
		when reco = 'hsic_3' then 'Cross-sell'
		when reco = 'tos_1' then 'Cross-sell'
		when reco = 'tos_2' then 'Cross-sell'
		when reco = 'tos_3' then 'Cross-sell'
		when reco = 'tos_4' then 'Cross-sell'
		when reco = 'rn_tos' then 'Renewal' 
		when reco = 'stmp' then 'Cross-sell'
		end as category,

		case 
		when reco = 'hsic' then 'Internet'  
		when reco = 'ht' then 'Internet + Optik' 
		when reco = 'hs' then 'Internet + SHS' 
		when reco = 'hts' then 'Internet + Optik + SHS' 
		when reco = 'ts' then 'Optik + SHS' 
		when reco = 'shs' then 'SHS' 
		when reco = 'tv' then 'Optik' 
		when reco = 'sws' then 'SWS' 
		when reco = 'tos_c' then 'TOS - Complete' 
		when reco = 'tos_u' then 'TOS - Ultimate'
		when reco = 'lwc' then 'LWC' 
		when reco = 'hp' then 'HP' 
		when reco = 'wfp' then 'Wi-Fi Plus' 
		when reco = 'whsia' then 'WHSIA' 
		when reco = 'hpro' then 'HPRO' 
		when reco = 'mob' then 'Mobility' 
		when reco = 'rn_shs' then 'SHS'
		when reco = 'up_hsia' then 'Internet'
		when reco = 'up_tv' then 'Optik' 
		when reco = 'hsic_1' then 'HSIC - Low Tier'
		when reco = 'hsic_2' then 'HSIC - Medium Tier'
		when reco = 'hsic_3' then 'HSIC - High Tier'
		when reco = 'tos_1' then 'TOS - Basic'
		when reco = 'tos_2' then 'TOS - Standard'
		when reco = 'tos_3' then 'TOS - Ultimate'
		when reco = 'tos_4' then 'TOS - Complete'
		when reco = 'rn_tos' then 'TOS' 
		when reco = 'stmp' then 'STMP'
		end as subcategory

		-- from `cdo-dse-workspace-np-45d0d5.development_weibo.existing_reco`
		-- from `bi-srv-mobilityds-pr-80a48d.telus_ffh_nba.bq_product_recommendation_ranked_vw`
		from nba_offer_targeting.nba_ffh_model_scores_existing
	),

	casa as (
		-- Home Solutions customers in casa that called Telus in the last 90 days that dealt with the offer (or eligible for the offers)
		SELECT  
		evar247_txt AS ban, 
		REPLACE (eVar223_txt,':',',') AS offer_id,
		CASE WHEN eVar224_txt = 'Not interested' or eVar224_txt = 'Pas intéresséd' THEN 1 ELSE 0 END AS not_interested_casa,
		CASE WHEN eVar224_txt = 'Highly interested' or eVar224_txt = 'Très intéressé' THEN 1 ELSE 0 END AS highly_interested_casa

		FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_casa_event` casa
		WHERE DATE_DIFF(current_date(), DATE(casa_event_ts), DAY) <= 90
		AND eVar224_txt IS NOT NULL 
		AND evar247_txt IS NOT NULL 
		AND (eVar223_txt = '1160203' OR CHAR_LENGTH(eVar223_txt) >= 19) 
		AND  evar131_txt = 'Home Solutions'
	),

	digital as (
		-- Home Solutions customers that visited Telus digital in the last 90 days that shown interests in the offers (or eligible for the offers)
		SELECT t1.clickstream_event_date,
		REPLACE(t1.offer_id,':',',') AS offer_id,
		t1.un_bus_cust_id AS cust_id,
		CASE WHEN 
		((offer_click_ind != TRUE OR offer_click_ind IS NULL) AND (offer_conversion_ind != TRUE OR offer_conversion_ind IS NULL) AND DATE_DIFF(CURRENT_DATE(), DATE(clickstream_event_date), DAY) > 7)
		OR
		(offer_click_ind = TRUE AND (offer_conversion_ind != TRUE OR offer_conversion_ind IS NULL) AND DATE_DIFF(CURRENT_DATE(), DATE(clickstream_event_date), DAY) > 3)
		THEN 1 ELSE 0 END AS not_interested_digital

		FROM  `dq-build-dsc-dev-682e3a.dq_workspace.nba_report_r5` t1
		LEFT JOIN `dq-build-dsc-dev-682e3a.dq_workspace.nba_offers_tbl` t3
		ON CAST(t1.offer_id AS STRING)=CAST(t3.Promo_IDs AS STRING)

		WHERE
		t1.offer_type = 'home solutions'
		AND un_bus_cust_id IS NOT NULL
		AND LENGTH(t1.offer_id)>=19 
		AND DATE_DIFF(CURRENT_DATE(), t1.clickstream_event_date, DAY) <= 90 
		AND (offer_impression_ind IS NOT NULL OR offer_click_ind IS NOT NULL OR offer_conversion_ind IS NOT NULL)
	), 

	ranking1 as (
		select distinct a.cust_id
		, a.bacct_num
		, a.lpds_id
		, a.candate
		, a.Category
		, a.Subcategory
		, a.digital_category
		, a.promo_seg
		, a.offer_code
		, a.ASSMT_VALID_START_TS
		, a.ASSMT_VALID_END_TS
		, a.rk
		, CAST(1 + RAND() * 10 AS INT64) as rk2
		, b.nba_ranking
		, c.not_interested_casa
		, c.highly_interested_casa
		, d.not_interested_digital
		, e.offer_ranking
		from nba_offer_targeting.qua_base_hs 
		left join nba_ranking b
		on a.cust_id = b.cust_id  and a.lpds_id = cast(b.lpds_id as int64) and trim(a.Category) = trim(b.category) and trim(a.Subcategory) = trim(b.subcategory)
		left join casa c
		on a.bacct_num = cast(c.ban as int) and a.offer_code = c.offer_id
		left join digital d
		on a.cust_id = cast(d.cust_id as int) and  a.offer_code = d.offer_id
		left join nba_offer_targeting.offer_ranking e
		on trim(a.Category) = trim(e.Category) and trim(a.Subcategory) = trim(e.Subcategory)
	), 

	ranking2 as (
		select * 
		, row_number() over (partition by cust_id, lpds_id, category, subcategory order by rk2) as row_num
		, case 
		when nba_ranking is null then 99
		when Category = 'Digital Renewal' and rk in (10,11,20,21) then 0.5
		when Category = 'Digital Renewal' and rk in (30,31) then 991
		-- when Category = 'Digital Renewal' and both =1 and rk in (11,21,31) then 99999
		when highly_interested_casa = 1 then 0
		when not_interested_casa = 1 or  not_interested_digital = 1 then 999
		when not_interested_casa = 1 and  not_interested_digital = 1 then 9999
		else nba_ranking
		end 
		as ad_nba_ranking
		from ranking1
	)

	select cust_id
	, null as mobility_ban
	, lpds_id
	, promo_seg
	, offer_code
	, assmt_valid_start_ts
	, assmt_valid_end_ts
	, rank() over (partition by cust_id, lpds_id order by ad_nba_ranking, offer_ranking, rk, candate) as ranking
	from ranking2
	where row_num = 1
	order by cust_id
	, ranking
	;

END
; 

CALL nba_offer_targeting.bq_sp_nba_ffh_offer_ranking_existing()




