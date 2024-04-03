

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_nba_offer_targeting_dashboard()

BEGIN 

-- This query prepares the dataset for Telus Home Solutions NBA Offer Targeting Count Summary Dashboard (For both Regular & IRPC Offers)
-- Dependencies: 
	-- `bi-stg-mobilityds-pr-db8ce2.nba_offer_targeting.bq_offer_targeting_params_upd` *** TO BE UPDATED TO A TABLE IN bi-srv-mobilityds-pr-80a48d
	-- `bi-srv-divgdsa-pr-098bdd.offer_intake.bq_product_offering`
	-- `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl` 
	-- `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_fda_mob_mobility_base` 
	-- `divg-team-v03-pr-de558a.nba_offer_targeting.nba_ffh_offer_ranking` *** TO BE UPDATED TO A TABLE IN bi-srv-mobilityds-pr-80a48d

	CREATE OR REPLACE TABLE nba_offer_targeting.nba_offer_targeting_dashboard AS 

	with max_dt AS ( 
	  SELECT 
	  max(part_dt) as part_dt
	  FROM `bi-stg-mobilityds-pr-db8ce2.nba_offer_targeting.bq_offer_targeting_params_upd` 
		), 
		
	reg_offer_info AS (
	  select a.NCID
			, a.line_of_business -- required
			, a.Category -- required
			, a.Subcategory -- required
			, a.promo_seg AS promo_seg -- required
			, a.Offer_Targeting -- required
			, CASE WHEN a.Digital_Availability = true THEN True ELSE False END AS Digital_Availability -- required
			, CASE WHEN a.CE_Availability = true THEN True ELSE False END AS CE_Availability -- required
			, CASE WHEN a.CSS_Availability = true THEN True ELSE False END AS CSS_Availability -- required
			, CASE WHEN a.D2C_Availability = true THEN True ELSE False END AS D2C_Availability -- required
			, CASE WHEN a.Corp_Store_Availability = true THEN True ELSE False END AS Corp_Store_Availability -- required
			, CASE WHEN a.Dealer_Availability = true THEN True ELSE False END AS Dealer_Availability-- required
			, CASE WHEN a.CSD_Availability = true THEN True ELSE False END AS CSD_Availability -- required
			, False AS Retail_Availability
			, CAST(a.valid_start_ts AS DATE FORMAT 'MON DD, YYYY') AS valid_start_dt  -- required
			, CAST(a.valid_end_ts AS DATE FORMAT 'MON DD, YYYY') AS valid_end_dt -- required
			, 1 AS ranking
	  from `bi-stg-mobilityds-pr-db8ce2.nba_offer_targeting.bq_offer_targeting_params_upd` a 
	  inner join max_dt b
	  on a.part_dt = b.part_dt
	  where a.if_active = 1
		), 
		
	base AS (
		SELECT offer_name as promo_seg
		, netcracker_id
		, offer_start_dt
		, offer_expiry_dt
		, line_of_business
		, offer_category
		, offer_subcategory
		, Offer_Targeting
		, marketing_status
		, flattened_field.name as criteria 
		, stc_flattened_field as specialized_criteria
		, flattened_field_2 AS channels
		FROM
		(
			SELECT offer_name
			, netcracker_id
			, offer_start_dt
			, offer_expiry_dt
			, line_of_business
			, offer_category
			, offer_subcategory
			, CASE WHEN offer_type = "Above the Line" THEN "ATL" 
				  WHEN offer_type = "Below the Line" THEN "BTL" 
			  END AS Offer_Targeting
			, marketing_status
			, flattened_field
			, stc_flattened_field
			FROM
			`bi-srv-divgdsa-pr-098bdd.offer_intake.bq_product_offering`,
			UNNEST(criteria.list_string_criteria) AS flattened_field, 
			UNNEST(criteria.specialized_targeting_criteria) AS stc_flattened_field
		) a
		, UNNEST(flattened_field.value) as flattened_field_2
	), 

	irpc_offer_info AS (
		select netcracker_id as NCID
		, line_of_business as line_of_business
		, offer_category as Category 
		, offer_subcategory as Subcategory
		, promo_seg as promo_seg 
		, Offer_Targeting
		, MAX(IF(channels = 'Digital', True, False)) AS Digital_Availability
		, MAX(IF(channels = 'CE', True, False)) AS CE_Availability
		, False AS CSS_Availability 
		, False AS D2C_Availability 
		, MAX(IF(channels = 'Corporate Store', True, False)) AS Corp_Store_Availability
		, MAX(IF(channels = 'Dealer', True, False)) AS Dealer_Availability
		, MAX(IF(channels = 'CSD', True, False)) AS CSD_Availability
		, MAX(IF(channels = 'Retail', True, False)) AS Retail_Availability
		, offer_start_dt AS valid_start_dt
		, offer_expiry_dt AS valid_end_dt
		, 2 AS ranking
		from base 
		where CURRENT_DATE() between offer_start_dt and offer_expiry_dt
		group by 1, 2, 3, 4, 5, 6, 15, 16
	)

	, all_offer_info AS (
		select * 
	, row_number() over (partition by NCID order by ranking) as row_num
		from 
		(
		(select * from reg_offer_info) 
		union all
		(select * from irpc_offer_info)
		)
	)

	, offer_info AS (
		select NCID
		, line_of_business
		, Category
		, Subcategory
		, promo_seg
		, Offer_Targeting
		, Digital_Availability
		, CE_Availability
		, CSS_Availability
		, D2C_Availability
		, Corp_Store_Availability
		, Dealer_Availability
		, CSD_Availability
		, Retail_Availability
		, valid_start_dt
		, valid_end_dt
		from all_offer_info
		where row_num = 1
		)
	
	, ffh_base AS (
	  select distinct cust_id
		, case when SERV_PROV in ('AB','BC') 
			  and (MNH_MOB_BAN = 0 or MNH_MOB_BAN is null) then 'Naked FFH West'
			when SERV_PROV in ('AB','BC') 
			  and (MNH_MOB_BAN > 0) then 'MNH West'
			when SERV_PROV not in ('AB','BC') then 'FFH East' 
			else 'Unknown Region'
			end as LOB 
		from `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl` 
	)
	
	, mob_base AS (
	  select distinct BAN
		, case when province in ('AB','BC') 
			  and (MNH_FFH_BAN = 0 or MNH_FFH_BAN is null) then 'Naked Mob West'
			when province in ('AB','BC') 
			  and (MNH_FFH_BAN > 0) then 'MNH West'
			when province not in ('AB','BC') then 'Mob East' 
			else 'Unknown Region'
			end as LOB 
		from `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_fda_mob_mobility_base` 
	)

	, spd AS (
		select distinct
		lpds_id
		, CONCAT(system_province_cd, coid) as prov_coid
		from `bi-srv-divgdsa-pr-098bdd.common.bq_premise_universe` 
		WHERE part_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
	)
	
	, today_count AS (
		select current_date() as load_dt
		, a.promo_seg
		, case when a.cust_id is not null then d.LOB
			 when a.mobility_ban is not null then e.LOB end as LOB
		, c.prov_coid 
		, b.Category  
		, b.Subcategory
		, b.Offer_Targeting as ATL  
		, case when a.ranking >=10 then 'Rank 10+'
			when a.ranking = 9 then 'Rank 9'
			when a.ranking = 8 then 'Rank 8'
			when a.ranking = 7 then 'Rank 7'
			when a.ranking = 6 then 'Rank 6'
			when a.ranking = 5 then 'Rank 5'
			when a.ranking = 4 then 'Rank 4'
			when a.ranking = 3 then 'Rank 3'
			when a.ranking = 2 then 'Rank 2'
			when a.ranking = 1 then 'Rank 1'
			end as rank
		, b.Digital_Availability as Digital
		, b.CE_Availability as CE
		, b.CSS_Availability as CSS
		, b.D2C_Availability as D2C
		, b.Corp_Store_Availability as Corporate_Store
		, b.Dealer_Availability as Dealer
		, b.CSD_Availability as CSD 
		, count(a.promo_seg) as COUNT_r
		, count(a.promo_seg)/1000 as COUNT_k
		from `divg-team-v03-pr-de558a.nba_offer_targeting.nba_ffh_offer_ranking` a
		left join offer_info b on a.offer_code = b.NCID
		left join spd c on a.lpds_id = c.lpds_id
		left join ffh_base d on a.cust_id = d.cust_id
		left join mob_base e on a.mobility_ban = e.ban
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
		order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	)
	
	, today_count12 AS (
		select 
		promo_seg
		, sum(COUNT_r) as total_counts
		, sum(COUNT_k) as total_k_counts
		from today_count
		group by promo_seg
	)
	select a.*
	, b.total_counts
	, b.total_k_counts
	, round(a.COUNT_r/b.total_counts, 2) as PERCENT
	from today_count a left join today_count12 b 
	on a.promo_seg = b.promo_seg
	; 

END		
; 

CALL nba_offer_targeting.bq_sp_hs_nba_ot_count_summary_dashboard()