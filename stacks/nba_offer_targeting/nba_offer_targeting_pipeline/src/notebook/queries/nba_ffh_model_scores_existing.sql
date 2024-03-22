

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_nba_ffh_model_scores_existing()

-- This stored procedure pulls the list of customers at cust_id + ban + lpds_id level who are eligible for Casa IRPC Offers

BEGIN

	--------------------------------------------------------------------------------------------
	-- NBA RECO for EXISTING FFH CX
	--------------------------------------------------------------------------------------------

	DECLARE hsic_w float64;
	DECLARE lwc_w int64;
	DECLARE shs_w float64;
	DECLARE sing_w int64;
	DECLARE tos_w float64;
	DECLARE tv_w int64;
	DECLARE wfp_w int64;
	DECLARE sws_w int64;
	DECLARE hs_w int64;
	DECLARE ht_w int64;
	DECLARE whsia_w float64;
	DECLARE uphsia_w int64;
	DECLARE up1g_w int64;
	DECLARE hts_w int64;

	SET hts_w=5;
	SET hs_w=4;
	SET ht_w=4;
	SET hsic_w=3.5;
	SET whsia_w=3.5;
	SET shs_w=1.5;
	SET tv_w=1;
	SET sing_w=1;
	SET lwc_w=1;
	SET sws_w=1;
	SET wfp_w=1;
	SET uphsia_w=1;
	SET up1g_w=1;
	SET tos_w=0.5;


	CREATE OR REPLACE TABLE nba_offer_targeting.nba_ffh_model_scores_existing AS 


	with models as (

	select lpds_id,  cust_id, reco, score 

	from 

		(
		select lpds_id,  cust_id,
		'hts' as reco,
		case when hts_decile = 1 then hts_score*hts_w*1.3
			 when hts_decile = 2 then hts_score*hts_w*1.2
			 when hts_decile = 3 then hts_score*hts_w*1.1
			 when hts_decile = 8 then hts_score*hts_w/1.1
			 when hts_decile = 9 then hts_score*hts_w/1.2
			 when hts_decile = 10 then hts_score*hts_w/1.3
		else hts_score*hs_w
		end 
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where hts_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'hs' as reco,
		case when hs_decile = 1 then hs_score*hs_w*1.3
			 when hs_decile = 2 then hs_score*hs_w*1.2
			 when hs_decile = 3 then hs_score*hs_w*1.1
			 when hs_decile = 8 then hs_score*hs_w/1.1
			 when hs_decile = 9 then hs_score*hs_w/1.2
			 when hs_decile = 10 then hs_score*hs_w/1.3
		else hs_score*hs_w
		end 
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where hs_score is not null
		)
		 
		union all

		(
		select lpds_id,  cust_id,
		'ht' as reco,
		case when ht_decile = 1 then ht_score*ht_w*1.3
			 when ht_decile = 2 then ht_score*ht_w*1.2
			 when ht_decile = 3 then ht_score*ht_w*1.1
			 when ht_decile = 8 then ht_score*ht_w/1.1
			 when ht_decile = 9 then ht_score*ht_w/1.2
			 when ht_decile = 10 then ht_score*ht_w/1.3
		else ht_score*ht_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where ht_score is not null
		)

		union all

		(
		select lpds_id, cust_id,
		'hsic' as reco,
		case when hsic_decile = 1 then hsic_score*hsic_w*1.3
			 when hsic_decile = 2 then hsic_score*hsic_w*1.2
			 when hsic_decile = 3 then hsic_score*hsic_w*1.1
			 when hsic_decile = 8 then hsic_score*hsic_w/1.1
			 when hsic_decile = 9 then hsic_score*hsic_w/1.2
			 when hsic_decile = 10 then hsic_score*hsic_w/1.3
		else hsic_score*hsic_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where hsic_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'tv' as reco,
		case when tv_decile = 1 then tv_score*tv_w*1.3
			 when tv_decile = 2 then tv_score*tv_w*1.2
			 when tv_decile = 3 then tv_score*tv_w*1.1
			 when tv_decile = 8 then tv_score*tv_w/1.1
			 when tv_decile = 9 then tv_score*tv_w/1.2
			 when tv_decile = 10 then tv_score*tv_w/1.3
		else tv_score*tv_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where tv_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'shs' as reco,
		case when shs_decile = 1 then shs_score*shs_w*1.3
			 when shs_decile = 2 then shs_score*shs_w*1.2
			 when shs_decile = 3 then shs_score*shs_w*1.1
			 when shs_decile = 8 then shs_score*shs_w/1.1
			 when shs_decile = 9 then shs_score*shs_w/1.2
			 when shs_decile = 10 then shs_score*shs_w/1.3
		else shs_score*shs_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where shs_score is not null
		)

		union all

		(
		select lpds_id, cust_id,
		'hp' as reco,
		case when sing_decile = 1 then sing_score*sing_w*1.3
			 when sing_decile = 2 then sing_score*sing_w*1.2
			 when sing_decile = 3 then sing_score*sing_w*1.1
			 when sing_decile = 8 then sing_score*sing_w/1.1
			 when sing_decile = 9 then sing_score*sing_w/1.2
			 when sing_decile = 10 then sing_score*sing_w/1.3
		else sing_score*sing_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where sing_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'tos' as reco,
		case when tos_decile = 1 then tos_score*tos_w*1.3
			 when tos_decile = 2 then tos_score*tos_w*1.2
			 when tos_decile = 3 then tos_score*tos_w*1.1
			 when tos_decile = 8 then tos_score*tos_w/1.1
			 when tos_decile = 9 then tos_score*tos_w/1.2
			 when tos_decile = 10 then tos_score*tos_w/1.3
		else tos_score*tos_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where tos_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'lwc' as reco,
		case when lwc_decile = 1 then lwc_score*lwc_w*1.3
			 when lwc_decile = 2 then lwc_score*lwc_w*1.2
			 when lwc_decile = 3 then lwc_score*lwc_w*1.1
			 when lwc_decile = 8 then lwc_score*lwc_w/1.1
			 when lwc_decile = 9 then lwc_score*lwc_w/1.2
			 when lwc_decile = 10 then lwc_score*lwc_w/1.3
		else lwc_score*lwc_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where lwc_score is not null
		)

		union all

		(
		select lpds_id, cust_id,
		'sws' as reco,
		case when sws_decile = 1 then sws_score*sws_w*1.3
			 when sws_decile = 2 then sws_score*sws_w*1.2
			 when sws_decile = 3 then sws_score*sws_w*1.1
			 when sws_decile = 8 then sws_score*sws_w/1.1
			 when sws_decile = 9 then sws_score*sws_w/1.2
			 when sws_decile = 10 then sws_score*sws_w/1.3
		else sws_score*sws_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where sws_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'wfp' as reco,
		case when wfp_decile = 1 then wfp_score*wfp_w*1.3
			 when wfp_decile = 2 then wfp_score*wfp_w*1.2
			 when wfp_decile = 3 then wfp_score*wfp_w*1.1
			 when wfp_decile = 8 then wfp_score*wfp_w/1.1
			 when wfp_decile = 9 then wfp_score*wfp_w/1.2
			 when wfp_decile = 10 then wfp_score*wfp_w/1.3
		else wfp_score*wfp_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where wfp_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'whsia' as reco,
		case when whsia_decile = 1 then whsia_score*whsia_w*1.3
			 when whsia_decile = 2 then whsia_score*whsia_w*1.2
			 when whsia_decile = 3 then whsia_score*whsia_w*1.1
			 when whsia_decile = 8 then whsia_score*whsia_w/1.1
			 when whsia_decile = 9 then whsia_score*whsia_w/1.2
			 when whsia_decile = 10 then whsia_score*whsia_w/1.3
		else whsia_score*whsia_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where whsia_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'up_hsia' as reco,
		case when us_hsic_any_decile = 1 then us_hsic_any_score*uphsia_w*1.3
			 when us_hsic_any_decile = 2 then us_hsic_any_score*uphsia_w*1.2
			 when us_hsic_any_decile = 3 then us_hsic_any_score*uphsia_w*1.1
			 when us_hsic_any_decile = 8 then us_hsic_any_score*uphsia_w/1.1
			 when us_hsic_any_decile = 9 then us_hsic_any_score*uphsia_w/1.2
			 when us_hsic_any_decile = 10 then us_hsic_any_score*uphsia_w/1.3
		else us_hsic_any_score*uphsia_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where us_hsic_any_score is not null
		)

		union all

		(
		select lpds_id,  cust_id,
		'up_1g' as reco,
		case when us_hsic1g_decile = 1 then us_hsic1g_score*up1g_w*1.3
			 when us_hsic1g_decile = 2 then us_hsic1g_score*up1g_w*1.2
			 when us_hsic1g_decile = 3 then us_hsic1g_score*up1g_w*1.1
			 when us_hsic1g_decile = 8 then us_hsic1g_score*up1g_w/1.1
			 when us_hsic1g_decile = 9 then us_hsic1g_score*up1g_w/1.2
			 when us_hsic1g_decile = 10 then us_hsic1g_score*up1g_w/1.3
		else us_hsic1g_score*up1g_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where us_hsic1g_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'rn_shs' as reco,
		case when renew_shs_decile = 1 then renew_shs_score*shs_w*1.3
			 when renew_shs_decile = 2 then renew_shs_score*shs_w*1.2
			 when renew_shs_decile = 3 then renew_shs_score*shs_w*1.1
			 when renew_shs_decile = 8 then renew_shs_score*shs_w/1.1
			 when renew_shs_decile = 9 then renew_shs_score*shs_w/1.2
			 when renew_shs_decile = 10 then renew_shs_score*shs_w/1.3
		else renew_shs_score*shs_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where renew_shs_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'stmp' as reco,
		case when us_netflix_decile = 1 then us_netflix_score*1.3
			 when us_netflix_decile = 2 then us_netflix_score*1.2
			 when us_netflix_decile = 3 then us_netflix_score*1.1
			 when us_netflix_decile = 8 then us_netflix_score/1.1
			 when us_netflix_decile = 9 then us_netflix_score/1.2
			 when us_netflix_decile = 10 then us_netflix_score/1.3
		else us_netflix_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where us_netflix_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'hsic_2' as reco,
		case when cs_hsic_t1_decile = 1 then cs_hsic_t1_score*1.3
			 when cs_hsic_t1_decile = 2 then cs_hsic_t1_score*1.2
			 when cs_hsic_t1_decile = 3 then cs_hsic_t1_score*1.1
			 when cs_hsic_t1_decile = 8 then cs_hsic_t1_score/1.1
			 when cs_hsic_t1_decile = 9 then cs_hsic_t1_score/1.2
			 when cs_hsic_t1_decile = 10 then cs_hsic_t1_score/1.3
		else cs_hsic_t1_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where cs_hsic_t1_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'hsic_3' as reco,
		case when cs_hsic_t2_decile = 1 then cs_hsic_t2_score*1.3
			 when cs_hsic_t2_decile = 2 then cs_hsic_t2_score*1.2
			 when cs_hsic_t2_decile = 3 then cs_hsic_t2_score*1.1
			 when cs_hsic_t2_decile = 8 then cs_hsic_t2_score/1.1
			 when cs_hsic_t2_decile = 9 then cs_hsic_t2_score/1.2
			 when cs_hsic_t2_decile = 10 then cs_hsic_t2_score/1.3
		else cs_hsic_t2_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where cs_hsic_t2_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'tos_2' as reco,
		case when cs_tos_standard_decile = 1 then cs_tos_standard_score*1.3
			 when cs_tos_standard_decile = 2 then cs_tos_standard_score*1.2
			 when cs_tos_standard_decile = 3 then cs_tos_standard_score*1.1
			 when cs_tos_standard_decile = 8 then cs_tos_standard_score/1.1
			 when cs_tos_standard_decile = 9 then cs_tos_standard_score/1.2
			 when cs_tos_standard_decile = 10 then cs_tos_standard_score/1.3
		else cs_tos_standard_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where cs_tos_standard_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'tos_3' as reco,
		case when cs_tos_ultimate_decile = 1 then cs_tos_ultimate_score*1.3
			 when cs_tos_ultimate_decile = 2 then cs_tos_ultimate_score*1.2
			 when cs_tos_ultimate_decile = 3 then cs_tos_ultimate_score*1.1
			 when cs_tos_ultimate_decile = 8 then cs_tos_ultimate_score/1.1
			 when cs_tos_ultimate_decile = 9 then cs_tos_ultimate_score/1.2
			 when cs_tos_ultimate_decile = 10 then cs_tos_ultimate_score/1.3
		else cs_tos_ultimate_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where cs_tos_ultimate_score is not null
		)
		
		union all

		(
		select lpds_id,  cust_id,
		'tos_4' as reco,
		case when us_tos_b2c_decile = 1 then us_tos_b2c_score*1.3
			 when us_tos_b2c_decile = 2 then us_tos_b2c_score*1.2
			 when us_tos_b2c_decile = 3 then us_tos_b2c_score*1.1
			 when us_tos_b2c_decile = 8 then us_tos_b2c_score/1.1
			 when us_tos_b2c_decile = 9 then us_tos_b2c_score/1.2
			 when us_tos_b2c_decile = 10 then us_tos_b2c_score/1.3
		else us_tos_b2c_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_ecsus_scored_combined`
		where us_tos_b2c_score is not null
		)
		
		union all 
		
		(
		select lpds_id
		, cust_id
		, 'mob' as reco
		, case when decile_grp_num = 1 then score_num*1.3
		  when decile_grp_num = 2 then score_num*1.2
		  when decile_grp_num = 3 then score_num*1.1
		  when decile_grp_num = 8 then score_num/1.1
		  when decile_grp_num = 9 then score_num/1.2
		  when decile_grp_num = 10 then score_num/1.3
		else score_num
		end
		as score
		from `bi-srv-mobilityds-pr-80a48d.ucar_ingestion.bq_product_instance_model_score` a 
		inner join 
		(
		select distinct cust_id
		, bacct_num
		, lpds_id
		, row_number() over(partition by cust_id, bacct_num order by lpds_id) as row_num
		from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`
		where part_load_dt = (select max(part_load_dt) from `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance`)
		qualify row_num = 1
		) b 
		on a.customer_id = b.cust_id
		and a.bus_bacct_num = b.bacct_num

		where predict_model_id = "5500"
		and create_dt = (select max(create_dt) from `bi-srv-mobilityds-pr-80a48d.ucar_ingestion.bq_product_instance_model_score` where predict_model_id = "5500")
		)
	)

	select cust_id,lpds_id,reco,
	CONCAT('reco_', cast(rank() over (partition by cust_id,lpds_id order by score desc) as string)) as rank_,
	rank() over (partition by cust_id,lpds_id order by score desc) as rank
	FROM models
	order by cust_id,lpds_id,rank_
	;
	
END
; 

CALL nba_offer_targeting.bq_sp_nba_ffh_model_scores_existing()

	-- EXPORT DATA
	--   OPTIONS( uri='gs://cdo-dse-workspace-np-45d0d5-development-weibo/IRPC/existing_reco*.csv',
	-- /*    compression=GZIP,*/
	--     format='CSV',
	--     overwrite=TRUE,
	--     header=TRUE,
	--     field_delimiter=',') AS

	-- select lpds_id,  cust_id,reco_1,reco_2,reco_3,reco_4,reco_5,reco_6,reco_7,reco_8,reco_9,reco_10
	-- from
	-- (
	--   select lpds_id,   cust_id,reco, rank_
	--   from nba_offer_targeting.nba_ffh_model_scores_existing
	-- ) d
	-- pivot
	-- (
	--   max(reco)
	--   for rank_ in ('reco_1','reco_2','reco_3','reco_4','reco_5','reco_6','reco_7','reco_8','reco_9','reco_10')
	-- ) piv
	-- order by cust_id, lpds_id

	-- limit 3000000
	-- ;

