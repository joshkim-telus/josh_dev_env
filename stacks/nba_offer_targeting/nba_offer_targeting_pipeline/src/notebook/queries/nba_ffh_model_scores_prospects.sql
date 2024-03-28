

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_nba_ffh_model_scores_prospects()

-- This stored procedure pulls the latest propensity scores for various product acquisitions for prospects (Naked MOB). 

BEGIN

	--------------------------------------------------------------------------------------------
	-- NBA RECO for EXISTING FFH CX
	--------------------------------------------------------------------------------------------

	DECLARE hts_w int64;
	DECLARE hs_w int64;
	DECLARE ht_w int64;
	DECLARE hsic_w float64;
	DECLARE tv_w int64;
	DECLARE shs_w float64;
	DECLARE sing_w int64;
	DECLARE tos_w float64;
	DECLARE lwc_w int64;
	DECLARE sws_w int64;
	DECLARE wfp_w int64;
	DECLARE whsia_w float64;
	
	SET hts_w=5;
	SET hs_w=4;
	SET ht_w=4;
	SET hsic_w=3.5;
	SET tv_w=1;
	SET shs_w=1.5;
	SET sing_w=1;
	SET tos_w=0.5;
	SET lwc_w=1;
	SET sws_w=1;
	SET wfp_w=1;
	SET whsia_w=3.5;


	CREATE OR REPLACE TABLE nba_offer_targeting.nba_ffh_model_scores_prospects AS 

	with models as (

	select lpds_id, reco, score 

	 from 

		(
		select lpds_id,
		'hts' as reco,
		case when hts_decile = 1 then hts_score*any_score*hts_w*1.3
			 when hts_decile = 2 then hts_score*any_score*hts_w*1.2
			 when hts_decile = 3 then hts_score*any_score*hts_w*1.1
			 when hts_decile = 8 then hts_score*any_score*hts_w/1.1
			 when hts_decile = 9 then hts_score*any_score*hts_w/1.2
			 when hts_decile = 10 then hts_score*any_score*hts_w/1.3
		else hts_score*any_score*hts_w
		end 
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where hts_score is not null
		)
		
		union all

		(
		select lpds_id,
		'hs' as reco,
		case when hs_decile = 1 then hs_score*any_score*hs_w*1.3
			 when hs_decile = 2 then hs_score*any_score*hs_w*1.2
			 when hs_decile = 3 then hs_score*any_score*hs_w*1.1
			 when hs_decile = 8 then hs_score*any_score*hs_w/1.1
			 when hs_decile = 9 then hs_score*any_score*hs_w/1.2
			 when hs_decile = 10 then hs_score*any_score*hs_w/1.3
		else hs_score*any_score*hs_w
		end 
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where hs_score is not null
		)
		 
		union all

		(
		select lpds_id,
		'ht' as reco,
		case when ht_decile = 1 then ht_score*any_score*ht_w*1.3
			 when ht_decile = 2 then ht_score*any_score*ht_w*1.2
			 when ht_decile = 3 then ht_score*any_score*ht_w*1.1
			 when ht_decile = 8 then ht_score*any_score*ht_w/1.1
			 when ht_decile = 9 then ht_score*any_score*ht_w/1.2
			 when ht_decile = 10 then ht_score*any_score*ht_w/1.3
		else ht_score*any_score*ht_w
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where ht_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where hsic_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where tv_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where shs_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where sing_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where tos_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where lwc_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where sws_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where wfp_score is not null
		)

		union all

		(
		select lpds_id,
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
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where whsia_score is not null
		)
		
		union all

		(
		select lpds_id,
		'tos_u' as reco,
		case when tosu_decile = 1 then tosu_score*1.3
			 when tosu_decile = 2 then tosu_score*1.2
			 when tosu_decile = 3 then tosu_score*1.1
			 when tosu_decile = 8 then tosu_score/1.1
			 when tosu_decile = 9 then tosu_score/1.2
			 when tosu_decile = 10 then tosu_score/1.3
		else tosu_score
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where tosu_score is not null
		and tosu_decile <= 5
		)
		
		union all 

		(
		select lpds_id,
		'tos_c' as reco,
		case when tosu_decile = 10 then (1-tosu_score)*1.3
			 when tosu_decile = 9 then (1-tosu_score)*1.2
			 when tosu_decile = 8 then (1-tosu_score)*1.1
			 when tosu_decile = 3 then (1-tosu_score)/1.1
			 when tosu_decile = 2 then (1-tosu_score)/1.2
			 when tosu_decile = 1 then (1-tosu_score)/1.3
		else (1-tosu_score)
		end
		as score
		from `cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined`
		where tosu_score is not null
		and tosu_decile >= 5
		)
	)

	select lpds_id,  
	reco,
	CONCAT('reco_', cast(rank() over (partition by lpds_id order by score desc) as string)) as rank_,
	rank() over (partition by lpds_id order by score desc) as ranks
	FROM models
	order by lpds_id, ranks
	;


END 
; 

CALL nba_offer_targeting.bq_sp_nba_ffh_model_scores_prospects()
; 








CREATE OR REPLACE TABLE `cdo-dse-workspace-np-45d0d5.development_weibo.prospect_reco3` as

select distinct lpds_id,reco_1,reco_2,reco_3,reco_4,reco_5,reco_6,reco_7,reco_8,reco_9,reco_10
from
(
  select lpds_id, reco, rank_
  from `cdo-dse-workspace-np-45d0d5.development_weibo.prospect_reco1`
) d
pivot
(
  max(reco)
  for rank_ in ('reco_1','reco_2','reco_3','reco_4','reco_5','reco_6','reco_7','reco_8','reco_9','reco_10')
) piv
;



CREATE OR REPLACE TABLE `cdo-dse-workspace-np-45d0d5.development_weibo.prospect_reco2` as
select a.ban, a.lpds_id,
b.reco_1,b.reco_2,b.reco_3,b.reco_4,b.reco_5,b.reco_6,b.reco_7,b.reco_8,b.reco_9,b.reco_10
from `cdo-dse-workspace-np-45d0d5.development_weibo.mobility_lpds_id` a
inner join `cdo-dse-workspace-np-45d0d5.development_weibo.prospect_reco3` b
on a.lpds_id =cast(b.lpds_id as int)
;



CREATE OR REPLACE TABLE `cdo-dse-workspace-np-45d0d5.development_weibo.prospect_1G` as

select a.ban, a.lpds_id
from `cdo-dse-workspace-np-45d0d5.development_weibo.mobility_lpds_id` a
inner join 
`cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined` b
on a.lpds_id =cast(b.lpds_id as int)
where b.hsic1g_decile<=3
;

CREATE OR REPLACE TABLE `cdo-dse-workspace-np-45d0d5.development_weibo.shs_ctlvd` as

select a.ban, a.lpds_id
from `cdo-dse-workspace-np-45d0d5.development_weibo.mobility_lpds_id` a
inner join 
`cdo-dse-workspace-np-45d0d5.featurestore.nba_prosp_acq_score_combined` b
on a.lpds_id =cast(b.lpds_id as int)
where b.shscv_decile<=5
;
