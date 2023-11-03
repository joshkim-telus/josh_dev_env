

--qry_contract_renewal_target_date_v2--
--CONTRACT RENEWAL STATUS AS OF TWO "target_dates"--

DECLARE target_date_1 DATE DEFAULT "2022-02-01";
DECLARE target_date_2 DATE DEFAULT "2023-02-01";
DECLARE interval_days INT64 DEFAULT 0;

SELECT camp_id
, contract_renewal
, count(distinct ban) as ban_count

FROM 

(
	SELECT camp_id
	, ban
	, CASE WHEN SUM(change_in_contract_end_dt) > 0 THEN 1
				ELSE 0 
				END AS contract_renewal

	FROM 

	(
		SELECT a.camp_id
		, a.ban AS ban
		, a.pi_prod_instnc_typ_cd
		, a.latest_contract_end_dt as latest_contract_end_dt_1
		, b.latest_contract_end_dt as latest_contract_end_dt_2
		
		, CASE  WHEN DATE(B.latest_contract_end_dt) = "9999-12-31" THEN 0 
				WHEN DATE_DIFF(B.latest_contract_end_dt, A.latest_contract_end_dt, DAY) BETWEEN 350 AND 1000000 THEN 1 
				-- WHEN DATE_DIFF(B.latest_contract_end_dt, A.latest_contract_end_dt, DAY) BETWEEN -1000 AND -350 THEN 2
				ELSE 0 
		  END AS change_in_contract_end_dt
		
		FROM 
		(
			SELECT b.camp_id
			, b.ban as ban
		    -- , a.bus_prod_instnc_id
		    , a.pi_prod_instnc_typ_cd
			, DATE(MAX(pi_cntrct_end_ts)) as latest_contract_end_dt

			FROM `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans` b 
			LEFT JOIN`cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a
			
			ON a.bacct_bus_bacct_num = b.ban 

			WHERE DATE(prod_instnc_ts) = target_date_1
			AND pi_prod_instnc_stat_cd = 'A'
			AND pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 

			GROUP BY b.camp_id
			, b.ban
			-- , a.bus_prod_instnc_id
			  , a.pi_prod_instnc_typ_cd

			ORDER BY b.camp_id
			, b.ban
			-- , a.bus_prod_instnc_id
			  , a.pi_prod_instnc_typ_cd
		) a 

		inner join 

		(
			SELECT b.camp_id
			, b.ban as ban
	  	    -- , a.bus_prod_instnc_id
		    , a.pi_prod_instnc_typ_cd
			,  DATE(MAX(pi_cntrct_end_ts)) as latest_contract_end_dt

			FROM `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans` b 
			LEFT JOIN`cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` a
			
			ON a.bacct_bus_bacct_num = b.ban 

			WHERE DATE(prod_instnc_ts) = target_date_2
			AND pi_prod_instnc_stat_cd = 'A'
			AND pi_prod_instnc_typ_cd IN ('SING', 'HSIC','TTV','SMHM') 

			GROUP BY b.camp_id
			, b.ban
			-- , a.bus_prod_instnc_id
			  , a.pi_prod_instnc_typ_cd

			ORDER BY b.camp_id
			, b.ban
			-- , a.bus_prod_instnc_id
			  , a.pi_prod_instnc_typ_cd
		) b 
		on a.ban = b.ban
		and a.pi_prod_instnc_typ_cd = b.pi_prod_instnc_typ_cd
	)

	GROUP BY camp_id
	, ban
)

WHERE contract_renewal = 1 

GROUP BY camp_id
, contract_renewal

ORDER BY camp_id
, contract_renewal DESC 