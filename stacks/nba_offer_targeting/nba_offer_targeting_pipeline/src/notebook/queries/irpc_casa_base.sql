

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_irpc_casa_base()

-- This stored procedure pulls the list of customers at cust_id + ban + lpds_id level who are eligible for Casa IRPC Offers

BEGIN

	DECLARE maxDate date;
	SET maxDate = (SELECT MAX(part_load_dt) FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` WHERE part_load_dt > DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)); 

	-- IRPC Casa Base
	-- Standard exclusions 
	-- AB and BC Customers Only 
	-- FIFA Customers Only 
	-- Fibre Customers Only 
	-- Have Internet 
	-- ~1.05M Customers
	-- ~645K Customers



	CREATE OR REPLACE TABLE nba_offer_targeting.bq_irpc_casa_base AS

	WITH casa_base1 AS 
		(
		SELECT 	a.cust_id,
				a.bacct_num,
				fms_address_id,
				lpds_id,
				ACCT_START_DT AS candate,
				OPTIK_TV_IND,HSIA_IND, 
				HSIA_CONTRACT_END_DT AS rpp_hsia_end_dt, 
				OPTIK_TV_CONTRACT_END_DT AS rpp_ttv_end_dt, 
				HSIA_MAX_SPD AS hs_max_speed_numeric, 
				PRVSN_HSIA_SPD AS provisioned_hs_speed_numeric, 
				b.intended_contract_end_dt AS prev_rpp_hsia_end_dt, 
				format_date("%Y_%m", date_sub(b.intended_contract_end_dt, interval 2 month)) AS bill_month, 
				c.hsic_avg_charges, 
				c.hsic_tot_charges_1M AS charges_30d, 
				c.hsic_tot_charges_2M AS charges_2M, 
				c.hsic_tot_charges_3M AS charges_3M, 
				c.hsic_tot_charges_4M AS charges_4M, 
				c.bill_month_1M, 
				c.bill_month_2M, 
				c.bill_month_3M, 
				c.bill_month_4M, 
				c.bill_month_5M

		FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl` a

		LEFT JOIN
		(
			SELECT 
			cust_id,bacct_num,intended_contract_end_dt
			FROM
			(
				SELECT
				cust_id,bacct_num,intended_contract_end_dt,
				RANK() OVER (PARTITION BY cust_id,bacct_num ORDER BY intended_contract_end_dt DESC) AS rk
				FROM `bi-srv-divgdsa-pr-098bdd.common.bq_ffh_contract_hist` 
				WHERE service_instance_type_cd = 'HSIC'
			)
			WHERE rk = 1 AND intended_contract_end_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND intended_contract_end_dt <CURRENT_DATE()
			ORDER BY cust_id,bacct_num
		) b 
		ON a.cust_id  = b.cust_id AND a.bacct_num = b.bacct_num

		LEFT JOIN nba_offer_targeting.internet_payment c
		ON a.cust_id  = c.cust_id AND a.bacct_num = c.ban

		WHERE a.EX_STANDARD_EX = 0
		AND a.SERV_PROV in ('AB','BC')
		AND a.PROVISIONING_SYSTEM = 'FIFA'
		AND a.TECH_GPON > 0
		AND a.HSIA_IND > 0 
		-- AND (a.HSIA_CONTRACT_END_DT < DATE_ADD(CURRENT_DATE(), INTERVAL 90 DAY) or a.HSIA_CONTRACT_END_DT IS NULL )
		),

	casa_exclusion AS 
		(
		SELECT distinct cust_id FROM

			(
			SELECT distinct cust_id
			FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` 
			WHERE prod_intrnl_nm LIKE '%EPP%'
			AND effective_end_dt >  DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
			AND  part_load_dt = maxDate
			)
			
		UNION ALL

			(
			SELECT distinct cust_id
			FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` 
			WHERE prod_intrnl_nm LIKE '%TSD%'
			AND effective_end_dt >  DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
			AND  part_load_dt = maxDate
			)
			
		UNION ALL

			(
			SELECT distinct cust_id
			FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` 
			WHERE 
			(
			UPPER(prod_intrnl_nm) LIKE '%CONNECTING FAMILIES%'
			OR UPPER(prod_intrnl_nm) LIKE '%REALTOR%'
			OR UPPER(prod_intrnl_nm) LIKE '%STRATA%'
			)
			AND effective_end_dt >  DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
			AND  part_load_dt = maxDate
			)
		
		UNION ALL

			(
			SELECT DISTINCT cust_id 
			FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` a 
			INNER JOIN nba_offer_targeting.prod_cd_exclusions b 
			ON a.prod_cd = b.prod_cd
			WHERE effective_end_dt >  DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
			AND part_load_dt =  maxDate 
			AND b.irpc_digital_exclusions_1 = 1
			)
		
		UNION ALL

			(
			SELECT DISTINCT cust_id 
			FROM `bi-srv-divgdsa-pr-098bdd.common.bq_hs_product_instance_details` a 
			INNER JOIN nba_offer_targeting.prod_cd_exclusions b 
			ON a.prod_cd = b.prod_cd
			WHERE product_instance_status_cd = 'A'
			AND b.irpc_digital_exclusions_2 = 1
			AND  part_load_dt =  maxDate 
			)
		)

	SELECT 	cust_id,
			bacct_num,
			fms_address_id,
			lpds_id,
			candate,
			OPTIK_TV_IND,
			HSIA_IND,
			hs_max_speed_numeric, 
			provisioned_hs_speed_numeric,
			rpp_hsia_end_dt,
			rpp_ttv_end_dt,
			
			CASE WHEN total_charges > 125 THEN 125 
				 WHEN total_charges IS NULL THEN 0 
				 ELSE total_charges END AS total_charges

	FROM 

		(
		-- Clarify the logic with Brianne
		SELECT 	cust_id,
				bacct_num,
				fms_address_id,
				lpds_id,
				candate,
				OPTIK_TV_IND,
				HSIA_IND,
				hs_max_speed_numeric, 
				provisioned_hs_speed_numeric,
				rpp_hsia_end_dt,
				rpp_ttv_end_dt,
				
				CASE 	WHEN DATE_DIFF(DATE(rpp_hsia_end_dt), CURRENT_DATE(), DAY) >= 0 THEN charges_30d
						WHEN (rpp_hsia_end_dt IS NULL AND DATE_DIFF(DATE((CURRENT_DATE())), DATE(prev_rpp_hsia_end_dt), DAY) <= 120) THEN 
						
						CASE 	WHEN TRIM(bill_month) = TRIM(bill_month_1M) AND charges_30d IS NOT NULL THEN charges_30d 
								WHEN TRIM(bill_month) = TRIM(bill_month_2M) AND charges_2M  IS NOT NULL THEN charges_2M 
								WHEN TRIM(bill_month) = TRIM(bill_month_3M) AND charges_3M  IS NOT NULL THEN charges_3M 
								WHEN TRIM(bill_month) = TRIM(bill_month_4M) AND charges_4M  IS NOT NULL THEN charges_4M 
								END 
								
				ELSE hsic_avg_charges 
				END AS total_charges
				
		FROM casa_base1
		WHERE cust_id NOT IN (SELECT cust_id FROM casa_exclusion)
		)
		
	WHERE total_charges > 0
	AND NOT (rpp_hsia_end_dt IS NOT NULL AND rpp_ttv_end_dt IS NOT NULL AND DATE_DIFF(rpp_hsia_end_dt, rpp_ttv_end_dt, DAY) = 0)	
	
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	;

END
; 

CALL nba_offer_targeting.bq_sp_irpc_casa_base()



