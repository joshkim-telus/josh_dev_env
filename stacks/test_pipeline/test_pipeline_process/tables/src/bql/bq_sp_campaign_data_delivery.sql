

-- call_to_retention_dataset.bq_sp_campaign_data_element_hcr("2023-06-09")

-- CREATE OR REPLACE PROCEDURE divg_compaign_element.bq_sp_campaign_data_element_hcr_em(_ref_dt DATE)

BEGIN

	DECLARE _part_dt DATE DEFAULT "2023-09-01";

	SET _part_dt = IF(_ref_dt >= CURRENT_DATE() OR _ref_dt IS NULL, DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY), _ref_dt);

	-------------------part 1------------------------

	INSERT INTO divg_compaign_element.bq_campaign_data_element

	-- LATEST PARTITION DATE FOR bq_prod_instnc_snpsht
	WITH score_date AS(
	  (SELECT DATE(_part_dt) AS max_date)
	),
	
	high_risk_customers_list AS (
		SELECT DISTINCT BACCT_NUM as ban
		FROM (
		  SELECT *
		  , CASE  WHEN CALL_RESULT IN ("1SAL", "2RPC") THEN 1 
              ELSE 0 
              END AS CALL_RESULT_SUMMARY
		  , CASE  WHEN ATTEMPTS >= 5 THEN 1 
			        WHEN CALL_RESULT IN ("1SAL", "2RPC") THEN 1 
              ELSE 0 
			        END AS RECORD_EXHAUSTED_IND_MANUAL
		  FROM divg_compaign_element.bq_campaign_records_hcr
		  )
		WHERE RECORD_EXHAUSTED_IND_MANUAL = 1 
		AND CALL_RESULT_SUMMARY = 0 
	), 

	fms_address_id AS (
		SELECT BACCT_NUM
          , CUST_ID
          , LPDS_ID
          , FMS_ADDRESS_ID
          , ROW_NUMBER() OVER(PARTITION BY BACCT_NUM ORDER BY FMS_ADDRESS_ID NULLS LAST) as rank
		FROM `bi-srv-hsmdet-pr-7b9def.hsmdet_public.bq_pub_dly_dbm_customer_profl` 
		QUALIFY rank = 1
	), 
	
	test_control AS (
		SELECT DISTINCT bacct_bus_bacct_num as ban
		, field_1 as test_control
		FROM divg_compaign_element.bq_campaign_data_element 
		WHERE initiative_name = "high_churn_risk"
	)

	SELECT "" as field_name
	, "high_churn_risk_email" as initiative_name
	, "recurring" as campaign_type
	, "weekly" as recurring_frequency
	, cast(c.CUST_ID as string) as cust_id
	, cast(a.ban as string) as bacct_bus_bacct_num
	, cast(LPDS_ID as string) as lpds_id
	, "" as mob_ban
	, cast((select max_date from score_date) as string) as date_of_run
	, CASE  WHEN d.test_control IS NOT NULL 
			THEN d.test_control 
			WHEN d.test_control IS NULL 
			THEN 'test' 
			END AS field_1
	, "test_control" as field_1_description
	, "" as field_2
	, "model_id" as field_2_description
	, "" as field_3
	, "score_num" as field_3_description
	, "" as field_4
	, "decile_grp_num" as field_4_description
	, "" as field_5
	, "contract_end_date" as field_5_description
	, FMS_ADDRESS_ID as field_6
	, "fms_address_id" as field_6_description
	, cast(CURRENT_TIMESTAMP() as string) as field_7
	, "timestamp" as field_7_description
	, "" as field_8
	, "" as field_8_description
	, "" as field_9
	, "" as field_9_description
	, "" as field_10
	, "" as field_10_description

	FROM high_risk_customers_list a 
	LEFT JOIN fms_address_id c 
	ON CAST(a.ban as int64) = c.BACCT_NUM
	LEFT JOIN test_control d 
	ON CAST(a.ban AS STRING) = CAST(d.ban AS STRING)
	;

END




