
CREATE OR REPLACE TABLE nba_offer_targeting.nba_ffh_qua_base AS

WITH std AS 
	(
	SELECT DISTINCT cust_id 
	
	FROM 
		(
		-- ~100k customers
		SELECT DISTINCT cust_id 
		FROM `cdo-dse-workspace-np-45d0d5.dsa_datastore.product_instance_fact`
		WHERE prod_intrnl_nm like '%EPP%'
		AND effective_end_dt >=  CURRENT_DATE()
		AND  part_load_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
		)
	UNION ALL
		(
		-- ~26k customers
		SELECT DISTINCT cust_id 
		FROM `cdo-dse-workspace-np-45d0d5.dsa_datastore.product_instance_fact` 
		WHERE prod_intrnl_nm like '%TSD%'
		AND effective_end_dt >=  CURRENT_DATE()
		AND part_load_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
		)
	UNION ALL
		(
		-- ~21k customers
		SELECT DISTINCT cust_id 
		FROM `cdo-dse-workspace-np-45d0d5.dsa_datastore.product_instance_fact` 
		WHERE 
			(
			UPPER(prod_intrnl_nm) like '%CONNECTING FAMILIES%'
			OR UPPER(prod_intrnl_nm) like '%REALTOR%'
			OR UPPER(prod_intrnl_nm) like '%STRATA%'
			)
		AND effective_end_dt >=  CURRENT_DATE()
		AND part_load_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  
		)
	UNION ALL
		(
		SELECT DISTINCT cust_id
		FROM `cdo-dse-workspace-np-45d0d5.dsa_datastore.product_instance_fact` 
		WHERE effective_end_dt >=  CURRENT_DATE()
		AND Prod_cd IN 
			(
			"21170668"	,
			"40772201"	,
			"40903"	,
			"21171128"	,
			"40771451"	,
			"40897"	,
			"21058868"	,
			"21170668"	,
			"21171128"	,
			"21172568"	,
			"40755913"	,
			"40755923"	,
			"40755933"	,
			"40756143"	,
			"40756153"	,
			"40756163"	,
			"40756233"	,
			"40756243"	,
			"40756253"	,
			"40771451"	,
			"40772201"	,
			"40786561"	,
			"40786571"	,
			"40786581"	,
			"40786611"	,
			"40786621"	,
			"40786641"	,
			"40786421"	,
			"40786441"	,
			"40786451"	,
			"40786471"	,
			"40786511"	,
			"40786521"	,
			"40897"	,
			"40903"	,
			"41111901"	,
			"41111911"	,
			"41111921"	,
			"41120591"	,
			"41120601"	,
			"41120601"	,
			"41120611"	,
			"21170668"	,
			"40772201"	,
			"21058868"	,
			"40771411"	,
			"40771451"	,
			"40771461"	
			) 
		AND part_load_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  
		)
	),

wfp1 AS (
	--Upsell Wi-Fi Plus
	--AB AND BC customers 
	--Has Internet
	--No SmartHub (WHY?) 
	--Not a current Wi-Fi Plus customer (uses PID table)
	
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Upsell' AS Category ,
	'Wi-Fi Plus' AS Subcategory   ,
	'202208 TELUS Wi-Fi Plus' AS promo_seg ,
	'9162267642120575793' AS offer_code ,
	DATETIME(2022, 07, 06, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2024, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND HSIA_IND > 0
	AND SMART_HUB_IND = 0
	AND cust_id not in (SELECT cust_id FROM `cdo-dse-workspace-np-45d0d5.dsa_datastore.product_instance_dim` 
	WHERE part_load_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
	AND current_ind = 1
	AND product_instance_status_cd = 'A' 
	AND service_instance_type_cd = 'WFP')
	),

-- whsia4 AS (
-- 	--Cross Sell, WHSIA
-- 	--AB AND BC Only 
-- 	--In FIFA
-- 	--No Internet 
-- 	--No Smart Hub 
-- 	--Smart Hub Eligible
-- 	SELECT 
-- 	cust_id,
-- 	bacct_num,
-- 	fms_address_id,
-- 	lpds_id,
-- 	ACCT_START_DT AS candate,
-- 	'Cross-sell' AS Category ,
-- 	'WHSIA' AS Subcategory   ,
-- 	'202208 Add wHSIA 25Mbps starting FROM $80' AS promo_seg ,
-- 	'9159683640113535776' AS offer_code ,
-- 	DATETIME(2022, 09, 12, 00, 00, 00) AS ASSMT_VALID_START_TS , 
-- 	DATETIME(2024, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
-- 	1 AS rk

-- 	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
-- 	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
-- 	AND SERV_PROV in ('AB','BC')
-- 	AND PROVISIONING_SYSTEM = 'FIFA'
-- 	AND HSIA_IND = 0
-- 	AND SMART_HUB_IND = 0
-- 	AND lpds_id in  (SELECT lpds_id FROM `bi-srv-hsmdet-pr-7b9def.fda_tables.bq_fda_smart_hub_eligible` 
-- 	WHERE WHSIA_SPEED =25)
-- 	),

ht1 AS (
	--Cross Sell Internet + Optik (2P Offer)
	--Non-TELUS/Koodo MOB Customers Only (Fibre Only) 
	--Eligible for customers with no internet AND Optik TV (No STV OR PikTV either) 
	--FIFA customers only 
	--Fibre customers only
	--Optik Eligible customers 
	--Not for Telus/Koodo Mobility Customers
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Cross-sell' AS Category ,
	'Internet + Optik' AS Subcategory   ,
	'202308 Get Internet + TV GWP Free TV' AS promo_seg   ,
	'9155352401613907466' AS offer_code ,
	DATETIME(2022, 02, 01, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk  


	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND HSIA_IND + STV_IND + OPTIK_TV_IND +PIK_TV_IND = 0
	AND (HSIA_MAX_SPD >15 OR HSIA_MAX_SPD = 15)
	AND OPTIK_ELIGIBLE > 0
	AND (MNH_MOB_BAN  = 0 OR MNH_MOB_BAN < 0)
	),

tv2 AS (
	--Cross Sell Optik TV 
	--Only HSIA-only customers with NO Telus Rewards
	--AB AND BC Only
	--FIFA Only 
	--No TV (STV, Optik TV, OR Pik TV) 
	--Have Internet 
	--Fibre Only 
	--Eligible for Optik TV 
	--No TELUS Rewards
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Cross-sell' AS Category ,
	'Optik' AS Subcategory   ,
	'202205 Add TV to HS - save $10' AS promo_seg   ,
	'9154252954313818263' AS offer_code ,
	DATETIME(2023, 01, 12, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk 

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND STV_IND + OPTIK_TV_IND +PIK_TV_IND = 0
	AND HSIA_IND > 0
	AND (HSIA_MAX_SPD >15 OR HSIA_MAX_SPD = 15)
	AND OPTIK_ELIGIBLE > 0
	AND (REWARDS_POINT_BALANCE=0 OR REWARDS_POINT_BALANCE is null)
	),

hs1 AS (
	--Cross Sell Internet
	--New Fibre FFH Customers w/ New Model (LWC, SWS, SHS, OR TOS) 
	--Exclusions 
	--AB AND BC Only
	--FIFA Only 
	--No internet, homephone, OR any TV
	--Fibre customers only
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Cross-sell' AS Category   ,
	'Internet' AS Subcategory   ,
	'202311 Add HS - Apple Watch GWP' AS promo_seg   ,
	'9167734447404313055' AS offer_code ,
	DATETIME(2023, 11, 14, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2024, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk 

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND STV_IND + OPTIK_TV_IND + PIK_TV_IND + HSIA_IND + HP_IND = 0
	AND TECH_GPON > 0
	),


tv3 AS (
	--Cross Sell TV 
	--Add Optik 4+1 for customers with Telus Rewards balance
	--HSIA 1P customers with Telus Rewards balance ONLY 
	--AB AND BC Only 
	--FIFA Only 
	--Have no TV (STV, Optik TV, OR Pik TV) 
	--Have Fibre Internet
	--Eligible for Optik 
	--Have Telus Rewards Balance
	
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Cross-sell' AS Category ,
	'Optik' AS Subcategory   ,
	'202301 Add TV AND Rewards 1P Offer' AS promo_seg ,
	'9164621219068675416' AS offer_code ,
	DATETIME(2023, 01, 12, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND STV_IND + OPTIK_TV_IND +PIK_TV_IND = 0
	AND HSIA_IND > 0
	AND (HSIA_MAX_SPD >15 OR HSIA_MAX_SPD = 15)
	AND OPTIK_ELIGIBLE > 0
	AND REWARDS_POINT_BALANCE>0
	),


tvu1 AS (
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Upsell' AS Category ,
	'Optik' AS Subcategory   ,
	'202303 YP8 upgrade to YP8&Movies' AS promo_seg ,
	'9142046828213433824' AS offer_code ,
	DATETIME(2023, 02, 24, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2024, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	1 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND OPTIK_TV_IND > 0
	AND UPPER(OPTIK_ALL_THEMEPACKS) not like '%NETFLIX%'
	AND OPTIK_PACKAGE_NM like  '%You Pick 8%' 
	AND OPTIK_PACKAGE_NM not like  '%You Pick 8 & Movies%'
	),

tvu2 AS (
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Upsell' AS Category ,
	'Optik' AS Subcategory   ,
	'202304 Add Xbox Game Pass Ultimate' AS promo_seg ,
	'9164370693367317641' AS offer_code ,
	DATETIME(2023, 04, 01, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	2 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND OPTIK_TV_IND > 0
	AND OPTIK_PACKAGE_NUM in (3,4)
	),

tvu3 AS (
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,
	'Upsell' AS Category ,
	'Optik' AS Subcategory   ,
	'202311 Upgrade Optik 2.0 You Pick 6' AS promo_seg ,
	'9142046828213433809' AS offer_code ,
	DATETIME(2023, 11, 08, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	3 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND OPTIK_TV_IND > 0
	AND OPTIK_PACKAGE_NUM in (2)
	AND UPPER(OPTIK_PACKAGE_NM) like  '%YOU PICK 6%' 
	),

tvu4 AS (
	SELECT 
	cust_id,
	bacct_num,
	fms_address_id,
	lpds_id,
	ACCT_START_DT AS candate,

	'Upsell' AS Category ,
	'Optik' AS Subcategory   ,
	'202311 Upgrade Optik 3.0 You Pick 8' AS promo_seg ,
	'9153358200213013887' AS offer_code ,
	DATETIME(2023, 11, 08, 00, 00, 00) AS ASSMT_VALID_START_TS , 
	DATETIME(2023, 12, 31, 23, 59, 59) AS ASSMT_VALID_END_TS,
	4 AS rk

	FROM `bi-srv-hsmdet-pr-7b9def.campaign_data.bq_dly_dbm_customer_profl`
	WHERE EX_STANDARD_EX = 0 AND cust_id not in (SELECT cust_id FROM std)
	AND SERV_PROV in ('AB','BC')
	AND PROVISIONING_SYSTEM = 'FIFA'
	AND OPTIK_TV_IND > 0
	AND OPTIK_PACKAGE_NUM in (2)
	AND UPPER(OPTIK_PACKAGE_NM) like  '%YOU PICK 8 & MOVIES%' 
	)

SELECT * FROM wfp1 WHERE wfp1.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND wfp1.ASSMT_VALID_END_TS  >=CURRENT_DATE()
UNION ALL
-- SELECT * FROM whsia4 WHERE whsia4.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND whsia4.ASSMT_VALID_END_TS >=CURRENT_DATE()
-- UNION ALL
SELECT * FROM ht1 WHERE ht1.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND ht1.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tv2 WHERE tv2.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tv2.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tv3 WHERE tv3.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tv3.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM hs1 WHERE hs1.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND hs1.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tvu1 WHERE tvu1.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tvu1.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tvu2 WHERE tvu2.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tvu2.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tvu3 WHERE tvu3.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tvu3.ASSMT_VALID_END_TS >=CURRENT_DATE()
UNION ALL
SELECT * FROM tvu4 WHERE tvu4.ASSMT_VALID_START_TS  <=CURRENT_DATE() AND tvu4.ASSMT_VALID_END_TS >=CURRENT_DATE()
