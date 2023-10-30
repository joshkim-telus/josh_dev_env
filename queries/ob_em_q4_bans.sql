
----------------------------------------------------------------------------------------
----------------------------------Jan 2022 to Apr 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.ob_q4`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
GROUP BY CAMP_ID
, BACCT_NUM
; 

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.em_q4_bans`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.em_q4`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
GROUP BY CAMP_ID
, BACCT_NUM
; 

----------------------------------------------------------------------------------------
----------------------------------Feb 2022 to May 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
AND CAMP_INHOME BETWEEN "2022-02-01" AND "2022-04-30"
GROUP BY CAMP_ID
, BACCT_NUM
; 


----------------------------------------------------------------------------------------
----------------------------------Jan 2022 to Mar 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_q1`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
AND CAMP_INHOME BETWEEN "2022-01-01" AND "2022-03-31"
GROUP BY CAMP_ID
, BACCT_NUM
; 


----------------------------------------------------------------------------------------
----------------------------------Apr 2022 to June 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_q2`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
AND CAMP_INHOME BETWEEN "2022-04-01" AND "2022-06-30"
GROUP BY CAMP_ID
, BACCT_NUM
; 


----------------------------------------------------------------------------------------
----------------------------------Jul 2022 to Sep 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_q3`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
AND CAMP_INHOME BETWEEN "2022-07-01" AND "2022-09-30"
GROUP BY CAMP_ID
, BACCT_NUM
; 


----------------------------------------------------------------------------------------
----------------------------------Oct 2022 to Dec 2022----------------------------------
----------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans_q4`
AS
SELECT CAMP_ID
, BACCT_NUM as ban
FROM `divg-churn-analysis-pr-7e40f6.SAStoGCP.MCXPEXPXPMTM2022`
WHERE CAMP_ID IN ("MCX", "MTM", "PEX", "PXP")
AND CAMP_INHOME BETWEEN "2022-10-01" AND "2022-12-31"
GROUP BY CAMP_ID
, BACCT_NUM
; 
