

CREATE OR REPLACE PROCEDURE shs_churn.sp_persist_shs_churn_targets(_from_dt DATE, _to_dt DATE)

BEGIN
/* 
----------------------------------------------------------------------------------------------------
DESCRIPTION
Persist all Targets for Predictions into Target Table.

INPUT PARAMETERS
- _from_dt DATE: date from which to start the target period
- _to_dt DATE: date to which to end the target period

TARGET TABLE
- nba_targets.master_target_table

SOURCES
- nba_targets.sp_persist_hs_enroll()

VARIABLES TO REPLACE: 
- {stack_name}
- {project_id}
- {dataset_id}
----------------------------------------------------------------------------------------------------
*/

SET _from_dt = COALESCE(_from_dt, CURRENT_DATE('America/Vancouver')-1);
SET _to_dt = COALESCE(_to_dt, CURRENT_DATE('America/Vancouver')-1);

-- Prestep - Stage Label 0 and Label 0 table functions
CREATE OR REPLACE TEMPORARY TABLE stage_shs_churn_label0 AS
SELECT * FROM `divg-groovyhoon-pr-d2eab4.shs_churn.shs_churn_label0_tf`(_from_dt, _to_dt);

CREATE OR REPLACE TEMPORARY TABLE stage_shs_churn_label1 AS
SELECT * FROM `divg-groovyhoon-pr-d2eab4.shs_churn.shs_churn_label1_tf`(_from_dt, _to_dt);

-- Persist HS Enroll Targets for daily predictions
CALL shs_churn.sp_persist_shs_churn(_from_dt, _to_dt, 'month');

####################################################################################################
# Exception Handling
####################################################################################################
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message, @@error.statement_text, @@error.formatted_stack_trace;

END
; 

CALL shs_churn.sp_persist_shs_churn_targets("2023-01-01", "2023-12-31")
; 

SELECT ref_dt
, count(distinct ban)
, count(*)
FROM `divg-groovyhoon-pr-d2eab4.shs_churn.master_target_table`
GROUP BY 1
ORDER BY 1


