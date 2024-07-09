

-- CREATE OR REPLACE PROCEDURE ml_made_easy.sp_persist_stack_name_targets(_from_dt DATE, _to_dt DATE)

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
CREATE OR REPLACE TEMPORARY TABLE stage_{stack_name}_label0 AS
SELECT * FROM `{project_id}.{dataset_id}.{stack_name}_label0_tf`(_from_dt, _to_dt);

CREATE OR REPLACE TEMPORARY TABLE stage_{stack_name}_label1 AS
SELECT * FROM `{project_id}.{dataset_id}.{stack_name}_label1_tf`(_from_dt, _to_dt);

-- Persist HS Enroll Targets for daily predictions
CALL {dataset_id}.sp_persist_{stack_name}(_from_dt, _to_dt, 'month');

####################################################################################################
# Exception Handling
####################################################################################################
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message, @@error.statement_text, @@error.formatted_stack_trace;

END
; 



