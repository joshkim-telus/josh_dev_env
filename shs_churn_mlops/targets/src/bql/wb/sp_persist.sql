
CREATE OR REPLACE PROCEDURE shs_churn.sp_persist_shs_churn(_from_dt DATE, _to_dt DATE, _date_trunc_day_week_month STRING)
 
BEGIN

/* 
----------------------------------------------------------------------------------------------------
DESCRIPTION
Persist Home Solutions Enroll (Acquisition) Targets.
SP will automatically detect training/predict mode based on the from/to date parameters. When they are the same date, this is Prediction mode.
There can only be 1 set of Training Data per Model Scenario.
There can be multiple sets of Prediction Data per Model Scenario.

INPUT PARAMETERS
- _from_dt DATE: date from which to start the target period
- _to_dt DATE: date to which to end the target period
- _date_trunc_day_week_month STRING: day, week, month

TARGET TABLE
- {dataset_id}.master_target_table

SOURCE TABLES
- {dataset_id}.`{stack_name}`_label1_tf()
- {dataset_id}.{stack_name}_label0_tf()

VARIABLES TO REPLACE: 
- {stack_name}
- {dataset_id}
----------------------------------------------------------------------------------------------------
*/

####################################################################################################
# Declare Vars
####################################################################################################

DECLARE _model_scenario STRING;
DECLARE _label_1_sql STRING;
DECLARE _label_0_sql STRING;
DECLARE _training_mode INT64;

SET _training_mode = IF(_from_dt <> _to_dt, 1, 0);

####################################################################################################
# Set Dynamic SQL 
####################################################################################################

SET _label_1_sql = 
  """
  CREATE OR REPLACE TEMPORARY TABLE stage_label_1 AS 
  SELECT CASE WHEN UPPER('<date_trunc_type>') = 'DAY'
              THEN 'DAY'
              WHEN UPPER('<date_trunc_type>') = 'WEEK'
              THEN 'WEEK'
              ELSE 'MONTH'
              END AS prediction_period, 
         CASE WHEN UPPER('<date_trunc_type>') = 'DAY'
              THEN _ref_dt
              WHEN UPPER('<date_trunc_type>') = 'WEEK'
              THEN DATE_TRUNC(_ref_dt, WEEK)
              ELSE DATE_TRUNC(_ref_dt, MONTH)
              END AS ref_dt,
         cust_id,
         cust_src_id, 
         ban,
         ban_src_id,
         target_ind AS label, 
         _ref_dt AS label_dt 
    FROM stage_shs_churn_label1;
  """;

SET _label_0_sql =
  """
  CREATE OR REPLACE TEMPORARY TABLE stage_label_0 AS 
  SELECT CASE WHEN UPPER('<date_trunc_type>') = 'DAY'
              THEN 'DAY'
              WHEN UPPER('<date_trunc_type>') = 'WEEK'
              THEN 'WEEK'
              ELSE 'MONTH'
              END AS prediction_period, 
         CASE WHEN from_dt = to_dt /*for label 0, if from & to date are same, then set ref_dt as label date */
                OR UPPER('<date_trunc_type>') = 'DAY' 
              THEN _ref_dt
              WHEN UPPER('<date_trunc_type>') = 'WEEK'
              THEN DATE_TRUNC(_ref_dt, WEEK)
              ELSE DATE_TRUNC(_ref_dt, MONTH)
              END AS ref_dt,
         cust_id,
         cust_src_id, 
         ban,
         ban_src_id,
         target_ind AS label, 
         _ref_dt AS label_dt
    FROM stage_shs_churn_label0 
   WHERE target_ind = 0;
  """;

####################################################################################################
# Replace variables in dynamic sql
####################################################################################################

# Replace Date Trunc Placeholders
IF UPPER(_date_trunc_day_week_month) IN ('DAY', 'WEEK', 'MONTH') THEN 
  SET _label_1_sql = REPLACE(_label_1_sql, '<date_trunc_type>', UPPER(_date_trunc_day_week_month));
  SET _label_0_sql = REPLACE(_label_0_sql, '<date_trunc_type>', UPPER(_date_trunc_day_week_month));
ELSE
  RAISE USING MESSAGE = '_date_trunc_day_week_month must be: day, week, month';
END IF;

####################################################################################################
# Run Dynamic SQL
####################################################################################################
EXECUTE IMMEDIATE _label_1_sql;
EXECUTE IMMEDIATE _label_0_sql;

####################################################################################################
# Stage Data - Eliminate Label 0 if it exists in Label 1 set
####################################################################################################
CREATE OR REPLACE TEMPORARY TABLE stage_data AS 
SELECT prediction_period, 
       ref_dt,
       cust_id,
       cust_src_id,
       ban,
       ban_src_id, 
       label,
       label_dt, -- 1st of each month
  FROM stage_label_1
 WHERE _training_mode = 1
 UNION ALL
SELECT prediction_period, 
       ref_dt,
       cust_id,
       cust_src_id,
       ban,
       ban_src_id, 
       label,
       label_dt, -- 1st of each month
  FROM stage_label_0 AS lbl0
 WHERE NOT EXISTS
       (
        SELECT 1
          FROM stage_label_1 AS lbl1
         WHERE CAST(lbl0.cust_id AS INT64) = CAST(lbl1.cust_id AS INT64)
           AND CAST(lbl0.ban AS INT64) = CAST(lbl1.ban AS INT64)
           AND _training_mode = 1
       );
       

####################################################################################################
# Set Split Type
####################################################################################################
CREATE OR REPLACE TEMPORARY TABLE set_split_type AS 
WITH 
get_distinct_ref_dt AS (
SELECT DISTINCT
       ref_dt
  FROM stage_data
),
calc_ntile AS (
SELECT ref_dt,
       NTILE(12) OVER(ORDER BY ref_dt DESC) AS tile --change this to 10 if you want 2 months test & val each
  FROM get_distinct_ref_dt
)
SELECT ref_dt,
       CASE WHEN _training_mode = 0 THEN '4-predict'
            WHEN tile <= 1 THEN '3-test'
            WHEN tile <= 2 THEN '2-val'
            ELSE '1-train'
            END AS split_type
  FROM calc_ntile;

####################################################################################################
# Insert into table
####################################################################################################
-- DELETE FROM shs_churn.master_target_table
--  WHERE training_mode = _training_mode
--    AND (
--          ( /* for prediction mode, delete only records for the day loaded */
--            _training_mode = 0
--            AND from_dt = _from_dt
--            AND to_dt = _to_dt
--          )
--          OR
--          ( /* for training mode, delete all records */
--            _training_mode = 1
--          )
--        );

-- INSERT INTO shs_churn.master_target_table
-- (
--   from_dt,
--   to_dt,
--   training_mode,
--   split_type,
--   prediction_period,
--   ref_dt,
--   cust_id,
--   cust_src_id, 
--   ban,
--   ban_src_id,
--   label,
--   label_dt
-- )

CREATE OR REPLACE TABLE shs_churn.master_target_table AS

SELECT _from_dt AS from_dt,
       _to_dt AS to_dt,
       _training_mode AS training_mode,
       split.split_type,
       base.prediction_period, 
       base.ref_dt,
       base.cust_id,
       base.cust_src_id,
       base.ban,
       base.ban_src_id,
       base.label,
       base.label_dt, -- 1st of each month
  FROM stage_data AS base
 INNER JOIN set_split_type AS split
    ON split.ref_dt = base.ref_dt;

####################################################################################################
# Exception Handling
####################################################################################################
EXCEPTION WHEN ERROR THEN
  SELECT @@error.message, @@error.statement_text, @@error.formatted_stack_trace;

END
; 












