
--qry_arpu_target_date--
--ARPU OF TARGET CUSTOMERS AS OF "target_date"--

DECLARE target_date_start DATE DEFAULT "2023-01-01";
DECLARE target_date_end DATE DEFAULT "2023-02-01";
DECLARE interval_days INT64 DEFAULT 0;

WITH YR_MTH AS (
SELECT EXTRACT(YEAR FROM target_date_end)||'-'||LPAD(CAST(EXTRACT(MONTH FROM target_date_end) AS STRING), 2, '0') AS yr_mth
)

SELECT (SELECT yr_mth FROM YR_MTH) AS year_month
, A.camp_id
, COUNT(DISTINCT A.ban) as ban_count
, SUM(A.tot_inv_amt) as total_inv_amt
, SUM(A.tot_inv_amt)/COUNT(A.ban) as ARPU

FROM 
(
SELECT a.ban 
, b.camp_id
, sum(tot_inv_amt) as tot_inv_amt
FROM (
  SELECT ban, 
         SUM(tot_inv_amt) as tot_inv_amt
  FROM 
  (
    SELECT billg_acct_num AS ban,
          EXTRACT(YEAR FROM bill_dt)||'-'||LPAD(CAST(EXTRACT(MONTH FROM bill_dt) AS STRING), 2, '0') AS yr_mth,
          MAX(bill_dt) OVER(PARTITION BY billg_acct_num) AS latest_bill_dt,
          bacct_billg_cycl_cd AS bill_cycle_day,
          bill_doc_id,
          tot_inv_amt,
          tot_tax_inv_amt,
          RANK() OVER(PARTITION BY billg_acct_num, bill_dt ORDER BY bill_doc_id DESC) AS rnk_dup_bill
      FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_inv_sum_view` 
    WHERE bill_dt between target_date_start AND target_date_end 
    QUALIFY rnk_dup_bill = 1
  )
  GROUP BY ban
    ) a 
INNER JOIN  `divg-josh-pr-d1cc3a.campaign_performance_analysis.ob_q4_bans` b 
ON a.ban = b.ban 

GROUP BY a.ban 
, b.camp_id
) A

GROUP BY A.camp_id

ORDER BY A.camp_id