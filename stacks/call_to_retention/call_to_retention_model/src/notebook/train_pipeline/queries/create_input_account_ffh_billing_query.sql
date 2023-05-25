-- GET FIRST DAY 6 MONTHS BACK AND LAST DAY OF PRIOR MONTH
          WITH cte_6mth_period AS (
            SELECT DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y-%m-%d', "{v_report_date}"), INTERVAL 6 MONTH), MONTH) AS period_start_date,
                   LAST_DAY(DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y-%m-%d', "{v_report_date}"), INTERVAL 1 MONTH), MONTH)) AS period_end_date
          ),

          -- CREATE TEMP DATE DIM
          cte_date AS (
              SELECT EXTRACT(YEAR FROM my_date)||'-'||LPAD(CAST(EXTRACT(MONTH FROM my_date) AS STRING), 2, '0') AS year_month
                FROM (
                      SELECT DATE_ADD((SELECT period_start_date FROM cte_6mth_period), INTERVAL param MONTH) AS my_date
                        FROM UNNEST(GENERATE_ARRAY(0, 5, 1)) AS param
                     )
          ),

          -- BAN INVOICE AMOUNT FROM LAST 6 MONTHS
          cte_stage_billing AS (
              SELECT bill.billg_acct_num AS ban,
                     EXTRACT(YEAR FROM bill.bill_dt)||'-'||LPAD(CAST(EXTRACT(MONTH FROM bill.bill_dt) AS STRING), 2, '0') AS bill_year_month,
                     bill.tot_inv_amt
                FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_inv_sum_view` AS bill
               CROSS JOIN cte_6mth_period AS dt
               WHERE bill.bill_dt BETWEEN dt.period_start_date AND dt.period_end_date
                 AND EXISTS --Filter for HS only
                     (
                         SELECT 1
                           FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` AS prod
                          WHERE prod.pi_prod_instnc_typ_cd IN ('SING','HSIC','TTV','SMHM')
                            AND prod.BACCT_BUS_BACCT_NUM = bill.billg_acct_num
                            AND DATE(prod.prod_instnc_ts) = dt.period_end_date
                     )
          ),

          -- CROSS JOIN WITH DIM DATE TO FILL BLANK MONTHS
          cte_billing AS (
              SELECT base.ban,
                     dt.year_month,
                     MAX(CASE WHEN dt.year_month = base.bill_year_month THEN base.tot_inv_amt
                              ELSE NULL
                              END) AS ffh_amt
                FROM cte_stage_billing AS base
               CROSS JOIN cte_date AS dt
               GROUP BY
                     base.ban,
                     dt.year_month
          ),

          -- GENERATE MONTHS BACK RANK
          cte_rank_billing AS (
              SELECT ban,
                     ffh_amt,
                     RANK() OVER(PARTITION BY ban ORDER BY year_month DESC) -1 AS months_back
                FROM cte_billing
          )
		  -- GET LAST 6 MONTH BILL AMOUNTS PER BAN
		  SELECT ban,
				 SUM(CASE WHEN months_back = 0 THEN ffh_amt ELSE 0 END) AS ffh_amt_0,
				 SUM(CASE WHEN months_back = 1 THEN ffh_amt ELSE 0 END) AS ffh_amt_1,
				 SUM(CASE WHEN months_back = 2 THEN ffh_amt ELSE 0 END) AS ffh_amt_2,
				 SUM(CASE WHEN months_back = 3 THEN ffh_amt ELSE 0 END) AS ffh_amt_3,
				 SUM(CASE WHEN months_back = 4 THEN ffh_amt ELSE 0 END) AS ffh_amt_4,
				 SUM(CASE WHEN months_back = 5 THEN ffh_amt ELSE 0 END) AS ffh_amt_5
			FROM cte_rank_billing
		   GROUP BY ban
		   ;