

create or replace table `divg-josh-pr-d1cc3a.tos_crosssell.bans_tos_orders_202208`
as 
select BILL_ACCOUNT_NUMBER
from `divg-churn-analysis-pr-7e40f6.SAStoGCP.TOScxsell`
where I_DT between "2022-08-01" and "2022-08-31"
and PRODUCT = "TOS"




sum(if ([Rgu Count Before] > 0  
and [CHURN_TOTAL]>0 
and ([CHURN_TOTAL]>=[Rgu Count Before])
or  [invol_deact_flag]=1
) then 1 else 0 END)