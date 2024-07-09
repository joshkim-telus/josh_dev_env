CREATE OR REPLACE FUNCTION `divg-groovyhoon-pr-d2eab4.shs_churn.distinct_cnt_sf`(arr ANY TYPE) AS 

(

(SELECT COUNT(DISTINCT x) FROM UNNEST(arr) AS x)

)