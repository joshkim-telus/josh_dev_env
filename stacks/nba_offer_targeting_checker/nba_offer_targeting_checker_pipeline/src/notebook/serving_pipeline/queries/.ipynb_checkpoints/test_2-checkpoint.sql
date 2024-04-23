--2) each customer should have only one ranking number assigned for each offer. please advise if you find any case where a customer has 2 same rankings (e.g. ranking == 2) for two different offers
SELECT 
  cust_id,
  mobility_ban,
  lpds_id,
  COUNT(CASE WHEN ranking = 1 THEN ranking ELSE NULL END) as ranking_1,
  COUNT(CASE WHEN ranking = 2 THEN ranking ELSE NULL END) as ranking_2,
  COUNT(CASE WHEN ranking = 3 THEN ranking ELSE NULL END) as ranking_3,
  COUNT(CASE WHEN ranking = 4 THEN ranking ELSE NULL END) as ranking_4,
  COUNT(CASE WHEN ranking = 5 THEN ranking ELSE NULL END) as ranking_5,
  COUNT(CASE WHEN ranking = 6 THEN ranking ELSE NULL END) as ranking_6,
  COUNT(CASE WHEN ranking = 7 THEN ranking ELSE NULL END) as ranking_7,
  COUNT(CASE WHEN ranking = 8 THEN ranking ELSE NULL END) as ranking_8,
  COUNT(CASE WHEN ranking = 9 THEN ranking ELSE NULL END) as ranking_9,
  COUNT(CASE WHEN ranking = 10 THEN ranking ELSE NULL END) as ranking_10
FROM 
  nba_offer_targeting_np.nba_ffh_offer_ranking
GROUP BY
  cust_id,
  mobility_ban,
  lpds_id
HAVING
  ranking_1 > 1 OR
  ranking_2 > 1 OR
  ranking_3 > 1 OR
  ranking_4 > 1 OR
  ranking_5 > 1 OR
  ranking_6 > 1 OR
  ranking_7 > 1 OR
  ranking_8 > 1 OR
  ranking_9 > 1 OR
  ranking_10 > 1
