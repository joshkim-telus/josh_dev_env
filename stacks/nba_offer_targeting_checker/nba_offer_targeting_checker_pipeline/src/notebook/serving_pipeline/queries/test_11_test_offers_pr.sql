
--11) checks if there are any rows with empty offer_name in test offers pr table
SELECT * 
FROM `bi-stg-mobilityds-pr-db8ce2.nba_offer_targeting.nba_offer_targeting_test_offers_pr` 
WHERE offer_name is null 
or trim(offer_name) = ""
