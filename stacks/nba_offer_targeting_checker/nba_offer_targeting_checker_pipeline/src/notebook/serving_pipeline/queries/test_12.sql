--12) checks if there are any rows with empty offer_name in test offers np table
SELECT * 
FROM `nba_offer_targeting_np.nba_offer_targeting_test_offers_np` 
WHERE offer_name is null 
or trim(offer_name) = ""
