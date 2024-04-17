-- promo_seg field cannot be empty in any row
SELECT 
  * 
FROM 
  nba_offer_targeting_np.nba_ffh_offer_ranking
WHERE
  CHAR_LENGTH(promo_seg) < 6 or promo_seg is null

