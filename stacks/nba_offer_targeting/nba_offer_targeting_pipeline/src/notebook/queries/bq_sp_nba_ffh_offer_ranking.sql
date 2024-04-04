

CREATE OR REPLACE PROCEDURE nba_offer_targeting.bq_sp_nba_ffh_offer_ranking()

BEGIN 

	CREATE OR REPLACE TABLE nba_offer_targeting.nba_ffh_offer_ranking AS 

	SELECT * 
	FROM 
	(
	(SELECT * FROM nba_offer_targeting.nba_ffh_offer_ranking_existing) 
	UNION ALL
	(SELECT * FROM nba_offer_targeting.nba_ffh_offer_ranking_prospects) 
	UNION ALL 
	(SELECT * FROM nba_offer_targeting.nba_ffh_offer_ranking_cat3) 
	)
	; 

END
; 