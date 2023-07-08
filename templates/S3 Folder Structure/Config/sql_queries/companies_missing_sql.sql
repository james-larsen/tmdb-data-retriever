--Return a distinct list of TMDB company ID integers of missing companies referenced in the Title Production Company table
SELECT DISTINCT TPC.production_company_id
FROM tmdb_title_production_company TPC
WHERE NOT EXISTS
(
    SELECT 1
    FROM tmdb_production_company C
    WHERE TPC.production_company_id = C.production_company_id
);