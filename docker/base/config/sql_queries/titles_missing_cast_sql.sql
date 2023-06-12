--Return a distinct list of TMDB Movie ID integers and "T/F" values representing adult content of titles with no cast information
SELECT DISTINCT tmdb_id, adult_flag
FROM nexus.tmdb_title T
WHERE NOT EXISTS
(
    SELECT 1
    FROM nexus.tmdb_title_cast TC
    WHERE TC.tmdb_id = T.tmdb_id
);