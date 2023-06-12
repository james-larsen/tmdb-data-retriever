--Return a distinct list of TMDB Movie ID integers and "T/F" values representing adult content of titles with no keywords
SELECT DISTINCT tmdb_id, adult_flag
FROM tmdb_title T
WHERE NOT EXISTS
(
    SELECT 1
    FROM tmdb_title_keyword TK
    WHERE TK.tmdb_id = T.tmdb_id
);