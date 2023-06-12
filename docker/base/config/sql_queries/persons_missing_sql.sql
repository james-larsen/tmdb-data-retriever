--Return a distinct list of TMDB Person ID integers and "T/F" values representing adult content of missing persons referenced in the Title Cast table
SELECT TC.person_id, min(TC.adult_flag) AS adult_flag
FROM nexus.tmdb_title_cast TC
WHERE NOT EXISTS
(
    SELECT 1
    FROM nexus.tmdb_person P
    WHERE TC.person_id = P.person_id
)
    AND known_for_department IN ('Acting', 'Directing', 'Creator')
    AND cast_order <= 10
GROUP BY TC.person_id;