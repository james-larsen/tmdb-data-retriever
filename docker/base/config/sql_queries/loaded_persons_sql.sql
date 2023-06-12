--Return a distinct list of TMDB Person ID integers of titles already loaded to the Person table
SELECT DISTINCT P.person_id, P.adult_flag
FROM nexus.tmdb_person P;