--Return a distinct list of TMDB Movie ID integers of titles already loaded to the Titles table
SELECT DISTINCT T.tmdb_id, T.adult_flag
FROM nexus.tmdb_title T;