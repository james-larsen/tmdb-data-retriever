--Return a distinct list of TMDB Movie ID integers of titles already loaded to the Title Cast table
SELECT DISTINCT TC.tmdb_id
FROM nexus.tmdb_title_cast TC;