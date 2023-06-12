--Return a distinct list of strings (aliased as "search_term") that should be used for title name searching
SELECT DISTINCT PM.title AS search_term
FROM nexus.plex_movie PM
WHERE length(PM.title) >= 15
ORDER BY PM.title;