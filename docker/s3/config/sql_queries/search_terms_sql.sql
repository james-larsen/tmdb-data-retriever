--Return a distinct list of strings (aliased as "search_term") that should be used for title name searching
SELECT DISTINCT M.movie_title AS search_term
FROM my_movies M
WHERE length(M.movie_title) >= 15
ORDER BY M.movie_title;