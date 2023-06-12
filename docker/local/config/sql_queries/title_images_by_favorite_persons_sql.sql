WITH CTE1 AS 
(
	SELECT T.title_name, P.person_name, PF.priority, TI.tmdb_id, TI.image_type, TI.local_file_path, K.keyword_name
	FROM nexus.tmdb_title_image TI
	JOIN nexus.tmdb_title_cast TC ON TC.tmdb_id = TI.tmdb_id
	JOIN nexus.tmdb_title T ON T.tmdb_id = TC.tmdb_id
	JOIN nexus.tmdb_person P ON P.person_id = TC.person_id
	LEFT JOIN nexus.tmdb_title_keyword TK ON TK.tmdb_id = TC.tmdb_id
	LEFT JOIN nexus.tmdb_keyword K ON K.keyword_id = TK.keyword_id
	LEFT JOIN nexus.tmdb_person_favorite PF ON PF.person_id = P.person_id
	WHERE 1 = 1
	  AND PF.person_id IS NOT NULL
)
SELECT A.tmdb_id, A.title_name, A.image_type, D.has_backdrop, A.local_file_path, min(person_list_display) AS person_list, keyword_list
FROM CTE1 A
JOIN
(
	SELECT tmdb_id, 
	string_agg(person_name, '; ' ORDER BY priority, person_name) AS person_list, 
	string_agg(person_name, '; ' ORDER BY person_name) AS person_list_display
	FROM
	(
		SELECT DISTINCT tmdb_id, person_name, priority
		FROM CTE1
	) A
	GROUP BY tmdb_id
) B ON B.tmdb_id = A.tmdb_id
LEFT JOIN
(
	SELECT tmdb_id, local_file_path, 
	ROW_NUMBER() OVER (PARTITION BY tmdb_id) AS row_num
	FROM 
	(
		SELECT DISTINCT tmdb_id, local_file_path
		FROM CTE1
		WHERE image_type = 'poster'
	) A
) C ON C.tmdb_id = A.tmdb_id AND C.local_file_path = A.local_file_path AND C.row_num = 1
JOIN
(
	SELECT tmdb_id, max(CASE WHEN image_type = 'backdrop' THEN 'Y' ELSE 'N' END) AS has_backdrop
	FROM CTE1
	GROUP BY tmdb_id
) D ON D.tmdb_id = A.tmdb_id
LEFT JOIN
(
	SELECT tmdb_id, 
	string_agg(keyword_name, '; ' ORDER BY keyword_name) AS keyword_list
	FROM
	(
		SELECT DISTINCT tmdb_id, keyword_name
		FROM CTE1
		WHERE keyword_name <> 'no keywords'
	) A
	GROUP BY tmdb_id
) E ON E.tmdb_id = A.tmdb_id
WHERE A.image_type = 'backdrop' OR (C.local_file_path IS NOT NULL AND A.image_type = 'poster' AND C.row_num = 1)
GROUP BY A.tmdb_id, A.title_name, A.image_type, D.has_backdrop, A.local_file_path, person_list, keyword_list
ORDER BY 
--min(B.person_list_display), 
B.person_list, 
min(A.priority), min(A.person_name), title_name, image_type DESC;