--Return a distinct list of TMDB Person ID integers and "T/F" values representing adult content of people considered "favorite"
SELECT PF.person_id, min(P.adult_flag) AS adult_flag
FROM nexus.tmdb_person_favorite PF
JOIN nexus.tmdb_person P ON P.person_id = PF.person_id
GROUP BY PF.person_id;