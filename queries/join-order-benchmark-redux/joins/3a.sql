SELECT MIN(t.title) AS movie_title
 FROM movie_keyword AS mk, mi, t, k, 
WHERE t.id = mi.movie_id
AND t.id = mk.movie_id
AND mk.movie_id = mi.movie_id
AND k.id = mk.keyword_id
;
