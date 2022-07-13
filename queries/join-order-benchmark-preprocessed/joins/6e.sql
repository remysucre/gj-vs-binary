SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS marvel_movie
 FROM k, t, cast_info AS ci, n, movie_keyword AS mk, 
WHERE k.id = mk.keyword_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mk.movie_id
AND n.id = ci.person_id
;
