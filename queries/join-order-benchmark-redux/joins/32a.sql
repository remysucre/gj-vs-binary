SELECT MIN(lt.link) AS link_type, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie
 FROM title AS t1, movie_link AS ml, k, movie_keyword AS mk, title AS t2, link_type AS lt, 
WHERE mk.keyword_id = k.id
AND t1.id = mk.movie_id
AND ml.movie_id = t1.id
AND ml.linked_movie_id = t2.id
AND lt.id = ml.link_type_id
AND mk.movie_id = t1.id
;
