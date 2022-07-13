SELECT MIN(n.name) AS cast_member_name, MIN(pi.info) AS cast_member_info
 FROM t, it, pi, lt, movie_link AS ml, an, cast_info AS ci, n, 
WHERE n.id = an.person_id
AND n.id = pi.person_id
AND ci.person_id = n.id
AND t.id = ci.movie_id
AND ml.linked_movie_id = t.id
AND lt.id = ml.link_type_id
AND it.id = pi.info_type_id
AND pi.person_id = an.person_id
AND pi.person_id = ci.person_id
AND an.person_id = ci.person_id
AND ci.movie_id = ml.linked_movie_id
;
