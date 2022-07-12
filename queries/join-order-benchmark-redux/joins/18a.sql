SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title
 FROM ci, title AS t, it2, it1, movie_info AS mi, movie_info_idx AS mi_idx, n, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
