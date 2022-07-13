SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS male_writer, MIN(t.title) AS violent_movie_title
 FROM ci, movie_info_idx AS mi_idx, it1, mi, n, t, movie_keyword AS mk, it2, k, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi_idx.movie_id = mk.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
;
