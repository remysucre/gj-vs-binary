SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS complete_gore_movie
 FROM ci, it1, it2, n, t, cct1, complete_cast AS cc, cct2, movie_keyword AS mk, k, mi, movie_info_idx AS mi_idx, 
WHERE t.id = mi.movie_id
AND t.id = mi_idx.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND ci.movie_id = mi.movie_id
AND ci.movie_id = mi_idx.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND mi_idx.movie_id = mk.movie_id
AND mi_idx.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND n.id = ci.person_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
