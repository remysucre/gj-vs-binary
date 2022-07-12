SELECT MIN(mi_idx.info) AS rating, MIN(t.title) AS north_european_dark_production
 FROM it2, k, t, movie_keyword AS mk, mi, it1, kt, mi_idx, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mi.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
;
