SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie
 FROM cn, mi, it1, mi_idx, company_type AS ct, it2, kt, mc, t, movie_keyword AS mk, k, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
;
