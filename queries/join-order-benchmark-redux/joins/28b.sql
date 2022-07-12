SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie
 FROM mi, mi_idx, cn, kt, mc, k, movie_keyword AS mk, t, cct1, cct2, company_type AS ct, complete_cast AS cc, it1, it2, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mi_idx.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mi_idx.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mi_idx.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = mi_idx.movie_id
AND mc.movie_id = cc.movie_id
AND mi_idx.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND it2.id = mi_idx.info_type_id
AND ct.id = mc.company_type_id
AND cn.id = mc.company_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
