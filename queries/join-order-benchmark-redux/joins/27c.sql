SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
 FROM ct, movie_link AS ml, complete_cast AS cc, cn, k, mc, mi, movie_keyword AS mk, cct1, t, cct2, lt, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND mi.movie_id = t.id
AND t.id = cc.movie_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
AND ml.movie_id = mi.movie_id
AND mk.movie_id = mi.movie_id
AND mc.movie_id = mi.movie_id
AND ml.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = cc.movie_id
;
