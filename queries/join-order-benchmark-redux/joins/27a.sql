SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel
 FROM cct1, movie_link AS ml, mc, lt, t, cct2, cn, ct, mi, complete_cast AS cc, k, movie_keyword AS mk, 
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
