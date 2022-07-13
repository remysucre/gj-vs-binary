SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie
 FROM mi, complete_cast AS cc, keyword AS k, movie_keyword AS mk, movie_companies AS mc, cct1, company_type AS ct, it1, t, kt, cn, 
WHERE kt.id = t.kind_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = cc.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = cc.movie_id
AND mc.movie_id = cc.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND cct1.id = cc.status_id
;
