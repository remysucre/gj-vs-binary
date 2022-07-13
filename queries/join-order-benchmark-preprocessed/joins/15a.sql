SELECT MIN(mi.info) AS release_date, MIN(t.title) AS internet_movie
 FROM aka_title AS at, mc, mi, movie_keyword AS mk, it1, cn, keyword AS k, t, company_type AS ct, 
WHERE t.id = at.movie_id
AND t.id = mi.movie_id
AND t.id = mk.movie_id
AND t.id = mc.movie_id
AND mk.movie_id = mi.movie_id
AND mk.movie_id = mc.movie_id
AND mk.movie_id = at.movie_id
AND mi.movie_id = mc.movie_id
AND mi.movie_id = at.movie_id
AND mc.movie_id = at.movie_id
AND k.id = mk.keyword_id
AND it1.id = mi.info_type_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
