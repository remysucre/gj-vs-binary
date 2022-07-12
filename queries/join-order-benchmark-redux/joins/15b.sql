SELECT MIN(mi.info) AS release_date, MIN(t.title) AS youtube_movie
 FROM aka_title AS at, mc, it1, mi, movie_keyword AS mk, t, cn, keyword AS k, company_type AS ct, 
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
