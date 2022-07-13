SELECT MIN(t.title) AS typical_european_movie
 FROM ct, t, mc, mi, info_type AS it, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND mc.movie_id = mi.movie_id
AND ct.id = mc.company_type_id
AND it.id = mi.info_type_id
;
