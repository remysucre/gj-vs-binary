SELECT MIN(mi.info) AS release_date, MIN(miidx.info) AS rating, MIN(t.title) AS german_movie
 FROM cn, movie_info AS mi, movie_info_idx AS miidx, ct, title AS t, it2, kt, movie_companies AS mc, it, 
WHERE mi.movie_id = t.id
AND it2.id = mi.info_type_id
AND kt.id = t.kind_id
AND mc.movie_id = t.id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND miidx.movie_id = t.id
AND it.id = miidx.info_type_id
AND mi.movie_id = miidx.movie_id
AND mi.movie_id = mc.movie_id
AND miidx.movie_id = mc.movie_id
;
