SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book
 FROM movie_keyword AS mk, mc, link_type AS lt, k, t, movie_link AS ml, cn, ct, 
WHERE lt.id = ml.link_type_id
AND ml.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_type_id = ct.id
AND mc.company_id = cn.id
AND ml.movie_id = mk.movie_id
AND ml.movie_id = mc.movie_id
AND mk.movie_id = mc.movie_id
;
