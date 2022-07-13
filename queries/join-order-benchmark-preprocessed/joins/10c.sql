SELECT MIN(chn.name) AS character, MIN(t.title) AS movie_with_american_producer
 FROM cn, company_type AS ct, movie_companies AS mc, t, role_type AS rt, char_name AS chn, ci, 
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND chn.id = ci.person_role_id
AND rt.id = ci.role_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
