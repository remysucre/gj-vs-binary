SELECT MIN(n.name) AS voicing_actress, MIN(t.title) AS kung_fu_panda
 FROM cn, mi, t, ci, rt, char_name AS chn, aka_name AS an, mc, it, n, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mi.movie_id = ci.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
;
