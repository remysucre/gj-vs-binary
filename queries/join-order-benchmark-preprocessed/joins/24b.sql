SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS kung_fu_panda
 FROM n, char_name AS chn, k, aka_name AS an, rt, cn, ci, t, movie_keyword AS mk, it, mi, movie_companies AS mc, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND ci.movie_id = mk.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND k.id = mk.keyword_id
;
