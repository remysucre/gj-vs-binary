SELECT MIN(chn.name) AS uncredited_voiced_character, MIN(t.title) AS russian_movie
 FROM company_type AS ct, char_name AS chn, rt, ci, t, cn, movie_companies AS mc, 
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND chn.id = ci.person_role_id
AND rt.id = ci.role_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
;
