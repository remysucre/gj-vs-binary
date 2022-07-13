SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS voiced_action_movie_jap_eng
 FROM ci, movie_keyword AS mk, rt, movie_companies AS mc, cn, n, char_name AS chn, it, k, mi, t, aka_name AS an, 
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
