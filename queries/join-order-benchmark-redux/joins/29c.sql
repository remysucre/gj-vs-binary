SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation
 FROM person_info AS pi, n, ci, k, it3, char_name AS chn, it, mi, rt, cn, t, movie_keyword AS mk, cct1, movie_companies AS mc, cct2, aka_name AS an, complete_cast AS cc, 
WHERE t.id = mi.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mk.movie_id
AND t.id = cc.movie_id
AND mc.movie_id = ci.movie_id
AND mc.movie_id = mi.movie_id
AND mc.movie_id = mk.movie_id
AND mc.movie_id = cc.movie_id
AND mi.movie_id = ci.movie_id
AND mi.movie_id = mk.movie_id
AND mi.movie_id = cc.movie_id
AND ci.movie_id = mk.movie_id
AND ci.movie_id = cc.movie_id
AND mk.movie_id = cc.movie_id
AND cn.id = mc.company_id
AND it.id = mi.info_type_id
AND n.id = ci.person_id
AND rt.id = ci.role_id
AND n.id = an.person_id
AND ci.person_id = an.person_id
AND chn.id = ci.person_role_id
AND n.id = pi.person_id
AND ci.person_id = pi.person_id
AND it3.id = pi.info_type_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
