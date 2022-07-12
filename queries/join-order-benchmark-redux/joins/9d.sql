SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress, MIN(t.title) AS american_movie
 FROM cn, char_name AS chn, aka_name AS an, title AS t, rt, movie_companies AS mc, n, ci, 
WHERE ci.movie_id = t.id
AND t.id = mc.movie_id
AND ci.movie_id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND n.id = ci.person_id
AND chn.id = ci.person_role_id
AND an.person_id = n.id
AND an.person_id = ci.person_id
;
