SELECT MIN(an1.name) AS actress_pseudonym, MIN(t.title) AS japanese_movie_dubbed
 FROM n1, aka_name AS an1, cn, ci, mc, rt, title AS t, 
WHERE an1.person_id = n1.id
AND n1.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an1.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
