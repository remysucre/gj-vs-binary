SELECT MIN(an.name) AS acress_pseudonym, MIN(t.title) AS japanese_anime_movie
 FROM mc, aka_name AS an, rt, cn, ci, n, t, 
WHERE an.person_id = n.id
AND n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
