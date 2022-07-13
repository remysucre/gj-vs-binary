SELECT MIN(an1.name) AS costume_designer_pseudo, MIN(t.title) AS movie_with_costumes
 FROM title AS t, aka_name AS an1, cast_info AS ci, movie_companies AS mc, rt, name AS n1, cn, 
WHERE an1.person_id = n1.id
AND n1.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.role_id = rt.id
AND an1.person_id = ci.person_id
AND ci.movie_id = mc.movie_id
;
