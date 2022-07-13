SELECT MIN(n.name) AS member_in_charnamed_movie, MIN(n.name) AS a1
 FROM title AS t, k, movie_companies AS mc, movie_keyword AS mk, company_name AS cn, cast_info AS ci, n, 
WHERE n.id = ci.person_id
AND ci.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND t.id = mc.movie_id
AND mc.company_id = cn.id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mc.movie_id = mk.movie_id
;
