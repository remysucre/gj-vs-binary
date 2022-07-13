SELECT MIN(n.name) AS member_in_charnamed_movie
 FROM cast_info AS ci, movie_companies AS mc, movie_keyword AS mk, k, n, title AS t, company_name AS cn, 
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
