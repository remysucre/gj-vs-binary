SELECT MIN(t.title) AS movie_title
 FROM title AS t, movie_keyword AS mk, movie_companies AS mc, k, cn, 
WHERE cn.id = mc.company_id
AND mc.movie_id = t.id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mc.movie_id = mk.movie_id
;
