SELECT MIN(n.name) AS cast_member, MIN(t.title) AS complete_dynamic_hero_movie
 FROM cct1, name AS n, cct2, k, cast_info AS ci, chn, kt, movie_keyword AS mk, t, complete_cast AS cc, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND ci.movie_id = cc.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
;
