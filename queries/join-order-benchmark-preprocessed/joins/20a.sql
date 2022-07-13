SELECT MIN(t.title) AS complete_downey_ironman_movie
 FROM k, kt, movie_keyword AS mk, name AS n, cct1, cct2, chn, complete_cast AS cc, t, cast_info AS ci, 
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
