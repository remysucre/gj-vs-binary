SELECT MIN(chn.name) AS character_name, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_hero_movie
 FROM complete_cast AS cc, movie_keyword AS mk, cast_info AS ci, k, movie_info_idx AS mi_idx, t, kt, chn, cct2, cct1, name AS n, it2, 
WHERE kt.id = t.kind_id
AND t.id = mk.movie_id
AND t.id = ci.movie_id
AND t.id = cc.movie_id
AND t.id = mi_idx.movie_id
AND mk.movie_id = ci.movie_id
AND mk.movie_id = cc.movie_id
AND mk.movie_id = mi_idx.movie_id
AND ci.movie_id = cc.movie_id
AND ci.movie_id = mi_idx.movie_id
AND cc.movie_id = mi_idx.movie_id
AND chn.id = ci.person_role_id
AND n.id = ci.person_id
AND k.id = mk.keyword_id
AND cct1.id = cc.subject_id
AND cct2.id = cc.status_id
AND it2.id = mi_idx.info_type_id
;
