Since tensor algebra only supports joins and projection, we need to remove the filters from each query and instead store the filtered intermediates as materialized views. For example, the following query 

```SQL
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
  FROM cast_info AS ci,
       info_type AS it1,
       info_type AS it2,
       movie_info AS mi,
       movie_info_idx AS mi_idx,
       name AS n,
       title AS t
 WHERE ci.note IN ('(producer)','(executive producer)')
   AND it1.info = 'budget'
   AND it2.info = 'votes'
   AND n.gender = 'm'
   AND n.name LIKE '%Tim%'
   AND t.id = mi.movie_id
   AND t.id = mi_idx.movie_id
   AND t.id = ci.movie_id
   AND ci.movie_id = mi.movie_id
   AND ci.movie_id = mi_idx.movie_id
   AND mi.movie_id = mi_idx.movie_id
   AND n.id = ci.person_id
   AND it1.id = mi.info_type_id
   AND it2.id = mi_idx.info_type_id;
```

Becomes 

```SQL
SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
  FROM ci,
       it1,
       it2,
       movie_info AS mi,
       movie_info_idx AS mi_idx,
       n,
       title AS t
 WHERE t.id = mi.movie_id
   AND t.id = mi_idx.movie_id
   AND t.id = ci.movie_id
   AND ci.movie_id = mi.movie_id
   AND ci.movie_id = mi_idx.movie_id
   AND mi.movie_id = mi_idx.movie_id
   AND n.id = ci.person_id
   AND it1.id = mi.info_type_id
   AND it2.id = mi_idx.info_type_id;
```

And we need to define intermediate tables: 

```SQL
SELECT (*)
  FROM cast_info AS ci
 WHERE ci.note IN ('(producer)','(executive producer)')
```

```SQL
SELECT (*)
  FROM info_type AS it1
 WHERE it1.info = 'budget'
```

```SQL
SELECT (*)
  FROM info_type AS it2
 WHERE it2.info = 'votes'
```

```SQL
SELECT (*)
  FROM name AS n
 WHERE n.gender = 'm'
   AND n.name LIKE '%Tim%'
```
