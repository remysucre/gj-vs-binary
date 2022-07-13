COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info > '8.0') TO '../data/26b/mi_idx.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind = 'cast') TO '../data/26b/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/26b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM kind_type AS kt WHERE kt.kind = 'movie') TO '../data/26b/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2005) TO '../data/26b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind LIKE '%complete%') TO '../data/26b/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('superhero', 'marvel-comics', 'based-on-comic', 'fight')) TO '../data/26b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM char_name AS chn WHERE chn.name IS NOT NULL AND (chn.name LIKE '%man%' OR chn.name LIKE '%Man%')) TO '../data/26b/chn.parquet' (FORMAT 'parquet');
