COPY (SELECT * FROM kind_type AS kt WHERE kt.kind = 'movie') TO '../data/20c/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser')) TO '../data/20c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM char_name AS chn WHERE chn.name IS NOT NULL AND (chn.name LIKE '%man%' OR chn.name LIKE '%Man%')) TO '../data/20c/chn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind = 'cast') TO '../data/20c/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind LIKE '%complete%') TO '../data/20c/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/20c/t.parquet' (FORMAT 'parquet');
