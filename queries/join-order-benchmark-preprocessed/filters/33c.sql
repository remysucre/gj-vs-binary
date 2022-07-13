COPY (SELECT * FROM kind_type AS kt2 WHERE kt2.kind IN ('tv series', 'episode')) TO '../data/33c/kt2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM kind_type AS kt1 WHERE kt1.kind IN ('tv series', 'episode')) TO '../data/33c/kt1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/33c/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t2 WHERE t2.production_year BETWEEN 2000 AND 2010) TO '../data/33c/t2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info_idx AS mi_idx2 WHERE mi_idx2.info < '3.5') TO '../data/33c/mi_idx2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'rating') TO '../data/33c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link IN ('sequel', 'follows', 'followed by')) TO '../data/33c/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn1 WHERE cn1.country_code <> '[us]') TO '../data/33c/cn1.parquet' (FORMAT 'parquet');
