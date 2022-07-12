COPY (SELECT * FROM kind_type AS kt WHERE kt.kind IN ('movie')) TO '../data/23a/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'release dates') TO '../data/23a/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/23a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.note LIKE '%internet%' AND mi.info IS NOT NULL AND (mi.info LIKE 'USA:% 199%' OR mi.info LIKE 'USA:% 200%')) TO '../data/23a/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/23a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind = 'complete+verified') TO '../data/23a/cct1.parquet' (FORMAT 'parquet');
