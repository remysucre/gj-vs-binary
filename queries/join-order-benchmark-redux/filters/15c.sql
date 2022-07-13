COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'release dates') TO '../data/15c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/15c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.note LIKE '%internet%' AND mi.info IS NOT NULL AND (mi.info LIKE 'USA:% 199%' OR mi.info LIKE 'USA:% 200%')) TO '../data/15c/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 1990) TO '../data/15c/t.parquet' (FORMAT 'parquet');
