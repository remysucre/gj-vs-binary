COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/15a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'release dates') TO '../data/15a/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.note LIKE '%internet%' AND mi.info LIKE 'USA:% 200%') TO '../data/15a/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(200%)%' AND mc.note LIKE '%(worldwide)%') TO '../data/15a/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/15a/cn.parquet' (FORMAT 'parquet');
