COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(200%)%' AND mc.note LIKE '%(worldwide)%') TO '../data/15b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.note LIKE '%internet%' AND mi.info LIKE 'USA:% 200%') TO '../data/15b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2005 AND 2010) TO '../data/15b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'release dates') TO '../data/15b/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]' AND cn.name = 'YouTube') TO '../data/15b/cn.parquet' (FORMAT 'parquet');
