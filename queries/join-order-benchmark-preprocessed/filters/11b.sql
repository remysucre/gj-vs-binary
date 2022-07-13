COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NULL) TO '../data/11b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/11b/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year = 1998 AND t.title LIKE '%Money%') TO '../data/11b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'sequel') TO '../data/11b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link LIKE '%follows%') TO '../data/11b/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')) TO '../data/11b/cn.parquet' (FORMAT 'parquet');
