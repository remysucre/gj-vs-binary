COPY (SELECT * FROM link_type AS lt WHERE lt.link LIKE '%follow%') TO '../data/21c/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NULL) TO '../data/21c/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'English')) TO '../data/21c/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 1950 AND 2010) TO '../data/21c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')) TO '../data/21c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'sequel') TO '../data/21c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/21c/ct.parquet' (FORMAT 'parquet');
