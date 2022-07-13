COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')) TO '../data/27c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NULL) TO '../data/27c/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind LIKE 'complete%') TO '../data/27c/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'sequel') TO '../data/27c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/27c/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind = 'cast') TO '../data/27c/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'English')) TO '../data/27c/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link LIKE '%follow%') TO '../data/27c/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 1950 AND 2010) TO '../data/27c/t.parquet' (FORMAT 'parquet');
