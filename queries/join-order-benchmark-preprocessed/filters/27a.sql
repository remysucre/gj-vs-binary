COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind IN ('cast', 'crew')) TO '../data/27a/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NULL) TO '../data/27a/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link LIKE '%follow%') TO '../data/27a/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind = 'complete') TO '../data/27a/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Germany', 'Swedish', 'German')) TO '../data/27a/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')) TO '../data/27a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'sequel') TO '../data/27a/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 1950 AND 2000) TO '../data/27a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/27a/ct.parquet' (FORMAT 'parquet');
