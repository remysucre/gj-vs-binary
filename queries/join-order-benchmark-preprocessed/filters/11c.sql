COPY (SELECT * FROM company_type AS ct WHERE ct.kind <> 'production companies' AND ct.kind IS NOT NULL) TO '../data/11c/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('sequel', 'revenge', 'based-on-novel')) TO '../data/11c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NOT NULL) TO '../data/11c/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[pl]' AND (cn.name LIKE '20th Century Fox%' OR cn.name LIKE 'Twentieth Century Fox%')) TO '../data/11c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 1950) TO '../data/11c/t.parquet' (FORMAT 'parquet');
