COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/8a/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n1 WHERE n1.name LIKE '%Yo%' AND n1.name NOT LIKE '%Yu%') TO '../data/8a/n1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(Japan)%' AND mc.note NOT LIKE '%(USA)%') TO '../data/8a/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[jp]') TO '../data/8a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note = '(voice: English version)') TO '../data/8a/ci.parquet' (FORMAT 'parquet');
