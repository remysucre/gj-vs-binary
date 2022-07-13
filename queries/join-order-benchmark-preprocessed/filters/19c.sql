COPY (SELECT * FROM info_type AS it WHERE it.info = 'release dates') TO '../data/19c/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) TO '../data/19c/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/19c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IS NOT NULL AND (mi.info LIKE 'Japan:%200%' OR mi.info LIKE 'USA:%200%')) TO '../data/19c/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/19c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%An%') TO '../data/19c/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/19c/rt.parquet' (FORMAT 'parquet');
