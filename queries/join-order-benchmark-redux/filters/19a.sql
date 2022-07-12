COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%Ang%') TO '../data/19a/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/19a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) TO '../data/19a/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'release dates') TO '../data/19a/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2005 AND 2009) TO '../data/19a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IS NOT NULL AND (mi.info LIKE 'Japan:%200%' OR mi.info LIKE 'USA:%200%')) TO '../data/19a/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note IS NOT NULL AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')) TO '../data/19a/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/19a/rt.parquet' (FORMAT 'parquet');
