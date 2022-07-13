COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/19b/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note = '(voice)') TO '../data/19b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%Angel%') TO '../data/19b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'release dates') TO '../data/19b/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/19b/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IS NOT NULL AND (mi.info LIKE 'Japan:%2007%' OR mi.info LIKE 'USA:%2008%')) TO '../data/19b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(200%)%' AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')) TO '../data/19b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2007 AND 2008 AND t.title LIKE '%Kung%Fu%Panda%') TO '../data/19b/t.parquet' (FORMAT 'parquet');
