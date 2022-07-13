COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%Angel%') TO '../data/9b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/9b/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note = '(voice)') TO '../data/9b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(200%)%' AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%')) TO '../data/9b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2007 AND 2010) TO '../data/9b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/9b/cn.parquet' (FORMAT 'parquet');
