COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actor') TO '../data/10a/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note LIKE '%(voice)%' AND ci.note LIKE '%(uncredited)%') TO '../data/10a/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[ru]') TO '../data/10a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2005) TO '../data/10a/t.parquet' (FORMAT 'parquet');
