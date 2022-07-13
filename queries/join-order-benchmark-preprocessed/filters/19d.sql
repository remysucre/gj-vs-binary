COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) TO '../data/19d/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/19d/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/19d/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/19d/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'release dates') TO '../data/19d/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'f') TO '../data/19d/n.parquet' (FORMAT 'parquet');
