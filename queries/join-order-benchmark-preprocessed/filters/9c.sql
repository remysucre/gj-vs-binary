COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%An%') TO '../data/9c/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) TO '../data/9c/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/9c/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/9c/cn.parquet' (FORMAT 'parquet');
