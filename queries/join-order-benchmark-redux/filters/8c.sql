COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'writer') TO '../data/8c/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/8c/cn.parquet' (FORMAT 'parquet');
