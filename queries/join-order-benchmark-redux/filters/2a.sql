COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/2a/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[de]') TO '../data/2a/cn.parquet' (FORMAT 'parquet');
