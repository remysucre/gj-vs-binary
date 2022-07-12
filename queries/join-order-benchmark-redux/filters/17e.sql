COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/17e/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/17e/k.parquet' (FORMAT 'parquet');
