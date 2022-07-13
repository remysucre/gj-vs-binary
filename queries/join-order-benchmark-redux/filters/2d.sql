COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/2d/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/2d/k.parquet' (FORMAT 'parquet');
