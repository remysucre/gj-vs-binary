COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/16b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/16b/cn.parquet' (FORMAT 'parquet');
