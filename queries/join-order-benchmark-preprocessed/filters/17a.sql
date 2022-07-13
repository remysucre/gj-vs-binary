COPY (SELECT * FROM name AS n WHERE n.name LIKE 'B%') TO '../data/17a/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/17a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/17a/k.parquet' (FORMAT 'parquet');
