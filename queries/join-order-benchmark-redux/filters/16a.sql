COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/16a/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/16a/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.episode_nr >= 50 AND t.episode_nr < 100) TO '../data/16a/t.parquet' (FORMAT 'parquet');
