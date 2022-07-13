COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/16d/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.episode_nr >= 5 AND t.episode_nr < 100) TO '../data/16d/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/16d/k.parquet' (FORMAT 'parquet');
