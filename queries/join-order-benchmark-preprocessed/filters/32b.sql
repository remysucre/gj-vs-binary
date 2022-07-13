COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/32b/k.parquet' (FORMAT 'parquet');
