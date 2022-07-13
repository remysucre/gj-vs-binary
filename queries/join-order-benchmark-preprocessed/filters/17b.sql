COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/17b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.name LIKE 'Z%') TO '../data/17b/n.parquet' (FORMAT 'parquet');
