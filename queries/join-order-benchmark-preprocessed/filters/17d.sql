COPY (SELECT * FROM name AS n WHERE n.name LIKE '%Bert%') TO '../data/17d/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'character-name-in-title') TO '../data/17d/k.parquet' (FORMAT 'parquet');
