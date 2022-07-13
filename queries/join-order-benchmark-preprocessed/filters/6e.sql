COPY (SELECT * FROM name AS n WHERE n.name LIKE '%Downey%Robert%') TO '../data/6e/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'marvel-cinematic-universe') TO '../data/6e/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/6e/t.parquet' (FORMAT 'parquet');
