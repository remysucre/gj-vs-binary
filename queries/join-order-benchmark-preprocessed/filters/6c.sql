COPY (SELECT * FROM keyword AS k WHERE k.keyword = 'marvel-cinematic-universe') TO '../data/6c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.name LIKE '%Downey%Robert%') TO '../data/6c/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2014) TO '../data/6c/t.parquet' (FORMAT 'parquet');
