COPY (SELECT * FROM title AS t WHERE t.production_year > 2014) TO '../data/6b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.name LIKE '%Downey%Robert%') TO '../data/6b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')) TO '../data/6b/k.parquet' (FORMAT 'parquet');
