COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Bulgaria')) TO '../data/3b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword LIKE '%sequel%') TO '../data/3b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2010) TO '../data/3b/t.parquet' (FORMAT 'parquet');
