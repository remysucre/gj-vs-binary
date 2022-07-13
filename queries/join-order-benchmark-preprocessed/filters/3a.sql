COPY (SELECT * FROM title AS t WHERE t.production_year > 2005) TO '../data/3a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword LIKE '%sequel%') TO '../data/3a/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German')) TO '../data/3a/mi.parquet' (FORMAT 'parquet');
